from __future__ import annotations

import os
from pathlib import Path
import secrets
from datetime import date, datetime
from dataclasses import dataclass
from markupsafe import Markup, escape

from dotenv import load_dotenv
from flask import (
    Flask,
    abort,
    flash,
    make_response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash

from backup import maybe_backup_to_github
from kakao_parser import parse_kakao_talk_txt
from storage import (
    add_diary_entry,
    delete_diary_entry,
    fetch_diary_entries,
    fetch_messages,
    fetch_senders,
    get_diary_entry,
    import_messages_canonicalized,
    normalize_db_senders_and_dedup,
    search_messages,
    serialize_diary_csv,
    serialize_diary_markdown,
    serialize_diary_plain,
    update_diary_entry,
)


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("CHAT_APP_DATA_DIR", "")).expanduser().resolve() if os.getenv("CHAT_APP_DATA_DIR") else (BASE_DIR / "data")
DB_PATH = DATA_DIR / "chat.db"
SECRET_KEY_PATH = DATA_DIR / "secret_key.txt"


def _is_truthy(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "y", "on")


def _ensure_secret_key() -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if SECRET_KEY_PATH.exists():
        return SECRET_KEY_PATH.read_text(encoding="utf-8").strip()
    key = secrets.token_hex(32)
    SECRET_KEY_PATH.write_text(key, encoding="utf-8")
    return key


def _decode_uploaded_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def _require_login() -> None:
    if session.get("auth_disabled"):
        return
    if not session.get("logged_in"):
        abort(401)


def _escape_with_br(text: str) -> Markup:
    return Markup(escape(text)).replace("\n", Markup("<br>"))


def _highlight_html(text: str, term: str | None) -> Markup:
    if not term:
        return _escape_with_br(text)
    term = term.strip()
    if not term:
        return _escape_with_br(text)
    parts = text.split(term)
    if len(parts) == 1:
        return _escape_with_br(text)
    out: list[Markup] = []
    for i, part in enumerate(parts):
        out.append(_escape_with_br(part))
        if i < len(parts) - 1:
            out.append(Markup("<mark class=\"hl\">") + _escape_with_br(term) + Markup("</mark>"))
    return Markup("").join(out)


def _format_ko_date(value: date) -> str:
    weekday_ko = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"][value.weekday()]
    return f"{value.year}년 {value.month}월 {value.day}일 {weekday_ko}"


def _parse_iso_date(value: str) -> date | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _diary_redirect_args(form) -> dict[str, str]:
    args: dict[str, str] = {}
    q = (form.get("q") or "").strip()
    if q:
        args["q"] = q
    start_date = (form.get("start_date") or "").strip()
    if start_date:
        args["start_date"] = start_date
    end_date = (form.get("end_date") or "").strip()
    if end_date:
        args["end_date"] = end_date
    return args


def create_app() -> Flask:
    load_dotenv(BASE_DIR / ".env", interpolate=False, encoding="utf-8-sig")

    auth_disabled = _is_truthy(os.getenv("CHAT_APP_DISABLE_AUTH", ""))
    password_hash = os.getenv("CHAT_APP_PASSWORD_HASH", "").strip().strip('"').strip("'")
    if not auth_disabled and not password_hash:
        raise RuntimeError(
            "CHAT_APP_PASSWORD_HASH가 필요합니다. tools/set_password.py를 실행해 .env를 만든 뒤 다시 실행하세요."
        )

    app = Flask(__name__)
    app.secret_key = os.getenv("CHAT_APP_SECRET_KEY", "").strip() or _ensure_secret_key()
    app.config.update(
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
    )

    canonical_me = os.getenv("CHAT_APP_CANONICAL_ME_NAME", "이성준").strip() or "이성준"
    canonical_other = os.getenv("CHAT_APP_CANONICAL_OTHER_NAME", "귀여운소연이").strip() or "귀여운소연이"
    app.config["CHAT_CANONICAL_ME_NAME"] = canonical_me
    app.config["CHAT_CANONICAL_OTHER_NAME"] = canonical_other

    app.config["CHAT_ME_NAME"] = os.getenv("CHAT_APP_ME", "").strip() or canonical_me
    app.config["CHAT_PASSWORD_HASH"] = password_hash
    app.config["AUTH_DISABLED"] = auth_disabled

    @app.before_request
    def _auth_flag_to_session():
        session["auth_disabled"] = bool(app.config.get("AUTH_DISABLED"))

    @app.errorhandler(401)
    def _unauthorized(_err):
        if app.config.get("AUTH_DISABLED"):
            return redirect(url_for("index"))
        return redirect(url_for("login", next=request.path))

    @app.get("/login")
    def login():
        if app.config.get("AUTH_DISABLED"):
            return redirect(url_for("index"))
        return render_template("login.html")

    @app.post("/login")
    def login_post():
        if app.config.get("AUTH_DISABLED"):
            session["logged_in"] = True
            session.permanent = True
            return redirect(url_for("index"))
        password = request.form.get("password", "")
        if not check_password_hash(app.config["CHAT_PASSWORD_HASH"], password):
            flash("비밀번호가 틀렸습니다.", "error")
            return redirect(url_for("login"))
        session["logged_in"] = True
        session.permanent = True
        return redirect(url_for("index"))

    @app.get("/logout")
    def logout():
        session.clear()
        if app.config.get("AUTH_DISABLED"):
            return redirect(url_for("index"))
        return redirect(url_for("login"))

    @app.get("/")
    def index():
        _require_login()
        view = request.args.get("view", "chat").strip().lower()
        if view not in ("chat", "txt"):
            view = "chat"
        q = (request.args.get("q") or "").strip()

        @dataclass
        class DayGroup:
            date_key: str
            date_ko: str
            messages: list[dict]

        me_name = session.get("me_name") or app.config["CHAT_ME_NAME"]
        if q:
            raw_messages = search_messages(DB_PATH, q, limit=5000)
        else:
            raw_messages = fetch_messages(DB_PATH, limit=None, before_dt=None, order="asc")
        days: list[DayGroup] = []
        current: DayGroup | None = None
        for m in raw_messages:
            dt = datetime.fromisoformat(m["dt"])
            date_key = dt.strftime("%Y-%m-%d")
            weekday_ko = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"][dt.weekday()]
            date_ko = f"{dt.year}년 {dt.month}월 {dt.day}일 {weekday_ko}"
            ampm = "오전" if dt.hour < 12 else "오후"
            h12 = dt.hour % 12 or 12
            time_ko = f"{ampm} {h12}:{dt.minute:02d}"

            m["date_key"] = date_key
            m["date_ko"] = date_ko
            m["time_ko"] = time_ko
            m["text_html"] = _highlight_html(m["text"], q if q else None)

            if current is None or current.date_key != date_key:
                current = DayGroup(date_key=date_key, date_ko=date_ko, messages=[])
                days.append(current)
            current.messages.append(m)

        template = "chat_txt.html" if view == "txt" else "chat.html"
        senders = fetch_senders(DB_PATH, limit=80)
        return render_template(
            template,
            days=days,
            me_name=me_name,
            senders=senders,
            view=view,
            search_query=q,
            search_count=len(raw_messages),
        )

    @app.get("/diary")
    def diary():
        _require_login()
        q = (request.args.get("q") or "").strip()
        start_date_raw = (request.args.get("start_date") or "").strip()
        end_date_raw = (request.args.get("end_date") or "").strip()
        edit_id_raw = (request.args.get("edit") or "").strip()

        start_date = _parse_iso_date(start_date_raw)
        end_date = _parse_iso_date(end_date_raw)
        if start_date_raw and not start_date:
            flash("시작 날짜 형식이 올바르지 않습니다.", "error")
        if end_date_raw and not end_date:
            flash("종료 날짜 형식이 올바르지 않습니다.", "error")

        entries = fetch_diary_entries(
            DB_PATH,
            limit=200,
            q=q or None,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
            order="desc",
        )
        editing = None
        if edit_id_raw.isdigit():
            editing = get_diary_entry(DB_PATH, int(edit_id_raw))
            if not editing:
                flash("수정할 일기를 찾지 못했습니다.", "error")
        elif edit_id_raw:
            flash("수정할 일기 번호가 올바르지 않습니다.", "error")
        if editing:
            editing["entry_date_value"] = str(editing.get("entry_date") or "")

        for entry in entries:
            entry_date_raw = str(entry["entry_date"])
            try:
                entry_date = date.fromisoformat(entry_date_raw)
                entry["date_ko"] = _format_ko_date(entry_date)
            except ValueError:
                entry["date_ko"] = entry_date_raw
            entry["body_html"] = _escape_with_br(str(entry["body"] or ""))
        return render_template(
            "diary.html",
            entries=entries,
            editing=editing,
            today=date.today().isoformat(),
            search_query=q,
            start_date=start_date.isoformat() if start_date else start_date_raw,
            end_date=end_date.isoformat() if end_date else end_date_raw,
        )

    @app.post("/diary")
    def diary_post():
        _require_login()
        entry_date_raw = (request.form.get("entry_date") or "").strip()
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()

        if not body:
            flash("내용이 비어있습니다.", "error")
            return redirect(url_for("diary"))

        if not entry_date_raw:
            entry_date_raw = date.today().isoformat()
        entry_date = _parse_iso_date(entry_date_raw)
        if not entry_date:
            flash("날짜 형식이 올바르지 않습니다.", "error")
            return redirect(url_for("diary", **_diary_redirect_args(request.form)))

        if not title:
            title = "무제"

        add_diary_entry(DB_PATH, entry_date.isoformat(), title, body)
        maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
        flash("일기를 저장했습니다.", "ok")
        return redirect(url_for("diary", **_diary_redirect_args(request.form)))

    @app.post("/diary/<int:entry_id>/edit")
    def diary_edit_post(entry_id: int):
        _require_login()
        entry_date_raw = (request.form.get("entry_date") or "").strip()
        title = (request.form.get("title") or "").strip()
        body = (request.form.get("body") or "").strip()

        if not body:
            flash("내용이 비어있습니다.", "error")
            return redirect(url_for("diary", edit=entry_id, **_diary_redirect_args(request.form)))

        if not entry_date_raw:
            entry_date_raw = date.today().isoformat()
        entry_date = _parse_iso_date(entry_date_raw)
        if not entry_date:
            flash("날짜 형식이 올바르지 않습니다.", "error")
            return redirect(url_for("diary", edit=entry_id, **_diary_redirect_args(request.form)))

        if not title:
            title = "무제"

        updated = update_diary_entry(DB_PATH, entry_id, entry_date.isoformat(), title, body)
        if not updated:
            flash("수정할 일기를 찾지 못했습니다.", "error")
            return redirect(url_for("diary", **_diary_redirect_args(request.form)))

        maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
        flash("일기를 수정했습니다.", "ok")
        return redirect(url_for("diary", **_diary_redirect_args(request.form)))

    @app.post("/diary/<int:entry_id>/delete")
    def diary_delete_post(entry_id: int):
        _require_login()
        deleted = delete_diary_entry(DB_PATH, entry_id)
        if deleted:
            maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
            flash("일기를 삭제했습니다.", "ok")
        else:
            flash("삭제할 일기를 찾지 못했습니다.", "error")
        return redirect(url_for("diary", **_diary_redirect_args(request.form)))

    @app.get("/diary/export")
    def diary_export():
        _require_login()
        fmt = (request.args.get("format") or "txt").strip().lower()
        q = (request.args.get("q") or "").strip()
        start_date = _parse_iso_date(request.args.get("start_date") or "")
        end_date = _parse_iso_date(request.args.get("end_date") or "")
        entries = fetch_diary_entries(
            DB_PATH,
            limit=None,
            q=q or None,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
            order="asc",
        )
        if fmt == "csv":
            content = serialize_diary_csv(entries)
            content_type = "text/csv; charset=utf-8"
            ext = "csv"
        elif fmt == "md":
            content = serialize_diary_markdown(entries)
            content_type = "text/markdown; charset=utf-8"
            ext = "md"
        else:
            content = serialize_diary_plain(entries)
            content_type = "text/plain; charset=utf-8"
            ext = "txt"

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"diary_export_{stamp}.{ext}"
        resp = make_response(content)
        resp.headers["Content-Type"] = content_type
        resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return resp

    @app.post("/me")
    def set_me():
        _require_login()
        name = (request.form.get("me_name") or "").strip()
        if name:
            session["me_name"] = name
        else:
            session.pop("me_name", None)
        return redirect(url_for("index"))

    @app.get("/admin/import")
    def admin_import():
        _require_login()
        return render_template("import.html")

    @app.post("/admin/import")
    def admin_import_post():
        _require_login()

        source_label = ""
        text = ""

        if "file" in request.files and request.files["file"].filename:
            up = request.files["file"]
            source_label = up.filename
            text = _decode_uploaded_bytes(up.read())
        else:
            text = request.form.get("text", "")
            source_label = "pasted"

        if not text.strip():
            flash("가져올 내용이 비어있습니다.", "error")
            return redirect(url_for("admin_import"))

        msgs = parse_kakao_talk_txt(text)
        if not msgs:
            flash("메시지를 찾지 못했습니다. (파일 형식을 확인하세요)", "error")
            return redirect(url_for("admin_import"))

        result = import_messages_canonicalized(
            DB_PATH,
            msgs,
            source=source_label,
            me_sender=app.config["CHAT_CANONICAL_ME_NAME"],
            other_sender=app.config["CHAT_CANONICAL_OTHER_NAME"],
        )
        maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
        flash(
            f"가져오기 완료: {result['inserted']}개 추가, {result['skipped']}개 중복 제외 (총 {result['total']}개 파싱)",
            "ok",
        )
        return redirect(url_for("index"))

    @app.post("/admin/normalize")
    def admin_normalize():
        _require_login()
        result = normalize_db_senders_and_dedup(
            DB_PATH,
            me_sender=app.config["CHAT_CANONICAL_ME_NAME"],
            other_sender=app.config["CHAT_CANONICAL_OTHER_NAME"],
        )
        maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
        flash(
            f"정리 완료: {result['kept']}개 유지, {result['dropped']}개 중복 제거 (총 {result['total']}개 처리)",
            "ok",
        )
        return redirect(url_for("index"))

    return app


if __name__ == "__main__":
    app = create_app()
    host = os.getenv("CHAT_APP_HOST", "127.0.0.1")
    port = int(os.getenv("CHAT_APP_PORT", "8000"))
    app.run(host=host, port=port, debug=False)
