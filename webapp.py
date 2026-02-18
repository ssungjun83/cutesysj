from __future__ import annotations

import io
import os
from pathlib import Path
import csv
import calendar as pycalendar
import re
import secrets
from datetime import date, datetime, timedelta
from dataclasses import dataclass
from markupsafe import Markup, escape
from zoneinfo import ZoneInfo

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
    send_file,
    url_for,
)
from werkzeug.security import check_password_hash

from backup import maybe_backup_to_github
from drive_client import (
    DriveConfigError,
    delete_drive_file,
    download_drive_file,
    get_drive_config_status,
    get_drive_folder_id,
    list_drive_images,
    upload_drive_file,
)
from export_utils import (
    build_export_header,
    parse_chat_csv,
    parse_chat_plain,
    parse_diary_csv,
    parse_diary_markdown,
    parse_diary_plain,
    parse_memories_csv,
    parse_memories_txt,
    serialize_chat_csv,
    serialize_chat_kakao,
    serialize_chat_plain,
    serialize_memories_csv,
    serialize_memories_txt,
    strip_export_header,
)
from kakao_parser import parse_kakao_talk_txt
from storage import (
    add_chat_bookmark,
    add_diary_entry,
    add_diary_comment,
    delete_chat_bookmark,
    delete_diary_entry,
    delete_diary_comment,
    delete_diary_photo,
    add_todo_item,
    check_todo_daily_item,
    complete_todo_item,
    delete_todo_item,
    fetch_diary_comments,
    fetch_diary_entries,
    fetch_diary_photos,
    fetch_chat_bookmarks,
    fetch_messages,
    fetch_messages_between,
    fetch_memory_albums,
    fetch_memory_photos,
    fetch_todo_items,
    fetch_senders,
    get_latest_dt,
    get_oldest_dt,
    get_diary_entry,
    get_diary_photo,
    get_chat_bookmark,
    import_messages,
    import_messages_canonicalized,
    get_memory_photo,
    get_todo_item,
    normalize_db_senders_and_dedup,
    search_messages,
    serialize_diary_csv,
    serialize_diary_markdown,
    serialize_diary_plain,
    update_diary_entry,
    update_chat_bookmark_title,
    upsert_diary_comment,
    upsert_diary_entry,
    migrate_diary_timezone_seoul,
    add_memory_photo,
    update_memory_photo,
    update_todo_item,
    delete_memory_photo,
    upsert_memory_photo,
    upsert_memory_photo_full,
)


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("CHAT_APP_DATA_DIR", "")).expanduser().resolve() if os.getenv("CHAT_APP_DATA_DIR") else (BASE_DIR / "data")
DB_PATH = DATA_DIR / "chat.db"
SECRET_KEY_PATH = DATA_DIR / "secret_key.txt"
SEOUL_TZ = ZoneInfo("Asia/Seoul")


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


_URL_RE = re.compile(r"https?://[^\s<]+", re.IGNORECASE)
_URL_TRAILING_PUNCT = ".,!?;:)]}\"'"


def _linkify_with_br(text: str) -> Markup:
    raw = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    if not raw:
        return Markup("")
    out: list[Markup] = []
    last = 0
    for match in _URL_RE.finditer(raw):
        start, end = match.span()
        out.append(escape(raw[last:start]))

        token = match.group(0)
        trimmed = token.rstrip(_URL_TRAILING_PUNCT)
        trailing = token[len(trimmed) :]
        if trimmed:
            href = escape(trimmed)
            out.append(
                Markup(
                    f"<a href=\"{href}\" target=\"_blank\" rel=\"noopener noreferrer nofollow ugc\">{href}</a>"
                )
            )
        else:
            out.append(escape(token))
        if trailing:
            out.append(escape(trailing))
        last = end

    out.append(escape(raw[last:]))
    return Markup("").join(out).replace("\n", Markup("<br>"))


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


def _today_seoul_date() -> date:
    return datetime.now(SEOUL_TZ).date()


def _parse_iso_date(value: str) -> date | None:
    value = (value or "").strip()
    if not value:
        return None


def _parse_year_month(value: str) -> tuple[int, int] | None:
    raw = (value or "").strip()
    if not raw:
        return None
    m = re.fullmatch(r"(\d{4})-(\d{2})", raw)
    if not m:
        return None
    year = int(m.group(1))
    month = int(m.group(2))
    if month < 1 or month > 12:
        return None
    return year, month


def _shift_year_month(year: int, month: int, delta: int) -> tuple[int, int]:
    idx = year * 12 + (month - 1) + delta
    shifted_year = idx // 12
    shifted_month = (idx % 12) + 1
    return shifted_year, shifted_month
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _format_comment_ts(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    parsed = _parse_timestamp(raw)
    if not parsed:
        return raw
    if parsed.tzinfo:
        parsed = parsed.astimezone(SEOUL_TZ)
    return parsed.strftime("%Y-%m-%d %H:%M")


def _parse_timestamp(value: str) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    try:
        d = datetime.strptime(raw, "%Y-%m-%d").date()
        return datetime(d.year, d.month, d.day)
    except ValueError:
        return None


def _format_entry_time(value: str) -> str:
    parsed = _parse_timestamp(value)
    if not parsed:
        return ""
    if parsed.tzinfo:
        parsed = parsed.astimezone(SEOUL_TZ)
    return parsed.strftime("%H:%M")


@dataclass
class ChatDayGroup:
    date_key: str
    date_ko: str
    messages: list[dict]


def _decorate_chat_messages(raw_messages: list[dict], *, search_term: str | None = None) -> list[dict]:
    messages: list[dict] = []
    for idx, raw in enumerate(raw_messages):
        message = dict(raw)
        dt = datetime.fromisoformat(str(message["dt"]))
        date_key = dt.strftime("%Y-%m-%d")
        message["date_key"] = date_key
        message["date_ko"] = _format_ko_date(dt.date())
        ampm = "오전" if dt.hour < 12 else "오후"
        hour12 = dt.hour % 12 or 12
        message["time_ko"] = f"{ampm} {hour12}:{dt.minute:02d}"
        message["seq"] = idx
        message["text_html"] = _highlight_html(str(message.get("text") or ""), search_term if search_term else None)
        messages.append(message)
    return messages


def _group_chat_days(messages: list[dict]) -> list[ChatDayGroup]:
    days: list[ChatDayGroup] = []
    current: ChatDayGroup | None = None
    for message in messages:
        date_key = str(message["date_key"])
        date_ko = str(message["date_ko"])
        if current is None or current.date_key != date_key:
            current = ChatDayGroup(date_key=date_key, date_ko=date_ko, messages=[])
            days.append(current)
        current.messages.append(message)
    return days


def _serialize_chat_days(days: list[ChatDayGroup], *, me_name: str | None = None) -> list[dict]:
    serialized: list[dict] = []
    me = (me_name or "").strip()
    for day in days:
        item = {
            "date_key": day.date_key,
            "date_ko": day.date_ko,
            "messages": [],
        }
        for message in day.messages:
            sender = str(message.get("sender") or "")
            item["messages"].append(
                {
                    "id": int(message["id"]),
                    "dt": str(message.get("dt") or ""),
                    "sender": sender,
                    "text": str(message.get("text") or ""),
                    "time_ko": str(message.get("time_ko") or ""),
                    "is_me": bool(me and sender == me),
                }
            )
        serialized.append(item)
    return serialized


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


def _memories_redirect_args(form) -> dict[str, str]:
    args: dict[str, str] = {}
    q = (form.get("filter_q") or form.get("q") or "").strip()
    if q:
        args["q"] = q
    album = (form.get("filter_album") or form.get("album") or "").strip()
    if album:
        args["album"] = album
    tag = (form.get("filter_tag") or form.get("tag") or "").strip()
    if tag:
        args["tag"] = tag
    start_date = (form.get("filter_start_date") or form.get("start_date") or "").strip()
    if start_date:
        args["start_date"] = start_date
    end_date = (form.get("filter_end_date") or form.get("end_date") or "").strip()
    if end_date:
        args["end_date"] = end_date
    return args


def _chat_redirect_args(values) -> dict[str, str]:
    args: dict[str, str] = {}
    view = (values.get("view") or "chat").strip().lower()
    args["view"] = view if view in ("chat", "txt") else "chat"
    q = (values.get("q") or "").strip()
    if q:
        args["q"] = q
    bookmark = (values.get("bookmark") or "").strip()
    if bookmark.isdigit():
        args["bookmark"] = bookmark
    focus_date = (values.get("date") or "").strip()
    if focus_date:
        args["date"] = focus_date
    return args


def _split_tags(tags: str) -> list[str]:
    raw = (tags or "").strip()
    if not raw:
        return []
    parts = [part.strip() for part in raw.split(",")]
    return [part for part in parts if part]


def _todo_tags_from_input(value: str) -> tuple[str, list[str]]:
    tags = _split_tags((value or "").replace("#", " "))
    return ", ".join(tags), tags


def _csv_header_fields(text: str) -> set[str]:
    _meta, body = strip_export_header(text)
    for line in body.splitlines():
        if line.strip():
            first_line = line
            break
    else:
        return set()
    try:
        row = next(csv.reader([first_line]))
    except Exception:
        return set()
    return {field.strip() for field in row if field}


def _detect_import_kind(text: str, requested: str | None) -> str:
    kind = (requested or "auto").strip().lower()
    if kind and kind != "auto":
        return kind

    meta, _body = strip_export_header(text)
    meta_type = (meta.get("type") or "").strip().lower()
    if meta_type in ("chat", "diary", "memories"):
        return meta_type

    fields = _csv_header_fields(text)
    if fields:
        if {"dt", "sender", "text"} <= fields:
            return "chat"
        if {"entry_date", "title", "body"} <= fields:
            return "diary"
        if "drive_file_id" in fields:
            return "memories"

    if parse_chat_plain(text):
        return "chat"
    if parse_kakao_talk_txt(text):
        return "chat"
    if parse_diary_plain(text) or parse_diary_markdown(text):
        return "diary"
    if parse_memories_txt(text):
        return "memories"
    return "unknown"


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
        SESSION_PERMANENT=False,
        SESSION_ABSOLUTE_TIMEOUT=timedelta(minutes=20),
    )

    canonical_me = os.getenv("CHAT_APP_CANONICAL_ME_NAME", "이성준").strip() or "이성준"
    canonical_other = os.getenv("CHAT_APP_CANONICAL_OTHER_NAME", "귀여운소연이").strip() or "귀여운소연이"
    app.config["CHAT_CANONICAL_ME_NAME"] = canonical_me
    app.config["CHAT_CANONICAL_OTHER_NAME"] = canonical_other

    app.config["CHAT_ME_NAME"] = os.getenv("CHAT_APP_ME", "").strip() or canonical_me
    app.config["CHAT_PASSWORD_HASH"] = password_hash
    app.config["AUTH_DISABLED"] = auth_disabled
    migrate_diary_timezone_seoul(DB_PATH)

    def _download_response(content: str, content_type: str, filename: str):
        resp = make_response(content)
        resp.headers["Content-Type"] = content_type
        resp.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
        return resp

    @app.before_request
    def _auth_flag_to_session():
        session["auth_disabled"] = bool(app.config.get("AUTH_DISABLED"))
        if session.get("auth_disabled"):
            return
        if session.get("logged_in"):
            now = datetime.utcnow()
            login_at = session.get("login_at")
            if login_at:
                try:
                    login_dt = datetime.fromisoformat(login_at)
                except ValueError:
                    login_dt = None
                if login_dt and now - login_dt > app.config["SESSION_ABSOLUTE_TIMEOUT"]:
                    session.clear()
                    session["auth_disabled"] = bool(app.config.get("AUTH_DISABLED"))
                    return
            else:
                session["login_at"] = now.isoformat(timespec="seconds")

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
            session["login_at"] = datetime.utcnow().isoformat(timespec="seconds")
            return redirect(url_for("index"))
        password = request.form.get("password", "")
        if not check_password_hash(app.config["CHAT_PASSWORD_HASH"], password):
            flash("비밀번호가 틀렸습니다.", "error")
            return redirect(url_for("login"))
        session["logged_in"] = True
        session["login_at"] = datetime.utcnow().isoformat(timespec="seconds")
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
        if not request.args:
            return redirect(url_for("diary"))
        view = request.args.get("view", "chat").strip().lower()
        if view not in ("chat", "txt"):
            view = "chat"
        q = (request.args.get("q") or "").strip()
        bookmark_raw = (request.args.get("bookmark") or "").strip()
        bookmark_selected_id = int(bookmark_raw) if bookmark_raw.isdigit() else None
        focus_date_raw = (request.args.get("date") or "").strip()
        requested_focus_date = _parse_iso_date(focus_date_raw)
        if focus_date_raw and not requested_focus_date:
            flash("조회 날짜 형식이 올바르지 않습니다.", "error")

        me_name = session.get("me_name") or app.config["CHAT_ME_NAME"]
        bookmark_items = fetch_chat_bookmarks(DB_PATH, limit=None)
        for bookmark in bookmark_items:
            start_stamp = str(bookmark.get("start_dt") or "").replace("T", " ")[:16]
            end_stamp = str(bookmark.get("end_dt") or "").replace("T", " ")[:16]
            is_range = bool(bookmark.get("end_message_id"))
            title = str(bookmark.get("title") or "").strip()
            bookmark["is_range"] = is_range
            bookmark["display_title"] = title or ("범위 책갈피" if is_range else "책갈피")
            bookmark["display_range"] = f"{start_stamp} ~ {end_stamp}" if is_range else start_stamp
            bookmark["id"] = int(bookmark["id"])
            bookmark["start_message_id"] = int(bookmark["start_message_id"])

        selected_bookmark = None
        if bookmark_selected_id is not None:
            selected_bookmark = next(
                (item for item in bookmark_items if int(item["id"]) == bookmark_selected_id),
                None,
            )
            if not selected_bookmark:
                selected_bookmark = get_chat_bookmark(DB_PATH, bookmark_selected_id)
                if selected_bookmark:
                    selected_bookmark["id"] = int(selected_bookmark["id"])
                    selected_bookmark["start_message_id"] = int(selected_bookmark["start_message_id"])

        bookmark_target_message_id: int | None = None
        bookmark_focus_date: date | None = None
        if selected_bookmark:
            bookmark_target_message_id = int(selected_bookmark["start_message_id"])
            start_dt_text = str(selected_bookmark.get("start_dt") or "")
            if start_dt_text:
                bookmark_focus_date = _parse_iso_date(start_dt_text[:10])

        chunk_days = 3
        loaded_start_date = ""
        loaded_end_date = ""
        oldest_date_value = ""
        latest_date_value = ""
        focus_date_value = ""
        if q:
            raw_messages = search_messages(DB_PATH, q, limit=5000)
        else:
            oldest_dt = get_oldest_dt(DB_PATH)
            latest_dt = get_latest_dt(DB_PATH)
            oldest_focus_date = _parse_iso_date(str(oldest_dt)[:10]) if oldest_dt else None
            latest_focus_date = _parse_iso_date(str(latest_dt)[:10]) if latest_dt else None
            oldest_date_value = oldest_focus_date.isoformat() if oldest_focus_date else ""
            latest_date_value = latest_focus_date.isoformat() if latest_focus_date else ""
            focus_date = bookmark_focus_date or requested_focus_date or latest_focus_date
            if focus_date:
                focus_date_value = focus_date.isoformat()
                window_start = focus_date - timedelta(days=chunk_days)
                window_end = focus_date + timedelta(days=chunk_days)
                loaded_start_date = window_start.isoformat()
                loaded_end_date = window_end.isoformat()
                raw_messages = fetch_messages_between(
                    DB_PATH,
                    start_dt=window_start.isoformat(),
                    end_dt=f"{window_end.isoformat()}~",
                    order="asc",
                )
            else:
                raw_messages = []

        decorated_messages = _decorate_chat_messages(raw_messages, search_term=q if q else None)
        days = _group_chat_days(decorated_messages)

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
            bookmarks=bookmark_items,
            bookmark_selected_id=bookmark_selected_id,
            bookmark_target_message_id=bookmark_target_message_id,
            focus_date=focus_date_value,
            loaded_start_date=loaded_start_date,
            loaded_end_date=loaded_end_date,
            oldest_date=oldest_date_value,
            latest_date=latest_date_value,
            window_chunk_days=chunk_days,
            chat_window_api=url_for("chat_window"),
        )

    @app.get("/chat/window")
    def chat_window():
        _require_login()
        start_date_raw = (request.args.get("start_date") or "").strip()
        end_date_raw = (request.args.get("end_date") or "").strip()
        start_date = _parse_iso_date(start_date_raw)
        end_date = _parse_iso_date(end_date_raw)
        if not start_date or not end_date:
            return {
                "ok": False,
                "error": "시작/종료 날짜가 필요합니다.",
            }, 400
        if end_date < start_date:
            start_date, end_date = end_date, start_date
        if (end_date - start_date).days > 20:
            return {
                "ok": False,
                "error": "조회 범위가 너무 큽니다.",
            }, 400

        raw_messages = fetch_messages_between(
            DB_PATH,
            start_dt=start_date.isoformat(),
            end_dt=f"{end_date.isoformat()}~",
            order="asc",
        )
        me_name = session.get("me_name") or app.config["CHAT_ME_NAME"]
        messages = _decorate_chat_messages(raw_messages, search_term=None)
        days = _group_chat_days(messages)
        return {
            "ok": True,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "count": len(messages),
            "days": _serialize_chat_days(days, me_name=me_name),
        }

    @app.post("/chat/bookmarks")
    def chat_bookmark_add():
        _require_login()
        message_id_raw = (request.form.get("message_id") or "").strip()
        title = (request.form.get("title") or "").strip()

        if not message_id_raw.isdigit():
            flash("책갈피로 저장할 메시지를 선택해 주세요.", "error")
            return redirect(url_for("index", **_chat_redirect_args(request.form)))

        created_id = add_chat_bookmark(
            DB_PATH,
            start_message_id=int(message_id_raw),
            title=title,
        )
        if not created_id:
            flash("책갈피 저장에 실패했습니다. (메시지를 찾지 못함)", "error")
            return redirect(url_for("index", **_chat_redirect_args(request.form)))

        args = _chat_redirect_args(request.form)
        args["bookmark"] = str(created_id)
        flash("책갈피를 저장했습니다.", "ok")
        return redirect(url_for("index", **args))

    @app.post("/chat/bookmarks/<int:bookmark_id>/rename")
    def chat_bookmark_rename(bookmark_id: int):
        _require_login()
        title = (request.form.get("title") or "").strip()
        if not title:
            flash("책갈피 이름을 입력해 주세요.", "error")
            return redirect(url_for("index", **_chat_redirect_args(request.form)))
        updated = update_chat_bookmark_title(DB_PATH, bookmark_id, title)
        if updated:
            flash("책갈피 이름을 변경했습니다.", "ok")
        else:
            flash("책갈피 이름 변경에 실패했습니다.", "error")
        args = _chat_redirect_args(request.form)
        args["bookmark"] = str(bookmark_id)
        return redirect(url_for("index", **args))

    @app.post("/chat/bookmarks/<int:bookmark_id>/delete")
    def chat_bookmark_delete(bookmark_id: int):
        _require_login()
        deleted = delete_chat_bookmark(DB_PATH, bookmark_id)
        if deleted:
            flash("책갈피를 삭제했습니다.", "ok")
        else:
            flash("삭제할 책갈피를 찾지 못했습니다.", "error")
        args = _chat_redirect_args(request.form)
        if args.get("bookmark") == str(bookmark_id):
            args.pop("bookmark", None)
        return redirect(url_for("index", **args))

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
        comments_by_entry = fetch_diary_comments(DB_PATH, [entry["id"] for entry in entries])
        photos_by_entry = fetch_diary_photos(DB_PATH, [entry["id"] for entry in entries])
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
            entry["body_html"] = _linkify_with_br(str(entry["body"] or ""))
            entry["time_ko"] = _format_entry_time(str(entry.get("created_at") or ""))
            comments = comments_by_entry.get(entry["id"], [])
            for comment in comments:
                comment["body_html"] = _linkify_with_br(str(comment.get("body") or ""))
                comment["created_at_display"] = _format_comment_ts(str(comment.get("created_at") or ""))
            entry["comments"] = comments
            entry["photos"] = photos_by_entry.get(entry["id"], [])
        return render_template(
            "diary.html",
            entries=entries,
            editing=editing,
            today=_today_seoul_date().isoformat(),
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
            entry_date_raw = _today_seoul_date().isoformat()
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

    @app.post("/diary/<int:entry_id>/comments")
    def diary_comment_post(entry_id: int):
        _require_login()
        body = (request.form.get("comment_body") or "").strip()
        if not body:
            flash("댓글 내용이 비어있습니다.", "error")
            return redirect(url_for("diary", **_diary_redirect_args(request.form)))
        if not get_diary_entry(DB_PATH, entry_id):
            flash("일기를 찾지 못했습니다.", "error")
            return redirect(url_for("diary", **_diary_redirect_args(request.form)))
        add_diary_comment(DB_PATH, entry_id, body)
        maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
        flash("댓글을 추가했습니다.", "ok")
        return redirect(url_for("diary", **_diary_redirect_args(request.form)))

    @app.post("/diary/comments/<int:comment_id>/delete")
    def diary_comment_delete(comment_id: int):
        _require_login()
        deleted = delete_diary_comment(DB_PATH, comment_id)
        if deleted:
            maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
            flash("댓글을 삭제했습니다.", "ok")
        else:
            flash("댓글을 찾지 못했습니다.", "error")
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
            entry_date_raw = _today_seoul_date().isoformat()
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
        photos = fetch_diary_photos(DB_PATH, [entry_id]).get(entry_id, [])
        deleted = delete_diary_entry(DB_PATH, entry_id)
        if deleted:
            for photo in photos:
                drive_file_id = str(photo.get("drive_file_id") or "")
                if not drive_file_id:
                    continue
                try:
                    delete_drive_file(drive_file_id)
                except Exception:
                    flash(f"사진 삭제 실패: {photo.get('file_name') or drive_file_id}", "error")
            maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
            flash("일기를 삭제했습니다.", "ok")
        else:
            flash("삭제할 일기를 찾지 못했습니다.", "error")
        return redirect(url_for("diary", **_diary_redirect_args(request.form)))

    @app.post("/diary/photos/<int:photo_id>/delete")
    def diary_photo_delete(photo_id: int):
        _require_login()
        photo = get_diary_photo(DB_PATH, photo_id)
        if not photo:
            flash("삭제할 사진을 찾지 못했습니다.", "error")
            return redirect(url_for("diary", **_diary_redirect_args(request.form)))
        deleted = delete_diary_photo(DB_PATH, photo_id)
        if deleted:
            drive_file_id = str(photo.get("drive_file_id") or "")
            if drive_file_id:
                try:
                    delete_drive_file(drive_file_id)
                except Exception:
                    flash("Drive에서 사진 삭제에 실패했습니다.", "error")
            maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
            flash("사진을 삭제했습니다.", "ok")
        else:
            flash("삭제할 사진을 찾지 못했습니다.", "error")
        return redirect(url_for("diary", **_diary_redirect_args(request.form)))

    @app.get("/diary/media/<file_id>")
    def diary_media(file_id: str):
        _require_login()
        try:
            content, mime_type = download_drive_file(file_id)
        except DriveConfigError:
            abort(404)
        except Exception:
            abort(404)
        return send_file(io.BytesIO(content), mimetype=mime_type)

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
        comments_by_entry = fetch_diary_comments(DB_PATH, [entry["id"] for entry in entries])
        for entry in entries:
            entry["comments"] = comments_by_entry.get(entry["id"], [])
        if fmt == "csv":
            content = build_export_header("diary", "csv") + serialize_diary_csv(entries)
            content_type = "text/csv; charset=utf-8"
            ext = "csv"
        elif fmt == "md":
            content = serialize_diary_markdown(entries)
            content_type = "text/markdown; charset=utf-8"
            ext = "md"
        else:
            content = build_export_header("diary", "txt") + serialize_diary_plain(entries)
            content_type = "text/plain; charset=utf-8"
            ext = "txt"

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"diary_export_{stamp}.{ext}"
        return _download_response(content, content_type, filename)

    @app.get("/calendar")
    def calendar():
        _require_login()
        month_raw = (request.args.get("month") or "").strip()
        parsed = _parse_year_month(month_raw)
        today = _today_seoul_date()
        if month_raw and not parsed:
            flash("달력 월 형식이 올바르지 않습니다. (YYYY-MM)", "error")
        if parsed:
            year, month = parsed
        else:
            year, month = today.year, today.month

        first_weekday_mon0, days_in_month = pycalendar.monthrange(year, month)
        month_start = f"{year:04d}-{month:02d}-01"
        month_end = f"{year:04d}-{month:02d}-{days_in_month:02d}"
        entries = fetch_diary_entries(
            DB_PATH,
            limit=None,
            start_date=month_start,
            end_date=month_end,
            order="asc",
        )

        entries_by_day: dict[int, list[dict]] = {}
        for entry in entries:
            entry_date_raw = str(entry.get("entry_date") or "")
            entry_date = _parse_iso_date(entry_date_raw)
            if not entry_date:
                continue
            day_items = entries_by_day.setdefault(entry_date.day, [])
            day_items.append(
                {
                    "id": int(entry["id"]),
                    "title": str(entry.get("title") or "무제"),
                    "entry_date": entry_date.isoformat(),
                }
            )

        # monthrange is Monday=0; calendar UI uses Sunday-first.
        lead_blanks = (first_weekday_mon0 + 1) % 7
        cells: list[dict] = []
        for _ in range(lead_blanks):
            cells.append({"day": None, "date_iso": "", "entries": []})
        for day in range(1, days_in_month + 1):
            date_iso = f"{year:04d}-{month:02d}-{day:02d}"
            cells.append(
                {
                    "day": day,
                    "date_iso": date_iso,
                    "is_today": date_iso == today.isoformat(),
                    "entries": entries_by_day.get(day, []),
                }
            )
        while len(cells) % 7 != 0:
            cells.append({"day": None, "date_iso": "", "entries": []})

        prev_year, prev_month = _shift_year_month(year, month, -1)
        next_year, next_month = _shift_year_month(year, month, 1)
        return render_template(
            "calendar.html",
            current_month=f"{year:04d}-{month:02d}",
            month_label=f"{year}년 {month}월",
            prev_month=f"{prev_year:04d}-{prev_month:02d}",
            next_month=f"{next_year:04d}-{next_month:02d}",
            today_month=f"{today.year:04d}-{today.month:02d}",
            cells=cells,
            weekdays=["일", "월", "화", "수", "목", "금", "토"],
        )

    @app.get("/todo")
    def todo():
        _require_login()
        edit_id_raw = (request.args.get("edit") or "").strip()
        editing = None
        if edit_id_raw.isdigit():
            editing = get_todo_item(DB_PATH, int(edit_id_raw))
            if not editing:
                flash("수정할 항목을 찾지 못했습니다.", "error")
            else:
                editing["tags_value"] = str(editing.get("tags") or "")
        elif edit_id_raw:
            flash("수정할 항목 번호가 올바르지 않습니다.", "error")

        daily, active_pending, active_done = fetch_todo_items(DB_PATH)
        for item in daily:
            item["body_html"] = _escape_with_br(str(item.get("body") or ""))
            item["done_today"] = bool(str(item.get("today_completed_at") or "").strip())
            item["today_completed_at_display"] = _format_comment_ts(str(item.get("today_completed_at") or ""))
        for item in active_pending:
            item["body_html"] = _escape_with_br(str(item.get("body") or ""))
            item["tags_list"] = _split_tags(str(item.get("tags") or ""))
        for item in active_done:
            item["body_html"] = _escape_with_br(str(item.get("body") or ""))
            item["completed_at_display"] = _format_comment_ts(str(item.get("completed_at") or ""))
            item["tags_list"] = _split_tags(str(item.get("tags") or ""))
        return render_template(
            "todo.html",
            daily=daily,
            active_pending=active_pending,
            active_done=active_done,
            editing=editing,
        )

    @app.post("/todo/daily")
    def todo_daily_post():
        _require_login()
        body = (request.form.get("body") or "").strip()
        if not body:
            flash("내용이 비어있습니다.", "error")
            return redirect(url_for("todo"))
        add_todo_item(DB_PATH, body, kind="daily")
        maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
        return redirect(url_for("todo"))

    @app.post("/todo/active")
    def todo_active_post():
        _require_login()
        body = (request.form.get("body") or "").strip()
        tags_raw = (request.form.get("tags") or "").strip()
        if not body:
            flash("내용이 비어있습니다.", "error")
            return redirect(url_for("todo"))
        tags_clean, _tags_list = _todo_tags_from_input(tags_raw)
        add_todo_item(DB_PATH, body, kind="active", tags=tags_clean)
        maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
        return redirect(url_for("todo"))

    @app.post("/todo/<int:item_id>/complete")
    def todo_complete(item_id: int):
        _require_login()
        complete_todo_item(DB_PATH, item_id)
        maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
        return redirect(url_for("todo"))

    @app.post("/todo/daily/<int:item_id>/check")
    def todo_daily_check(item_id: int):
        _require_login()
        check_todo_daily_item(DB_PATH, item_id)
        maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
        return redirect(url_for("todo"))

    @app.post("/todo/<int:item_id>/edit")
    def todo_edit(item_id: int):
        _require_login()
        body = (request.form.get("body") or "").strip()
        tags_raw = (request.form.get("tags") or "").strip()
        if not body:
            flash("내용이 비어있습니다.", "error")
            return redirect(url_for("todo", edit=item_id))
        tags_clean, _tags_list = _todo_tags_from_input(tags_raw)
        updated = update_todo_item(DB_PATH, item_id, body, tags=tags_clean)
        if not updated:
            flash("수정할 항목을 찾지 못했습니다.", "error")
        else:
            maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
            flash("항목을 수정했습니다.", "ok")
        return redirect(url_for("todo"))

    @app.post("/todo/<int:item_id>/delete")
    def todo_delete(item_id: int):
        _require_login()
        deleted = delete_todo_item(DB_PATH, item_id)
        if not deleted:
            flash("삭제할 항목을 찾지 못했습니다.", "error")
        else:
            maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
            flash("항목을 삭제했습니다.", "ok")
        return redirect(url_for("todo"))

    @app.get("/memories")
    def memories():
        _require_login()
        q = (request.args.get("q") or "").strip()
        album = (request.args.get("album") or "").strip()
        tag = (request.args.get("tag") or "").strip()
        start_date_raw = (request.args.get("start_date") or "").strip()
        end_date_raw = (request.args.get("end_date") or "").strip()
        edit_id_raw = (request.args.get("edit") or "").strip()

        start_date = _parse_iso_date(start_date_raw)
        end_date = _parse_iso_date(end_date_raw)
        if start_date_raw and not start_date:
            flash("시작 날짜 형식이 올바르지 않습니다.", "error")
        if end_date_raw and not end_date:
            flash("종료 날짜 형식이 올바르지 않습니다.", "error")

        photos = fetch_memory_photos(
            DB_PATH,
            q=q or None,
            album=album or None,
            tag=tag or None,
            start_date=start_date.isoformat() if start_date else None,
            end_date=end_date.isoformat() if end_date else None,
            limit=200,
            order="desc",
        )
        albums = fetch_memory_albums(DB_PATH)

        editing = None
        if edit_id_raw.isdigit():
            editing = get_memory_photo(DB_PATH, int(edit_id_raw))
            if not editing:
                flash("수정할 사진을 찾지 못했습니다.", "error")
        elif edit_id_raw:
            flash("수정할 사진 번호가 올바르지 않습니다.", "error")

        for photo in photos:
            date_raw = str(photo.get("taken_date") or "")
            date_obj = _parse_iso_date(date_raw)
            photo["date_ko"] = _format_ko_date(date_obj) if date_obj else date_raw
            photo["tags_list"] = _split_tags(str(photo.get("tags") or ""))

        drive_ready, drive_hint = get_drive_config_status()

        return render_template(
            "memories.html",
            photos=photos,
            albums=albums,
            editing=editing,
            search_query=q,
            selected_album=album,
            tag_query=tag,
            start_date=start_date.isoformat() if start_date else start_date_raw,
            end_date=end_date.isoformat() if end_date else end_date_raw,
            drive_ready=drive_ready,
            drive_hint=drive_hint,
            today=date.today().isoformat(),
        )

    @app.post("/memories/upload")
    def memories_upload():
        _require_login()
        files = request.files.getlist("files")
        if not files:
            flash("업로드할 파일이 없습니다.", "error")
            return redirect(url_for("memories", **_memories_redirect_args(request.form)))

        caption = (request.form.get("caption") or "").strip()
        album = (request.form.get("album") or "").strip()
        tags = (request.form.get("tags") or "").strip()
        taken_date_raw = (request.form.get("taken_date") or "").strip()
        taken_date = _parse_iso_date(taken_date_raw)

        if taken_date_raw and not taken_date:
            flash("날짜 형식이 올바르지 않습니다.", "error")
            return redirect(url_for("memories", **_memories_redirect_args(request.form)))

        try:
            folder_id = get_drive_folder_id()
        except DriveConfigError as exc:
            flash(str(exc), "error")
            return redirect(url_for("memories", **_memories_redirect_args(request.form)))

        uploaded = 0
        for file_storage in files:
            if not file_storage or not file_storage.filename:
                continue
            try:
                created = upload_drive_file(file_storage, folder_id)
            except Exception as exc:
                flash(f"업로드 실패: {file_storage.filename}", "error")
                continue
            add_memory_photo(
                DB_PATH,
                drive_file_id=created.file_id,
                file_name=created.name,
                mime_type=created.mime_type,
                caption=caption,
                album=album,
                tags=tags,
                taken_date=(taken_date.isoformat() if taken_date else (created.created_time or ""))[:10],
            )
            uploaded += 1

        if uploaded:
            flash(f"업로드 완료: {uploaded}개", "ok")
        return redirect(url_for("memories", **_memories_redirect_args(request.form)))

    @app.post("/memories/sync")
    def memories_sync():
        _require_login()
        try:
            folder_id = get_drive_folder_id()
            files = list_drive_images(folder_id)
        except DriveConfigError as exc:
            flash(str(exc), "error")
            return redirect(url_for("memories", **_memories_redirect_args(request.form)))
        except Exception:
            flash("드라이브 동기화에 실패했습니다.", "error")
            return redirect(url_for("memories", **_memories_redirect_args(request.form)))

        inserted = 0
        for item in files:
            added = upsert_memory_photo(
                DB_PATH,
                drive_file_id=item.file_id,
                file_name=item.name,
                mime_type=item.mime_type,
                taken_date=(item.created_time or "")[:10] if item.created_time else None,
            )
            if added:
                inserted += 1
        flash(f"동기화 완료: {inserted}개 추가", "ok")
        return redirect(url_for("memories", **_memories_redirect_args(request.form)))

    @app.post("/memories/<int:photo_id>/edit")
    def memories_edit(photo_id: int):
        _require_login()
        caption = (request.form.get("caption") or "").strip()
        album = (request.form.get("album") or "").strip()
        tags = (request.form.get("tags") or "").strip()
        taken_date_raw = (request.form.get("taken_date") or "").strip()
        taken_date = _parse_iso_date(taken_date_raw)
        if taken_date_raw and not taken_date:
            flash("날짜 형식이 올바르지 않습니다.", "error")
            return redirect(url_for("memories", edit=photo_id, **_memories_redirect_args(request.form)))

        updated = update_memory_photo(
            DB_PATH,
            photo_id,
            caption=caption,
            album=album,
            tags=tags,
            taken_date=taken_date.isoformat() if taken_date else None,
        )
        if not updated:
            flash("수정할 사진을 찾지 못했습니다.", "error")
        else:
            flash("사진 정보를 수정했습니다.", "ok")
        return redirect(url_for("memories", **_memories_redirect_args(request.form)))

    @app.post("/memories/<int:photo_id>/delete")
    def memories_delete(photo_id: int):
        _require_login()
        delete_drive = (request.form.get("delete_drive") or "").strip() == "1"
        photo = get_memory_photo(DB_PATH, photo_id)
        if not photo:
            flash("삭제할 사진을 찾지 못했습니다.", "error")
            return redirect(url_for("memories", **_memories_redirect_args(request.form)))
        deleted = delete_memory_photo(DB_PATH, photo_id)
        if deleted:
            if delete_drive:
                try:
                    delete_drive_file(str(photo.get("drive_file_id")))
                except Exception:
                    flash("드라이브 파일 삭제에 실패했습니다.", "error")
            flash("사진을 삭제했습니다.", "ok")
        return redirect(url_for("memories", **_memories_redirect_args(request.form)))

    @app.get("/memories/<int:photo_id>")
    def memories_detail(photo_id: int):
        _require_login()
        photo = get_memory_photo(DB_PATH, photo_id)
        if not photo:
            abort(404)
        date_raw = str(photo.get("taken_date") or "")
        date_obj = _parse_iso_date(date_raw)
        photo["date_ko"] = _format_ko_date(date_obj) if date_obj else date_raw
        photo["tags_list"] = _split_tags(str(photo.get("tags") or ""))
        return render_template(
            "memories_detail.html",
            photo=photo,
            back_query=_memories_redirect_args(request.args),
        )

    @app.get("/memories/media/<file_id>")
    def memories_media(file_id: str):
        _require_login()
        try:
            content, mime_type = download_drive_file(file_id)
        except DriveConfigError:
            abort(404)
        except Exception:
            abort(404)
        return send_file(io.BytesIO(content), mimetype=mime_type)

    @app.post("/me")
    def set_me():
        _require_login()
        name = (request.form.get("me_name") or "").strip()
        if name:
            session["me_name"] = name
        else:
            session.pop("me_name", None)
        return redirect(url_for("index"))

    @app.get("/admin/export")
    def admin_export():
        _require_login()
        return render_template("export.html")

    @app.get("/admin/export/chat")
    def admin_export_chat():
        _require_login()
        fmt = (request.args.get("format") or "txt").strip().lower()
        q = (request.args.get("q") or "").strip()
        if q:
            messages = search_messages(DB_PATH, q, limit=5000)
        else:
            messages = fetch_messages(DB_PATH, limit=None, before_dt=None, order="asc")

        if fmt == "kakao":
            content = serialize_chat_kakao(messages, include_header=True)
            content_type = "text/plain; charset=utf-8"
            ext = "txt"
        elif fmt == "csv":
            content = serialize_chat_csv(messages, include_header=True)
            content_type = "text/csv; charset=utf-8"
            ext = "csv"
        else:
            content = serialize_chat_plain(messages, include_header=True)
            content_type = "text/plain; charset=utf-8"
            ext = "txt"

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"chat_export_{stamp}.{ext}"
        return _download_response(content, content_type, filename)

    @app.get("/admin/export/memories")
    def admin_export_memories():
        _require_login()
        fmt = (request.args.get("format") or "csv").strip().lower()
        photos = fetch_memory_photos(
            DB_PATH,
            q=None,
            album=None,
            tag=None,
            start_date=None,
            end_date=None,
            limit=None,
            order="asc",
        )
        if fmt == "txt":
            content = serialize_memories_txt(photos, include_header=True)
            content_type = "text/plain; charset=utf-8"
            ext = "txt"
        else:
            content = serialize_memories_csv(photos, include_header=True)
            content_type = "text/csv; charset=utf-8"
            ext = "csv"
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"memories_export_{stamp}.{ext}"
        return _download_response(content, content_type, filename)

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

        meta, body = strip_export_header(text)
        import_kind = _detect_import_kind(text, request.form.get("import_kind"))
        format_hint = (meta.get("format") or "").strip().lower()

        if import_kind == "chat":
            msgs = []
            if format_hint == "csv":
                msgs = parse_chat_csv(text)
            elif format_hint == "txt":
                msgs = parse_chat_plain(text)
            elif format_hint == "kakao":
                msgs = parse_kakao_talk_txt(body)
            else:
                msgs = parse_chat_csv(text)
                if not msgs:
                    msgs = parse_chat_plain(text)
                if not msgs:
                    msgs = parse_kakao_talk_txt(body)

            if not msgs:
                flash("메시지를 찾지 못했습니다. (파일 형식을 확인하세요)", "error")
                return redirect(url_for("admin_import"))

            if meta.get("type") == "chat":
                result = import_messages(DB_PATH, msgs, source=source_label)
            else:
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

        if import_kind == "diary":
            entries = []
            if format_hint == "csv":
                entries = parse_diary_csv(text)
            elif format_hint == "txt":
                entries = parse_diary_plain(text)
            elif format_hint == "md":
                entries = parse_diary_markdown(text)
            else:
                entries = parse_diary_csv(text)
                if not entries:
                    entries = parse_diary_plain(text)
                if not entries:
                    entries = parse_diary_markdown(text)

            if not entries:
                flash("일기 내용을 찾지 못했습니다. (파일 형식을 확인하세요)", "error")
                return redirect(url_for("admin_import"))

            inserted_entries = 0
            skipped_entries = 0
            inserted_comments = 0
            skipped_comments = 0
            for entry in entries:
                upserted = upsert_diary_entry(
                    DB_PATH,
                    entry.entry_date,
                    entry.title,
                    entry.body,
                    created_at=entry.created_at,
                )
                if not upserted:
                    skipped_entries += 1
                    continue
                entry_id, was_inserted = upserted
                if was_inserted:
                    inserted_entries += 1
                else:
                    skipped_entries += 1

                for comment in entry.comments:
                    if not comment.body:
                        continue
                    added = upsert_diary_comment(
                        DB_PATH,
                        entry_id,
                        comment.body,
                        created_at=comment.created_at,
                    )
                    if added:
                        inserted_comments += 1
                    else:
                        skipped_comments += 1

            maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
            flash(
                "일기 가져오기 완료: "
                f"{inserted_entries}개 추가, {skipped_entries}개 중복 제외, "
                f"댓글 {inserted_comments}개 추가",
                "ok",
            )
            return redirect(url_for("diary"))

        if import_kind == "memories":
            rows = []
            if format_hint == "csv":
                rows = parse_memories_csv(text)
            elif format_hint == "txt":
                rows = parse_memories_txt(text)
            else:
                rows = parse_memories_csv(text)
                if not rows:
                    rows = parse_memories_txt(text)

            if not rows:
                flash("사진 데이터를 찾지 못했습니다. (파일 형식을 확인하세요)", "error")
                return redirect(url_for("admin_import"))

            inserted = 0
            for row in rows:
                if not row.get("drive_file_id"):
                    continue
                if not row.get("file_name"):
                    row["file_name"] = row.get("drive_file_id")
                added = upsert_memory_photo_full(
                    DB_PATH,
                    drive_file_id=str(row.get("drive_file_id")),
                    file_name=str(row.get("file_name") or ""),
                    mime_type=str(row.get("mime_type") or ""),
                    caption=str(row.get("caption") or ""),
                    album=str(row.get("album") or ""),
                    tags=str(row.get("tags") or ""),
                    taken_date=str(row.get("taken_date") or ""),
                    created_at=row.get("created_at"),
                    updated_at=row.get("updated_at"),
                )
                if added:
                    inserted += 1

            maybe_backup_to_github(DB_PATH, BASE_DIR, logger=app.logger)
            flash(f"사진 가져오기 완료: {inserted}개 추가", "ok")
            return redirect(url_for("memories"))

        flash("가져올 데이터를 찾지 못했습니다. (파일 형식을 확인하세요)", "error")
        return redirect(url_for("admin_import"))

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
