from __future__ import annotations

import base64
import csv
from dataclasses import dataclass
from datetime import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from storage import (
    fetch_diary_comments,
    fetch_diary_entries,
    fetch_messages,
    serialize_diary_plain,
)


_WEEKDAYS_KO = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"]


def _format_kakao_date(dt: datetime) -> str:
    weekday_ko = _WEEKDAYS_KO[dt.weekday()]
    return f"{dt.year}년 {dt.month}월 {dt.day}일 {weekday_ko}"


def _format_kakao_time(dt: datetime) -> str:
    ampm = "오전" if dt.hour < 12 else "오후"
    h12 = dt.hour % 12 or 12
    return f"{ampm} {h12}:{dt.minute:02d}"


def _export_kakao(messages: list[dict]) -> str:
    lines: list[str] = []
    current_date: str | None = None
    for msg in messages:
        dt = datetime.fromisoformat(str(msg["dt"]))
        date_key = dt.strftime("%Y-%m-%d")
        if current_date != date_key:
            current_date = date_key
            lines.append(f"--------------- {_format_kakao_date(dt)} ---------------")
        time_ko = _format_kakao_time(dt)
        text = str(msg["text"] or "")
        lines.append(f"[{msg['sender']}] [{time_ko}] {text}")
    if not lines:
        return ""
    return "\n".join(lines).rstrip() + "\n"


def _export_plain(messages: list[dict]) -> str:
    lines: list[str] = []
    for msg in messages:
        dt = datetime.fromisoformat(str(msg["dt"]))
        stamp = dt.strftime("%Y-%m-%d %H:%M")
        text = str(msg["text"] or "")
        lines.append(f"{stamp} | {msg['sender']} | {text}")
    if not lines:
        return ""
    return "\n".join(lines).rstrip() + "\n"


def _export_csv(messages: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["dt", "sender", "text"])
    for msg in messages:
        writer.writerow([msg["dt"], msg["sender"], msg["text"]])
    return buf.getvalue()


def _parse_github_repo(url: str) -> str | None:
    url = (url or "").strip()
    if not url:
        return None
    https_match = re.search(r"github\.com/(?P<repo>[^/]+/[^/]+?)(?:\.git)?$", url)
    if https_match:
        return https_match.group("repo")
    ssh_match = re.search(r"git@github\.com:(?P<repo>[^/]+/[^/]+?)(?:\.git)?$", url)
    if ssh_match:
        return ssh_match.group("repo")
    return None


def _read_git_repo_from_config(base_dir: Path) -> str | None:
    config_path = base_dir / ".git" / "config"
    if not config_path.exists():
        return None
    try:
        text = config_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("url ="):
            repo = _parse_github_repo(line.split("=", 1)[1].strip())
            if repo:
                return repo
    return None


@dataclass(frozen=True)
class GitHubBackupConfig:
    token: str
    repo: str
    branch: str
    prefix: str


_PERIODIC_BACKUP_LOCK = threading.Lock()
_PERIODIC_BACKUP_STARTED = False
_LAST_BACKUP_SIGNATURE_LOCK = threading.Lock()
_LAST_BACKUP_SIGNATURE: str | None = None
_BACKUP_STATE_LOCK = threading.Lock()
_CHAT_PARTS_HEADER = "# CHAT_BACKUP_PARTS v1"


def _get_backup_config(base_dir: Path) -> GitHubBackupConfig | None:
    token = os.getenv("CHAT_APP_GITHUB_TOKEN", "").strip().strip('"').strip("'")
    if not token:
        return None
    repo = os.getenv("CHAT_APP_GITHUB_REPO", "").strip()
    if repo:
        repo = _parse_github_repo(repo) or repo
    if not repo:
        repo = _read_git_repo_from_config(base_dir) or ""
    if not repo:
        return None
    branch = os.getenv("CHAT_APP_GITHUB_BRANCH", "main").strip() or "main"
    prefix = os.getenv("CHAT_APP_GITHUB_BACKUP_PREFIX", "backup/chat_export").strip()
    if not prefix:
        prefix = "backup/chat_export"
    return GitHubBackupConfig(token=token, repo=repo, branch=branch, prefix=prefix.lstrip("/"))


def _github_request(
    method: str,
    url: str,
    token: str,
    *,
    data: bytes | None = None,
) -> object:
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Authorization", f"token {token}")
    req.add_header("User-Agent", "chat-backup")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = resp.read()
    if not payload:
        return {}
    return json.loads(payload.decode("utf-8"))


def _github_get_file_sha(cfg: GitHubBackupConfig, path: str) -> str | None:
    url = f"https://api.github.com/repos/{cfg.repo}/contents/{path}?ref={cfg.branch}"
    try:
        data = _github_request("GET", url, cfg.token)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    if not isinstance(data, dict):
        return None
    return str(data.get("sha") or "") or None


def _github_put_file(cfg: GitHubBackupConfig, path: str, content: str, message: str) -> None:
    url = f"https://api.github.com/repos/{cfg.repo}/contents/{path}"
    sha = _github_get_file_sha(cfg, path)
    payload = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "branch": cfg.branch,
    }
    if sha:
        payload["sha"] = sha
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    _github_request("PUT", url, cfg.token, data=data)


def _github_get_file_text(cfg: GitHubBackupConfig, path: str, ref: str | None = None) -> str | None:
    use_ref = (ref or "").strip() or cfg.branch
    url = f"https://api.github.com/repos/{cfg.repo}/contents/{path}?ref={use_ref}"
    try:
        data = _github_request("GET", url, cfg.token)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    if not isinstance(data, dict):
        return None
    encoded = str(data.get("content") or "")
    if not encoded:
        return ""
    try:
        raw = base64.b64decode(encoded.encode("ascii"), validate=False)
    except Exception:
        return None
    return raw.decode("utf-8", errors="replace")


def _get_chat_chunk_bytes() -> int:
    raw = os.getenv("CHAT_APP_GITHUB_CHAT_CHUNK_BYTES", "").strip()
    if not raw:
        return 700_000
    try:
        value = int(raw)
    except ValueError:
        return 700_000
    if value <= 0:
        return 700_000
    return max(20_000, value)


def _split_text_chunks_by_bytes(text: str, max_bytes: int) -> list[str]:
    if max_bytes <= 0:
        return [text]
    if not text:
        return [""]

    chunks: list[str] = []
    current_lines: list[str] = []
    current_size = 0

    def flush_current() -> None:
        nonlocal current_lines, current_size
        if current_lines:
            chunks.append("".join(current_lines))
            current_lines = []
            current_size = 0

    def append_long_line_segments(line: str) -> None:
        segment_chars: list[str] = []
        segment_size = 0
        for ch in line:
            ch_size = len(ch.encode("utf-8"))
            if segment_chars and segment_size + ch_size > max_bytes:
                chunks.append("".join(segment_chars))
                segment_chars = [ch]
                segment_size = ch_size
            else:
                segment_chars.append(ch)
                segment_size += ch_size
        if segment_chars:
            chunks.append("".join(segment_chars))

    for line in text.splitlines(keepends=True):
        line_size = len(line.encode("utf-8"))
        if line_size > max_bytes:
            flush_current()
            append_long_line_segments(line)
            continue

        if current_lines and current_size + line_size > max_bytes:
            flush_current()
        current_lines.append(line)
        current_size += line_size

    flush_current()
    return chunks or [""]


def _build_chat_parts_manifest(part_paths: list[str]) -> str:
    payload = {"parts": part_paths}
    return f"{_CHAT_PARTS_HEADER}\n{json.dumps(payload, ensure_ascii=False)}\n"


def _parse_chat_parts_manifest(text: str) -> list[str] | None:
    lines = text.splitlines()
    if not lines or lines[0].strip() != _CHAT_PARTS_HEADER:
        return None
    body = "\n".join(lines[1:]).strip()
    if not body:
        return None
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None
    raw_parts = payload.get("parts")
    if not isinstance(raw_parts, list) or not raw_parts:
        return None
    parts: list[str] = []
    for item in raw_parts:
        path = str(item or "").strip().lstrip("/")
        if path:
            parts.append(path)
    return parts or None


def _upload_chat_backup(cfg: GitHubBackupConfig, prefix: str, chat_plain: str, message: str, logger=None) -> None:
    chunk_bytes = _get_chat_chunk_bytes()
    total_size = len(chat_plain.encode("utf-8"))

    if total_size <= chunk_bytes:
        _github_put_file(cfg, f"{prefix}.txt", chat_plain, message)
        return

    chunks = _split_text_chunks_by_bytes(chat_plain, chunk_bytes)
    part_paths: list[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        part_path = f"{prefix}.part{idx:03d}.txt"
        _github_put_file(cfg, part_path, chunk, message)
        part_paths.append(part_path)

    manifest = _build_chat_parts_manifest(part_paths)
    _github_put_file(cfg, f"{prefix}.txt", manifest, message)
    if logger:
        try:
            logger.info(
                "Uploaded chunked chat backup: %d parts (size=%d bytes, chunk=%d bytes).",
                len(part_paths),
                total_size,
                chunk_bytes,
            )
        except Exception:
            pass


def _download_chat_backup(cfg: GitHubBackupConfig, prefix: str, ref: str | None = None) -> str | None:
    raw = _github_get_file_text(cfg, f"{prefix}.txt", ref=ref)
    if raw is None:
        return None
    part_paths = _parse_chat_parts_manifest(raw)
    if not part_paths:
        return raw

    parts: list[str] = []
    for part_path in part_paths:
        part_text = _github_get_file_text(cfg, part_path, ref=ref)
        if part_text is None:
            return None
        parts.append(part_text)
    return "".join(parts)


def fetch_backup_texts_from_github(
    base_dir: Path,
    logger=None,
    *,
    ref: str | None = None,
) -> tuple[str | None, str | None]:
    cfg = _get_backup_config(base_dir)
    if not cfg:
        return None, None
    prefix = cfg.prefix
    try:
        chat_text = _download_chat_backup(cfg, prefix, ref=ref)
        diary_text = _github_get_file_text(cfg, f"{prefix}_diary.txt", ref=ref)
        return chat_text, diary_text
    except Exception as exc:
        if logger:
            try:
                logger.exception("GitHub backup download failed: %s", exc)
            except Exception:
                pass
        return None, None


def list_backup_versions(base_dir: Path, *, limit: int = 20, logger=None) -> list[dict[str, str]]:
    cfg = _get_backup_config(base_dir)
    if not cfg:
        return []

    per_page = max(1, min(int(limit), 100))
    path = f"{cfg.prefix}.txt"
    params = urllib.parse.urlencode(
        {
            "sha": cfg.branch,
            "path": path,
            "per_page": str(per_page),
        }
    )
    url = f"https://api.github.com/repos/{cfg.repo}/commits?{params}"

    try:
        data = _github_request("GET", url, cfg.token)
    except Exception as exc:
        if logger:
            try:
                logger.exception("GitHub backup history fetch failed: %s", exc)
            except Exception:
                pass
        return []

    if not isinstance(data, list):
        return []

    versions: list[dict[str, str]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        sha = str(row.get("sha") or "").strip()
        if not sha:
            continue
        commit = row.get("commit") if isinstance(row.get("commit"), dict) else {}
        author = commit.get("author") if isinstance(commit.get("author"), dict) else {}
        message = str(commit.get("message") or "").strip()
        created_at = str(author.get("date") or "").strip()
        versions.append(
            {
                "sha": sha,
                "short_sha": sha[:7],
                "message": message.splitlines()[0][:120] if message else "",
                "created_at": created_at,
            }
        )
    return versions


def _read_latest_remote_counts(cfg: GitHubBackupConfig) -> tuple[int | None, int | None]:
    path = f"{cfg.prefix}.txt"
    params = urllib.parse.urlencode(
        {
            "sha": cfg.branch,
            "path": path,
            "per_page": "1",
        }
    )
    url = f"https://api.github.com/repos/{cfg.repo}/commits?{params}"
    try:
        data = _github_request("GET", url, cfg.token)
    except Exception:
        return None, None
    if not isinstance(data, list) or not data:
        return None, None
    row = data[0]
    if not isinstance(row, dict):
        return None, None
    commit = row.get("commit") if isinstance(row.get("commit"), dict) else {}
    message = str(commit.get("message") or "")
    m = re.search(r"\(chat\s+(\d+),\s*diary\s+(\d+)\)", message)
    if not m:
        return None, None
    try:
        return int(m.group(1)), int(m.group(2))
    except ValueError:
        return None, None


def _backup_state_path(db_path: Path, cfg: GitHubBackupConfig) -> Path:
    state_key = hashlib.sha256(f"{cfg.repo}:{cfg.branch}:{cfg.prefix}".encode("utf-8")).hexdigest()[:10]
    return db_path.parent / f"github_backup_state_{state_key}.json"


def _load_backup_state(db_path: Path, cfg: GitHubBackupConfig) -> dict[str, object]:
    path = _backup_state_path(db_path, cfg)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    return raw


def _save_backup_state(
    db_path: Path,
    cfg: GitHubBackupConfig,
    *,
    chat_count: int,
    diary_count: int,
    signature: str,
) -> None:
    path = _backup_state_path(db_path, cfg)
    payload = {
        "chat_count": int(chat_count),
        "diary_count": int(diary_count),
        "signature": signature,
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp_path.replace(path)


def _as_int(value: object) -> int | None:
    try:
        return int(str(value))
    except Exception:
        return None


def _should_skip_backup_on_decrease(
    state: dict[str, object],
    *,
    chat_count: int,
    diary_count: int,
) -> bool:
    prev_chat = _as_int(state.get("chat_count"))
    prev_diary = _as_int(state.get("diary_count"))
    if prev_chat is None or prev_diary is None:
        return False
    return chat_count < prev_chat or diary_count < prev_diary


def _compute_backup_signature(
    cfg: GitHubBackupConfig,
    chat_plain: str,
    diary_plain: str,
) -> str:
    hasher = hashlib.sha256()
    hasher.update(cfg.repo.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(cfg.branch.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(cfg.prefix.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(chat_plain.encode("utf-8"))
    hasher.update(b"\0")
    hasher.update(diary_plain.encode("utf-8"))
    return hasher.hexdigest()


def _get_periodic_backup_interval_seconds() -> float:
    raw = os.getenv("CHAT_APP_GITHUB_BACKUP_INTERVAL_MINUTES", "").strip()
    if not raw:
        return 43200.0
    try:
        minutes = float(raw)
    except ValueError:
        return 0.0
    if minutes <= 0:
        return 0.0
    return max(60.0, minutes * 60.0)


def maybe_backup_to_github(db_path: Path, base_dir: Path, logger=None, *, force: bool = False) -> bool:
    global _LAST_BACKUP_SIGNATURE
    cfg = _get_backup_config(base_dir)
    if not cfg:
        return False

    try:
        messages = fetch_messages(db_path, limit=None, before_dt=None, order="asc")
        diary_entries = fetch_diary_entries(db_path, limit=None, order="asc")
        comments_by_entry = fetch_diary_comments(db_path, [entry["id"] for entry in diary_entries])
        for entry in diary_entries:
            entry["comments"] = comments_by_entry.get(entry["id"], [])
        chat_plain = _export_plain(messages)
        diary_plain = serialize_diary_plain(diary_entries)
        chat_count = len(messages)
        diary_count = len(diary_entries)
        signature = _compute_backup_signature(cfg, chat_plain, diary_plain)

        with _BACKUP_STATE_LOCK:
            state = _load_backup_state(db_path, cfg)
            if _as_int(state.get("chat_count")) is None or _as_int(state.get("diary_count")) is None:
                remote_chat, remote_diary = _read_latest_remote_counts(cfg)
                if remote_chat is not None and remote_diary is not None:
                    state["chat_count"] = remote_chat
                    state["diary_count"] = remote_diary
            if not force and _should_skip_backup_on_decrease(
                state,
                chat_count=chat_count,
                diary_count=diary_count,
            ):
                if logger:
                    try:
                        logger.warning(
                            "Auto backup skipped due to count decrease (chat %s->%s, diary %s->%s).",
                            state.get("chat_count"),
                            chat_count,
                            state.get("diary_count"),
                            diary_count,
                        )
                    except Exception:
                        pass
                return False

            state_signature = str(state.get("signature") or "").strip()
            with _LAST_BACKUP_SIGNATURE_LOCK:
                if _LAST_BACKUP_SIGNATURE == signature or state_signature == signature:
                    return False

        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        message = f"backup {ts} (chat {chat_count}, diary {diary_count})"
        prefix = cfg.prefix
        _upload_chat_backup(cfg, prefix, chat_plain, message, logger=logger)
        _github_put_file(cfg, f"{prefix}_diary.txt", diary_plain, message)
        with _BACKUP_STATE_LOCK:
            _save_backup_state(
                db_path,
                cfg,
                chat_count=chat_count,
                diary_count=diary_count,
                signature=signature,
            )
            with _LAST_BACKUP_SIGNATURE_LOCK:
                _LAST_BACKUP_SIGNATURE = signature
        return True
    except Exception as exc:
        if logger:
            try:
                logger.exception("GitHub backup failed: %s", exc)
            except Exception:
                pass
        return False


def start_periodic_github_backup(db_path: Path, base_dir: Path, logger=None) -> bool:
    global _PERIODIC_BACKUP_STARTED
    interval_seconds = _get_periodic_backup_interval_seconds()
    if interval_seconds <= 0:
        return False

    cfg = _get_backup_config(base_dir)
    if not cfg:
        if logger:
            try:
                logger.warning(
                    "Periodic GitHub backup disabled: set CHAT_APP_GITHUB_TOKEN and CHAT_APP_GITHUB_REPO."
                )
            except Exception:
                pass
        return False

    with _PERIODIC_BACKUP_LOCK:
        if _PERIODIC_BACKUP_STARTED:
            return True
        _PERIODIC_BACKUP_STARTED = True

    interval_minutes = interval_seconds / 60.0

    def _loop() -> None:
        if logger:
            try:
                logger.info(
                    "Periodic GitHub backup started: every %.2f minutes (%s/%s).",
                    interval_minutes,
                    cfg.repo,
                    cfg.branch,
                )
            except Exception:
                pass
        while True:
            started = time.monotonic()
            maybe_backup_to_github(db_path, base_dir, logger=logger)
            elapsed = time.monotonic() - started
            wait_seconds = max(5.0, interval_seconds - elapsed)
            time.sleep(wait_seconds)

    thread = threading.Thread(target=_loop, name="github-backup-loop", daemon=True)
    thread.start()
    return True
