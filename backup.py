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
) -> dict:
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


def _github_get_file_text(cfg: GitHubBackupConfig, path: str) -> str | None:
    url = f"https://api.github.com/repos/{cfg.repo}/contents/{path}?ref={cfg.branch}"
    try:
        data = _github_request("GET", url, cfg.token)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    encoded = str(data.get("content") or "")
    if not encoded:
        return ""
    try:
        raw = base64.b64decode(encoded.encode("ascii"), validate=False)
    except Exception:
        return None
    return raw.decode("utf-8", errors="replace")


def fetch_backup_texts_from_github(base_dir: Path, logger=None) -> tuple[str | None, str | None]:
    cfg = _get_backup_config(base_dir)
    if not cfg:
        return None, None
    prefix = cfg.prefix
    try:
        chat_text = _github_get_file_text(cfg, f"{prefix}.txt")
        diary_text = _github_get_file_text(cfg, f"{prefix}_diary.txt")
        return chat_text, diary_text
    except Exception as exc:
        if logger:
            try:
                logger.exception("GitHub backup download failed: %s", exc)
            except Exception:
                pass
        return None, None


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
        return 600.0
    try:
        minutes = float(raw)
    except ValueError:
        return 0.0
    if minutes <= 0:
        return 0.0
    return max(60.0, minutes * 60.0)


def maybe_backup_to_github(db_path: Path, base_dir: Path, logger=None) -> bool:
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
        signature = _compute_backup_signature(cfg, chat_plain, diary_plain)
        with _LAST_BACKUP_SIGNATURE_LOCK:
            if _LAST_BACKUP_SIGNATURE == signature:
                return False
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        message = f"backup {ts} (chat {len(messages)}, diary {len(diary_entries)})"
        prefix = cfg.prefix
        _github_put_file(cfg, f"{prefix}.txt", chat_plain, message)
        _github_put_file(cfg, f"{prefix}_diary.txt", diary_plain, message)
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
