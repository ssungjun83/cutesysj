from __future__ import annotations

import base64
import csv
from dataclasses import dataclass
from datetime import datetime
import io
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.request

from storage import (
    fetch_diary_entries,
    fetch_messages,
    serialize_diary_csv,
    serialize_diary_markdown,
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


def maybe_backup_to_github(db_path: Path, base_dir: Path, logger=None) -> bool:
    cfg = _get_backup_config(base_dir)
    if not cfg:
        return False

    try:
        messages = fetch_messages(db_path, limit=None, before_dt=None, order="asc")
        diary_entries = fetch_diary_entries(db_path, limit=None, order="asc")
        exported = {
            "plain": _export_plain(messages),
            "kakao": _export_kakao(messages),
            "csv": _export_csv(messages),
        }
        diary_exported = {
            "plain": serialize_diary_plain(diary_entries),
            "csv": serialize_diary_csv(diary_entries),
            "md": serialize_diary_markdown(diary_entries),
        }
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
        message = f"backup {ts} (chat {len(messages)}, diary {len(diary_entries)})"
        prefix = cfg.prefix
        paths = {
            "plain": f"{prefix}.txt",
            "kakao": f"{prefix}_kakao.txt",
            "csv": f"{prefix}.csv",
            "diary_plain": f"{prefix}_diary.txt",
            "diary_csv": f"{prefix}_diary.csv",
            "diary_md": f"{prefix}_diary.md",
        }
        for key, path in paths.items():
            if key.startswith("diary_"):
                diary_key = key.replace("diary_", "", 1)
                _github_put_file(cfg, path, diary_exported[diary_key], message)
            else:
                _github_put_file(cfg, path, exported[key], message)
        return True
    except Exception as exc:
        if logger:
            try:
                logger.exception("GitHub backup failed: %s", exc)
            except Exception:
                pass
        return False
