from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import csv
import io
import re

from kakao_parser import KakaoMessage


EXPORT_HEADER_PREFIX = "# CHAT_EXPORT v1"
_COMMENT_MARKER = "\uB313\uAE00"


def build_export_header(kind: str, fmt: str) -> str:
    lines = [
        EXPORT_HEADER_PREFIX,
        f"# type={kind}",
        f"# format={fmt}",
    ]
    return "\n".join(lines) + "\n"


def strip_export_header(text: str) -> tuple[dict[str, str], str]:
    lines = text.splitlines()
    if not lines or not lines[0].startswith(EXPORT_HEADER_PREFIX):
        return {}, text

    meta: dict[str, str] = {"version": "1"}
    idx = 0
    for line in lines:
        if not line.startswith("#"):
            break
        idx += 1
        payload = line.lstrip("#").strip()
        if payload.startswith("CHAT_EXPORT"):
            parts = payload.split()
            if len(parts) >= 2 and parts[1].startswith("v"):
                meta["version"] = parts[1][1:]
            continue
        if "=" in payload:
            key, value = payload.split("=", 1)
            meta[key.strip()] = value.strip()
    body = "\n".join(lines[idx:]).lstrip("\n")
    return meta, body


def serialize_chat_plain(messages: list[dict], *, include_header: bool = False) -> str:
    lines: list[str] = []
    for msg in messages:
        dt = datetime.fromisoformat(str(msg["dt"]))
        stamp = dt.strftime("%Y-%m-%d %H:%M")
        text = str(msg.get("text") or "")
        lines.append(f"{stamp} | {msg['sender']} | {text}")
    body = "\n".join(lines).rstrip() + ("\n" if lines else "")
    if not include_header:
        return body
    return build_export_header("chat", "txt") + body


def serialize_chat_kakao(messages: list[dict], *, include_header: bool = False) -> str:
    from backup import _export_kakao  # reuse existing kakao formatting

    body = _export_kakao(messages)
    if not include_header:
        return body
    return build_export_header("chat", "kakao") + body


def serialize_chat_csv(messages: list[dict], *, include_header: bool = False) -> str:
    buf = io.StringIO()
    if include_header:
        buf.write(build_export_header("chat", "csv"))
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["dt", "sender", "text"])
    for msg in messages:
        writer.writerow([msg.get("dt"), msg.get("sender"), msg.get("text")])
    return buf.getvalue()


_CHAT_PLAIN_RE = re.compile(
    r"^(?P<dt>\d{4}-\d{2}-\d{2} \d{2}:\d{2}(?::\d{2})?) \| (?P<sender>.*?) \| (?P<body>.*)$"
)


def parse_chat_plain(text: str) -> list[KakaoMessage]:
    _meta, body = strip_export_header(text)
    messages: list[KakaoMessage] = []
    current: dict[str, object] | None = None
    for line in body.splitlines():
        match = _CHAT_PLAIN_RE.match(line)
        if match:
            if current:
                messages.append(
                    KakaoMessage(
                        dt=current["dt"],
                        sender=current["sender"],
                        text=current["text"],
                    )
                )
            dt_raw = match.group("dt")
            try:
                dt = datetime.fromisoformat(dt_raw)
            except ValueError:
                continue
            current = {
                "dt": dt,
                "sender": match.group("sender").strip(),
                "text": match.group("body"),
            }
            continue
        if current is not None:
            current["text"] = f"{current['text']}\n{line}"
    if current:
        messages.append(
            KakaoMessage(
                dt=current["dt"],
                sender=current["sender"],
                text=current["text"],
            )
        )
    return messages


def parse_chat_csv(text: str) -> list[KakaoMessage]:
    _meta, body = strip_export_header(text)
    buf = io.StringIO(body)
    peek = buf.readline()
    if not peek:
        return []
    buf.seek(0)
    reader = csv.DictReader(buf)
    if not reader.fieldnames:
        return []
    fieldnames = {name.strip() for name in reader.fieldnames if name}
    messages: list[KakaoMessage] = []
    if {"dt", "sender", "text"} <= fieldnames:
        for row in reader:
            dt_raw = (row.get("dt") or "").strip()
            sender = (row.get("sender") or "").strip()
            text = row.get("text") or ""
            if not dt_raw or not sender:
                continue
            try:
                dt = datetime.fromisoformat(dt_raw)
            except ValueError:
                continue
            messages.append(KakaoMessage(dt=dt, sender=sender, text=text))
        return messages

    buf.seek(0)
    for row in csv.reader(buf):
        if len(row) < 3:
            continue
        try:
            dt = datetime.fromisoformat(row[0].strip())
        except ValueError:
            continue
        sender = row[1].strip()
        text = row[2]
        if sender:
            messages.append(KakaoMessage(dt=dt, sender=sender, text=text))
    return messages


@dataclass
class DiaryImportComment:
    body: str
    created_at: str | None = None


@dataclass
class DiaryImportEntry:
    entry_date: str
    title: str
    body: str
    created_at: str | None = None
    comments: list[DiaryImportComment] = field(default_factory=list)


_DIARY_HEADER_RE = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})\s+\|\s+(?P<title>.+)$")
_DIARY_MD_HEADER_RE = re.compile(r"^##\s+(?P<date>\d{4}-\d{2}-\d{2})\s+-\s+(?P<title>.+)$")
_COMMENT_RE = re.compile(
    r"^-\s+(?P<ts>\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}(?::\d{2})?)?)(?:\s+(?P<body>.*))?$"
)


def _parse_comment_lines(lines: list[str]) -> list[DiaryImportComment]:
    comments: list[DiaryImportComment] = []
    current: DiaryImportComment | None = None
    for line in lines:
        if line.startswith("  ") and current:
            current.body = f"{current.body}\n{line[2:]}"
            continue
        match = _COMMENT_RE.match(line)
        if match:
            if current:
                comments.append(current)
            created_at = match.group("ts")
            body = match.group("body") or ""
            current = DiaryImportComment(body=body, created_at=created_at)
            continue
        if line.startswith("- "):
            if current:
                comments.append(current)
            current = DiaryImportComment(body=line[2:].strip() or "", created_at=None)
            continue
        if current:
            current.body = f"{current.body}\n{line}"
    if current:
        comments.append(current)
    return comments


def parse_diary_plain(text: str) -> list[DiaryImportEntry]:
    _meta, body = strip_export_header(text)
    entries: list[DiaryImportEntry] = []
    current: DiaryImportEntry | None = None
    body_lines: list[str] = []
    comment_lines: list[str] = []
    in_comments = False

    def flush_entry() -> None:
        nonlocal current, body_lines, comment_lines, in_comments
        if not current:
            return
        current.body = "\n".join(body_lines).strip()
        current.comments = _parse_comment_lines(comment_lines)
        entries.append(current)
        current = None
        body_lines = []
        comment_lines = []
        in_comments = False

    for line in body.splitlines():
        match = _DIARY_HEADER_RE.match(line)
        if match:
            flush_entry()
            current = DiaryImportEntry(
                entry_date=match.group("date"),
                title=match.group("title").strip() or "\uBB34\uC81C",
                body="",
            )
            continue
        if current is None:
            continue
        if line.strip() == _COMMENT_MARKER:
            in_comments = True
            continue
        if in_comments:
            comment_lines.append(line)
        else:
            body_lines.append(line)

    flush_entry()
    return entries


def parse_diary_markdown(text: str) -> list[DiaryImportEntry]:
    _meta, body = strip_export_header(text)
    entries: list[DiaryImportEntry] = []
    current: DiaryImportEntry | None = None
    body_lines: list[str] = []
    comment_lines: list[str] = []
    in_comments = False

    def flush_entry() -> None:
        nonlocal current, body_lines, comment_lines, in_comments
        if not current:
            return
        current.body = "\n".join(body_lines).strip()
        current.comments = _parse_comment_lines(comment_lines)
        entries.append(current)
        current = None
        body_lines = []
        comment_lines = []
        in_comments = False

    for line in body.splitlines():
        match = _DIARY_MD_HEADER_RE.match(line)
        if match:
            flush_entry()
            current = DiaryImportEntry(
                entry_date=match.group("date"),
                title=match.group("title").strip() or "\uBB34\uC81C",
                body="",
            )
            continue
        if current is None:
            continue
        if line.strip() == "### " + _COMMENT_MARKER:
            in_comments = True
            continue
        if in_comments:
            comment_lines.append(line)
        else:
            body_lines.append(line)

    flush_entry()
    return entries


def parse_diary_csv(text: str) -> list[DiaryImportEntry]:
    _meta, body = strip_export_header(text)
    buf = io.StringIO(body)
    reader = csv.DictReader(buf)
    if not reader.fieldnames:
        return []
    fieldnames = {name.strip() for name in reader.fieldnames if name}
    if not {"entry_date", "title", "body"} <= fieldnames:
        return []
    entries: list[DiaryImportEntry] = []
    for row in reader:
        entry_date = (row.get("entry_date") or "").strip()
        title = (row.get("title") or "").strip() or "\uBB34\uC81C"
        body_text = row.get("body") or ""
        created_at = (row.get("created_at") or "").strip() or None
        comments_raw = row.get("comments") or ""
        comments = _parse_comment_lines(comments_raw.splitlines())
        if not entry_date:
            continue
        entries.append(
            DiaryImportEntry(
                entry_date=entry_date,
                title=title,
                body=body_text,
                created_at=created_at,
                comments=comments,
            )
        )
    return entries


def serialize_memories_csv(photos: list[dict], *, include_header: bool = False) -> str:
    buf = io.StringIO()
    if include_header:
        buf.write(build_export_header("memories", "csv"))
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(
        [
            "drive_file_id",
            "file_name",
            "mime_type",
            "caption",
            "album",
            "tags",
            "taken_date",
            "created_at",
            "updated_at",
        ]
    )
    for photo in photos:
        writer.writerow(
            [
                photo.get("drive_file_id"),
                photo.get("file_name"),
                photo.get("mime_type"),
                photo.get("caption"),
                photo.get("album"),
                photo.get("tags"),
                photo.get("taken_date"),
                photo.get("created_at"),
                photo.get("updated_at"),
            ]
        )
    return buf.getvalue()


def serialize_memories_txt(photos: list[dict], *, include_header: bool = False) -> str:
    lines: list[str] = []
    for photo in photos:
        lines.append(
            " | ".join(
                [
                    str(photo.get("drive_file_id") or ""),
                    str(photo.get("file_name") or ""),
                    str(photo.get("mime_type") or ""),
                    str(photo.get("caption") or ""),
                    str(photo.get("album") or ""),
                    str(photo.get("tags") or ""),
                    str(photo.get("taken_date") or ""),
                ]
            )
        )
    body = "\n".join(lines).rstrip() + ("\n" if lines else "")
    if not include_header:
        return body
    return build_export_header("memories", "txt") + body


def parse_memories_csv(text: str) -> list[dict]:
    _meta, body = strip_export_header(text)
    buf = io.StringIO(body)
    reader = csv.DictReader(buf)
    if not reader.fieldnames:
        return []
    fieldnames = {name.strip() for name in reader.fieldnames if name}
    if "drive_file_id" not in fieldnames:
        return []
    rows: list[dict] = []
    for row in reader:
        drive_file_id = (row.get("drive_file_id") or "").strip()
        if not drive_file_id:
            continue
        rows.append(
            {
                "drive_file_id": drive_file_id,
                "file_name": row.get("file_name") or "",
                "mime_type": row.get("mime_type") or "",
                "caption": row.get("caption") or "",
                "album": row.get("album") or "",
                "tags": row.get("tags") or "",
                "taken_date": row.get("taken_date") or "",
                "created_at": (row.get("created_at") or "").strip() or None,
                "updated_at": (row.get("updated_at") or "").strip() or None,
            }
        )
    return rows


def parse_memories_txt(text: str) -> list[dict]:
    _meta, body = strip_export_header(text)
    rows: list[dict] = []
    for line in body.splitlines():
        if not line.strip():
            continue
        parts = line.split(" | ", 6)
        if len(parts) < 7:
            continue
        rows.append(
            {
                "drive_file_id": parts[0].strip(),
                "file_name": parts[1].strip(),
                "mime_type": parts[2].strip(),
                "caption": parts[3].strip(),
                "album": parts[4].strip(),
                "tags": parts[5].strip(),
                "taken_date": parts[6].strip(),
            }
        )
    return rows
