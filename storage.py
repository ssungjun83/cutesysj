from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import csv
import hashlib
import io
import sqlite3
from pathlib import Path
from zoneinfo import ZoneInfo

from kakao_parser import KakaoMessage, normalize_text_for_dedup


SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  dt TEXT NOT NULL,
  dt_minute TEXT NOT NULL,
  sender TEXT NOT NULL,
  text TEXT NOT NULL,
  norm_text TEXT NOT NULL,
  dedup_key TEXT NOT NULL UNIQUE,
  source TEXT,
  imported_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE INDEX IF NOT EXISTS idx_messages_dt ON messages(dt);

CREATE TABLE IF NOT EXISTS diary_entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_date TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE INDEX IF NOT EXISTS idx_diary_entries_date ON diary_entries(entry_date);

CREATE TABLE IF NOT EXISTS diary_comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_id INTEGER NOT NULL,
  body TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE INDEX IF NOT EXISTS idx_diary_comments_entry ON diary_comments(entry_id);

CREATE TABLE IF NOT EXISTS diary_photos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_id INTEGER NOT NULL,
  drive_file_id TEXT NOT NULL,
  file_name TEXT NOT NULL,
  mime_type TEXT,
  created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE INDEX IF NOT EXISTS idx_diary_photos_entry ON diary_photos(entry_id);

CREATE TABLE IF NOT EXISTS todo_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  body TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'active',
  tags TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
  completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_todo_items_completed ON todo_items(completed_at);

CREATE TABLE IF NOT EXISTS todo_daily_checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id INTEGER NOT NULL,
  check_date TEXT NOT NULL,
  completed_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
  UNIQUE (item_id, check_date)
);

CREATE INDEX IF NOT EXISTS idx_todo_daily_checks_item ON todo_daily_checks(item_id);
CREATE INDEX IF NOT EXISTS idx_todo_daily_checks_date ON todo_daily_checks(check_date);

CREATE TABLE IF NOT EXISTS memories_photos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  drive_file_id TEXT NOT NULL UNIQUE,
  file_name TEXT NOT NULL,
  mime_type TEXT,
  caption TEXT,
  album TEXT,
  tags TEXT,
  taken_date TEXT,
  created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
  updated_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE INDEX IF NOT EXISTS idx_memories_photos_date ON memories_photos(taken_date);
CREATE INDEX IF NOT EXISTS idx_memories_photos_album ON memories_photos(album);

CREATE TABLE IF NOT EXISTS app_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

SEOUL_TZ = ZoneInfo("Asia/Seoul")
DIARY_TZ_META_KEY = "diary_tz_seoul_v1"


def _dt_minute(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M")


def _dedup_key(dt_minute: str, norm_text: str) -> str:
    raw = f"{dt_minute}\n{norm_text}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


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


def _format_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _now_seoul_timestamp() -> str:
    return _format_timestamp(datetime.now(SEOUL_TZ))


def _utc_to_seoul_timestamp(value: str) -> str:
    dt = _parse_timestamp(value)
    if not dt:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return _format_timestamp(dt.astimezone(SEOUL_TZ))


def migrate_diary_timezone_seoul(db_path: Path) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        existing = conn.execute(
            "SELECT value FROM app_meta WHERE key = ? LIMIT 1",
            (DIARY_TZ_META_KEY,),
        ).fetchone()
        if existing:
            return False

        conn.execute("BEGIN")
        entry_rows = conn.execute("SELECT id, created_at FROM diary_entries").fetchall()
        for row in entry_rows:
            created_at = str(row["created_at"] or "").strip()
            if not created_at:
                continue
            converted = _utc_to_seoul_timestamp(created_at)
            if converted != created_at:
                conn.execute(
                    "UPDATE diary_entries SET created_at = ? WHERE id = ?",
                    (converted, int(row["id"])),
                )

        comment_rows = conn.execute("SELECT id, created_at FROM diary_comments").fetchall()
        for row in comment_rows:
            created_at = str(row["created_at"] or "").strip()
            if not created_at:
                continue
            converted = _utc_to_seoul_timestamp(created_at)
            if converted != created_at:
                conn.execute(
                    "UPDATE diary_comments SET created_at = ? WHERE id = ?",
                    (converted, int(row["id"])),
                )

        conn.execute(
            "INSERT OR REPLACE INTO app_meta (key, value) VALUES (?, ?)",
            (DIARY_TZ_META_KEY, _now_seoul_timestamp()),
        )
        conn.commit()
    return True


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA)
        # Backward-compatible migration for existing DBs created before todo kind was added.
        try:
            conn.execute("ALTER TABLE todo_items ADD COLUMN kind TEXT NOT NULL DEFAULT 'active'")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE todo_items ADD COLUMN tags TEXT NOT NULL DEFAULT ''")
        except sqlite3.OperationalError:
            pass
        conn.commit()


def import_messages(db_path: Path, messages: list[KakaoMessage], source: str | None = None) -> dict:
    init_db(db_path)
    inserted = 0
    skipped = 0
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for msg in messages:
            dt_iso = msg.dt.isoformat(timespec="seconds")
            dt_minute = _dt_minute(msg.dt)
            norm_text = normalize_text_for_dedup(msg.text)
            key = _dedup_key(dt_minute, norm_text)
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO messages
                (dt, dt_minute, sender, text, norm_text, dedup_key, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (dt_iso, dt_minute, msg.sender, msg.text, norm_text, key, source),
            )
            if cur.rowcount == 1:
                inserted += 1
            else:
                skipped += 1
        conn.commit()

    return {"inserted": inserted, "skipped": skipped, "total": len(messages)}


def _canonicalize_sender(sender: str, me_sender: str, other_sender: str) -> str:
    sender = (sender or "").strip()
    if sender == me_sender:
        return me_sender
    return other_sender


def import_messages_canonicalized(
    db_path: Path,
    messages: list[KakaoMessage],
    source: str | None = None,
    *,
    me_sender: str = "이성준",
    other_sender: str = "귀여운소연이",
) -> dict:
    canonical = [
        KakaoMessage(dt=m.dt, sender=_canonicalize_sender(m.sender, me_sender, other_sender), text=m.text)
        for m in messages
    ]
    return import_messages(db_path, canonical, source=source)


def normalize_db_senders_and_dedup(
    db_path: Path,
    *,
    me_sender: str = "이성준",
    other_sender: str = "귀여운소연이",
) -> dict:
    """
    Canonicalize sender names and drop any duplicates by rebuilding the messages table.
    Keeps the earliest row per dedup_key (based on ORDER BY dt ASC, id ASC).
    """
    init_db(db_path)
    kept = 0
    dropped = 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")

        # Ensure we have a clean destination with the current schema.
        conn.execute("DROP TABLE IF EXISTS messages_new")
        conn.executescript(
            SCHEMA.replace("CREATE TABLE IF NOT EXISTS messages", "CREATE TABLE IF NOT EXISTS messages_new")
            .replace("idx_messages_dt ON messages", "idx_messages_dt_new ON messages_new")
        )

        rows = conn.execute(
            """
            SELECT dt, sender, text, source
            FROM messages
            ORDER BY dt ASC, id ASC
            """
        ).fetchall()

        for r in rows:
            dt_iso = str(r["dt"])
            dt = datetime.fromisoformat(dt_iso)
            dt_minute = _dt_minute(dt)
            sender = _canonicalize_sender(str(r["sender"]), me_sender, other_sender)
            text = str(r["text"])
            norm_text = normalize_text_for_dedup(text)
            key = _dedup_key(dt_minute, norm_text)
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO messages_new
                (dt, dt_minute, sender, text, norm_text, dedup_key, source)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (dt_iso, dt_minute, sender, text, norm_text, key, r["source"]),
            )
            if cur.rowcount == 1:
                kept += 1
            else:
                dropped += 1

        conn.execute("DROP TABLE messages")
        conn.execute("ALTER TABLE messages_new RENAME TO messages")
        conn.execute("DROP INDEX IF EXISTS idx_messages_dt_new")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_dt ON messages(dt)")
        conn.commit()

    return {"kept": kept, "dropped": dropped, "total": kept + dropped}


def fetch_messages(
    db_path: Path,
    limit: int | None = None,
    before_dt: str | None = None,
    order: str = "asc",
) -> list[dict]:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        params: list[object] = []
        where = ""
        if before_dt:
            where = "WHERE dt < ?"
            params.append(before_dt)

        order_sql = "ASC" if order.lower() == "asc" else "DESC"
        limit_sql = ""
        if limit is not None:
            limit_sql = "LIMIT ?"
            params.append(int(limit))
        rows = conn.execute(
            f"""
            SELECT id, dt, sender, text, source, imported_at
            FROM messages
            {where}
            ORDER BY dt {order_sql}, id {order_sql}
            {limit_sql}
            """,
            params,
        ).fetchall()
    items = [dict(r) for r in rows]
    return items


def fetch_senders(db_path: Path, limit: int = 50) -> list[dict]:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT sender, COUNT(*) AS count
            FROM messages
            GROUP BY sender
            ORDER BY count DESC, sender ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [dict(r) for r in rows]


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def search_messages(db_path: Path, query: str, limit: int = 2000) -> list[dict]:
    init_db(db_path)
    q = (query or "").strip()
    if not q:
        return []
    like = f"%{_escape_like(q)}%"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, dt, sender, text, source, imported_at
            FROM messages
            WHERE text LIKE ? ESCAPE '\\' OR norm_text LIKE ? ESCAPE '\\'
            ORDER BY dt ASC, id ASC
            LIMIT ?
            """,
            (like, like, int(limit)),
        ).fetchall()
    return [dict(r) for r in rows]


def get_latest_dt(db_path: Path) -> str | None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT dt FROM messages ORDER BY dt DESC, id DESC LIMIT 1").fetchone()
    return row[0] if row else None


def get_oldest_dt(db_path: Path) -> str | None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT dt FROM messages ORDER BY dt ASC, id ASC LIMIT 1").fetchone()
    return row[0] if row else None


def add_diary_entry(db_path: Path, entry_date: str, title: str, body: str) -> int:
    init_db(db_path)
    created_at = _now_seoul_timestamp()
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO diary_entries (entry_date, title, body, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (entry_date, title, body, created_at),
        )
        conn.commit()
        return int(cur.lastrowid)


def upsert_diary_entry(
    db_path: Path,
    entry_date: str,
    title: str,
    body: str,
    created_at: str | None = None,
) -> tuple[int, bool] | None:
    init_db(db_path)
    created_at_value = created_at or _now_seoul_timestamp()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT id
            FROM diary_entries
            WHERE entry_date = ? AND title = ? AND body = ?
            LIMIT 1
            """,
            (entry_date, title, body),
        ).fetchone()
        if row:
            return int(row["id"]), False
        if created_at:
            cur = conn.execute(
                """
                INSERT INTO diary_entries (entry_date, title, body, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (entry_date, title, body, created_at),
            )
        else:
            cur = conn.execute(
                """
                INSERT INTO diary_entries (entry_date, title, body, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (entry_date, title, body, created_at_value),
            )
        conn.commit()
        return int(cur.lastrowid), True


def fetch_diary_entries(
    db_path: Path,
    *,
    limit: int | None = 200,
    q: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    order: str = "desc",
) -> list[dict]:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        params: list[object] = []
        conditions: list[str] = []
        if q:
            like = f"%{_escape_like(q)}%"
            conditions.append("(title LIKE ? ESCAPE '\\' OR body LIKE ? ESCAPE '\\')")
            params.extend([like, like])
        if start_date:
            conditions.append("entry_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("entry_date <= ?")
            params.append(end_date)
        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order_sql = "ASC" if order.lower() == "asc" else "DESC"
        limit_sql = ""
        if limit is not None:
            limit_sql = "LIMIT ?"
            params.append(int(limit))
        rows = conn.execute(
            f"""
            SELECT id, entry_date, title, body, created_at
            FROM diary_entries
            {where_sql}
            ORDER BY entry_date {order_sql}, id {order_sql}
            {limit_sql}
            """,
            params,
        ).fetchall()
    return [dict(r) for r in rows]


def get_diary_entry(db_path: Path, entry_id: int) -> dict | None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT id, entry_date, title, body, created_at
            FROM diary_entries
            WHERE id = ?
            """,
            (int(entry_id),),
        ).fetchone()
    return dict(row) if row else None


def update_diary_entry(db_path: Path, entry_id: int, entry_date: str, title: str, body: str) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            UPDATE diary_entries
            SET entry_date = ?, title = ?, body = ?
            WHERE id = ?
            """,
            (entry_date, title, body, int(entry_id)),
        )
        conn.commit()
        return cur.rowcount == 1


def delete_diary_entry(db_path: Path, entry_id: int) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM diary_comments WHERE entry_id = ?", (int(entry_id),))
        conn.execute("DELETE FROM diary_photos WHERE entry_id = ?", (int(entry_id),))
        cur = conn.execute("DELETE FROM diary_entries WHERE id = ?", (int(entry_id),))
        conn.commit()
        return cur.rowcount == 1


def add_diary_comment(db_path: Path, entry_id: int, body: str) -> int:
    init_db(db_path)
    created_at = _now_seoul_timestamp()
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO diary_comments (entry_id, body, created_at)
            VALUES (?, ?, ?)
            """,
            (int(entry_id), body, created_at),
        )
        conn.commit()
        return int(cur.lastrowid)


def upsert_diary_comment(
    db_path: Path,
    entry_id: int,
    body: str,
    created_at: str | None = None,
) -> bool:
    init_db(db_path)
    created_at_value = created_at or _now_seoul_timestamp()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        if created_at:
            row = conn.execute(
                """
                SELECT id
                FROM diary_comments
                WHERE entry_id = ? AND body = ? AND created_at = ?
                LIMIT 1
                """,
                (int(entry_id), body, created_at),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id
                FROM diary_comments
                WHERE entry_id = ? AND body = ?
                LIMIT 1
                """,
                (int(entry_id), body),
            ).fetchone()
        if row:
            return False
        if created_at:
            conn.execute(
                """
                INSERT INTO diary_comments (entry_id, body, created_at)
                VALUES (?, ?, ?)
                """,
                (int(entry_id), body, created_at),
            )
        else:
            conn.execute(
                """
                INSERT INTO diary_comments (entry_id, body, created_at)
                VALUES (?, ?, ?)
                """,
                (int(entry_id), body, created_at_value),
            )
        conn.commit()
        return True


def fetch_diary_comments(db_path: Path, entry_ids: list[int]) -> dict[int, list[dict]]:
    init_db(db_path)
    if not entry_ids:
        return {}
    ids = [int(entry_id) for entry_id in entry_ids]
    placeholders = ", ".join("?" for _ in ids)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT id, entry_id, body, created_at
            FROM diary_comments
            WHERE entry_id IN ({placeholders})
            ORDER BY entry_id ASC, id ASC
            """,
            ids,
        ).fetchall()
    grouped: dict[int, list[dict]] = {}
    for row in rows:
        entry_id = int(row["entry_id"])
        grouped.setdefault(entry_id, []).append(dict(row))
    return grouped


def delete_diary_comment(db_path: Path, comment_id: int) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("DELETE FROM diary_comments WHERE id = ?", (int(comment_id),))
        conn.commit()
        return cur.rowcount == 1


def _normalize_todo_tags(tags: str | None) -> str:
    raw = (tags or "").replace("#", " ").strip()
    if not raw:
        return ""
    parts = [part.strip() for part in raw.split(",")]
    cleaned = [part for part in parts if part]
    return ", ".join(cleaned[:3])


def add_todo_item(db_path: Path, body: str, *, kind: str = "active", tags: str | None = None) -> int:
    init_db(db_path)
    created_at = _now_seoul_timestamp()
    kind_value = "daily" if kind == "daily" else "active"
    tags_value = _normalize_todo_tags(tags if kind_value == "active" else "")
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO todo_items (body, kind, tags, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (body, kind_value, tags_value, created_at),
        )
        conn.commit()
        return int(cur.lastrowid)


def get_todo_item(db_path: Path, item_id: int) -> dict | None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT id, body, kind, tags, created_at, completed_at
            FROM todo_items
            WHERE id = ?
            LIMIT 1
            """,
            (int(item_id),),
        ).fetchone()
    return dict(row) if row else None


def update_todo_item(db_path: Path, item_id: int, body: str, *, tags: str | None = None) -> bool:
    init_db(db_path)
    tags_value = _normalize_todo_tags(tags)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            UPDATE todo_items
            SET body = ?,
                tags = CASE WHEN kind = 'active' THEN ? ELSE '' END
            WHERE id = ?
            """,
            (body, tags_value, int(item_id)),
        )
        conn.commit()
        return cur.rowcount == 1


def delete_todo_item(db_path: Path, item_id: int) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM todo_daily_checks WHERE item_id = ?", (int(item_id),))
        cur = conn.execute("DELETE FROM todo_items WHERE id = ?", (int(item_id),))
        conn.commit()
        return cur.rowcount == 1


def complete_todo_item(db_path: Path, item_id: int, completed_at: str | None = None) -> bool:
    init_db(db_path)
    completed_value = completed_at or _now_seoul_timestamp()
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            UPDATE todo_items
            SET completed_at = ?
            WHERE id = ? AND kind = 'active' AND completed_at IS NULL
            """,
            (completed_value, int(item_id)),
        )
        conn.commit()
        return cur.rowcount == 1


def check_todo_daily_item(
    db_path: Path,
    item_id: int,
    *,
    check_date: str | None = None,
    completed_at: str | None = None,
) -> bool:
    init_db(db_path)
    check_date_value = check_date or datetime.now(SEOUL_TZ).date().isoformat()
    completed_value = completed_at or _now_seoul_timestamp()
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id
            FROM todo_items
            WHERE id = ? AND kind = 'daily'
            LIMIT 1
            """,
            (int(item_id),),
        ).fetchone()
        if not row:
            return False
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO todo_daily_checks (item_id, check_date, completed_at)
            VALUES (?, ?, ?)
            """,
            (int(item_id), check_date_value, completed_value),
        )
        conn.commit()
        return cur.rowcount == 1


def fetch_todo_items(db_path: Path) -> tuple[list[dict], list[dict], list[dict]]:
    init_db(db_path)
    today = datetime.now(SEOUL_TZ).date().isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        daily_rows = conn.execute(
            """
            SELECT t.id,
                   t.body,
                   t.kind,
                   t.tags,
                   t.created_at,
                   t.completed_at,
                   dc.completed_at AS today_completed_at
            FROM todo_items t
            LEFT JOIN todo_daily_checks dc
              ON dc.item_id = t.id
             AND dc.check_date = ?
            WHERE t.kind = 'daily'
            ORDER BY t.created_at ASC, t.id ASC
            """,
            (today,),
        ).fetchall()
        pending_rows = conn.execute(
            """
            SELECT id, body, kind, tags, created_at, completed_at
            FROM todo_items
            WHERE kind = 'active' AND completed_at IS NULL
            ORDER BY created_at ASC, id ASC
            """
        ).fetchall()
        done_rows = conn.execute(
            """
            SELECT id, body, kind, tags, created_at, completed_at
            FROM todo_items
            WHERE kind = 'active' AND completed_at IS NOT NULL
            ORDER BY completed_at DESC, id DESC
            """
        ).fetchall()
    return ([dict(r) for r in daily_rows], [dict(r) for r in pending_rows], [dict(r) for r in done_rows])


def add_diary_photo(
    db_path: Path,
    *,
    entry_id: int,
    drive_file_id: str,
    file_name: str,
    mime_type: str | None = None,
) -> int:
    init_db(db_path)
    created_at = _now_seoul_timestamp()
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT INTO diary_photos (entry_id, drive_file_id, file_name, mime_type, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (int(entry_id), drive_file_id, file_name, mime_type, created_at),
        )
        conn.commit()
        return int(cur.lastrowid)


def fetch_diary_photos(db_path: Path, entry_ids: list[int]) -> dict[int, list[dict]]:
    init_db(db_path)
    if not entry_ids:
        return {}
    ids = [int(entry_id) for entry_id in entry_ids]
    placeholders = ", ".join("?" for _ in ids)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT id, entry_id, drive_file_id, file_name, mime_type, created_at
            FROM diary_photos
            WHERE entry_id IN ({placeholders})
            ORDER BY entry_id ASC, id ASC
            """,
            ids,
        ).fetchall()
    grouped: dict[int, list[dict]] = {}
    for row in rows:
        entry_id = int(row["entry_id"])
        grouped.setdefault(entry_id, []).append(dict(row))
    return grouped


def get_diary_photo(db_path: Path, photo_id: int) -> dict | None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT id, entry_id, drive_file_id, file_name, mime_type, created_at
            FROM diary_photos
            WHERE id = ?
            """,
            (int(photo_id),),
        ).fetchone()
    return dict(row) if row else None


def delete_diary_photo(db_path: Path, photo_id: int) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("DELETE FROM diary_photos WHERE id = ?", (int(photo_id),))
        conn.commit()
        return cur.rowcount == 1


def _format_comment_lines(comments: list[dict]) -> list[str]:
    lines: list[str] = []
    for comment in comments:
        created_at = str(comment.get("created_at") or "").strip()
        body = str(comment.get("body") or "").replace("\r\n", "\n").replace("\r", "\n").strip()
        body_lines = [line for line in body.split("\n")] if body else [""]
        prefix = f"- {created_at}".strip()
        first_line = body_lines[0].strip()
        if first_line:
            line = f"{prefix} {first_line}".strip()
        else:
            line = prefix or "-"
        lines.append(line)
        for extra in body_lines[1:]:
            lines.append(f"  {extra}")
    return lines


def serialize_diary_plain(entries: list[dict]) -> str:
    lines: list[str] = []
    for entry in entries:
        entry_date = str(entry.get("entry_date") or "")
        title = str(entry.get("title") or "무제")
        body = str(entry.get("body") or "")
        lines.append(f"{entry_date} | {title}")
        if body:
            lines.append(body)
        comments = entry.get("comments") or []
        if comments:
            lines.append("댓글")
            lines.extend(_format_comment_lines(comments))
        lines.append("")
    if not lines:
        return ""
    return "\n".join(lines).rstrip() + "\n"


def serialize_diary_markdown(entries: list[dict]) -> str:
    lines: list[str] = []
    for entry in entries:
        entry_date = str(entry.get("entry_date") or "")
        title = str(entry.get("title") or "무제")
        body = str(entry.get("body") or "")
        lines.append(f"## {entry_date} - {title}")
        if body:
            lines.append(body)
        comments = entry.get("comments") or []
        if comments:
            lines.append("### 댓글")
            lines.extend(_format_comment_lines(comments))
        lines.append("")
    if not lines:
        return ""
    return "\n".join(lines).rstrip() + "\n"


def serialize_diary_csv(entries: list[dict]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["entry_date", "title", "body", "created_at", "comments"])
    for entry in entries:
        comments = entry.get("comments") or []
        comments_text = "\n".join(_format_comment_lines(comments)) if comments else ""
        writer.writerow(
            [
                entry.get("entry_date"),
                entry.get("title"),
                entry.get("body"),
                entry.get("created_at"),
                comments_text,
            ]
        )
    return buf.getvalue()


def _normalize_tags(tags: str) -> str:
    raw = (tags or "").replace("#", " ").strip()
    if not raw:
        return ""
    parts = [part.strip() for part in raw.split(",")]
    cleaned = [part for part in parts if part]
    return ", ".join(cleaned)


def add_memory_photo(
    db_path: Path,
    *,
    drive_file_id: str,
    file_name: str,
    mime_type: str | None = None,
    caption: str | None = None,
    album: str | None = None,
    tags: str | None = None,
    taken_date: str | None = None,
) -> bool:
    init_db(db_path)
    tags_clean = _normalize_tags(tags or "")
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO memories_photos
            (drive_file_id, file_name, mime_type, caption, album, tags, taken_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                drive_file_id,
                file_name,
                mime_type,
                caption or "",
                album or "",
                tags_clean,
                taken_date,
            ),
        )
        conn.commit()
        return cur.rowcount == 1


def upsert_memory_photo_full(
    db_path: Path,
    *,
    drive_file_id: str,
    file_name: str,
    mime_type: str | None = None,
    caption: str | None = None,
    album: str | None = None,
    tags: str | None = None,
    taken_date: str | None = None,
    created_at: str | None = None,
    updated_at: str | None = None,
) -> bool:
    init_db(db_path)
    tags_clean = _normalize_tags(tags or "")
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO memories_photos
            (drive_file_id, file_name, mime_type, caption, album, tags, taken_date, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
            """,
            (
                drive_file_id,
                file_name,
                mime_type,
                caption or "",
                album or "",
                tags_clean,
                taken_date,
                created_at,
                updated_at,
            ),
        )
        inserted = cur.rowcount == 1
        conn.execute(
            """
            UPDATE memories_photos
            SET file_name = ?,
                mime_type = ?,
                caption = ?,
                album = ?,
                tags = ?,
                taken_date = ?,
                updated_at = COALESCE(?, CURRENT_TIMESTAMP)
            WHERE drive_file_id = ?
            """,
            (
                file_name,
                mime_type,
                caption or "",
                album or "",
                tags_clean,
                taken_date,
                updated_at,
                drive_file_id,
            ),
        )
        conn.commit()
    return inserted


def upsert_memory_photo(
    db_path: Path,
    *,
    drive_file_id: str,
    file_name: str,
    mime_type: str | None = None,
    taken_date: str | None = None,
) -> bool:
    inserted = add_memory_photo(
        db_path,
        drive_file_id=drive_file_id,
        file_name=file_name,
        mime_type=mime_type,
        taken_date=taken_date,
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE memories_photos
            SET file_name = ?,
                mime_type = ?,
                taken_date = CASE
                  WHEN taken_date IS NULL OR taken_date = '' THEN ?
                  ELSE taken_date
                END,
                updated_at = CURRENT_TIMESTAMP
            WHERE drive_file_id = ?
            """,
            (file_name, mime_type, taken_date, drive_file_id),
        )
        conn.commit()
    return inserted


def fetch_memory_photos(
    db_path: Path,
    *,
    q: str | None = None,
    album: str | None = None,
    tag: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = 200,
    order: str = "desc",
) -> list[dict]:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        params: list[object] = []
        conditions: list[str] = []
        if q:
            like = f"%{_escape_like(q)}%"
            conditions.append(
                "(file_name LIKE ? ESCAPE '\\' OR caption LIKE ? ESCAPE '\\' OR tags LIKE ? ESCAPE '\\' OR album LIKE ? ESCAPE '\\')"
            )
            params.extend([like, like, like, like])
        if album:
            conditions.append("album = ?")
            params.append(album)
        if tag:
            like = f"%{_escape_like(tag)}%"
            conditions.append("tags LIKE ? ESCAPE '\\'")
            params.append(like)
        if start_date:
            conditions.append("taken_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("taken_date <= ?")
            params.append(end_date)
        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order_sql = "ASC" if order.lower() == "asc" else "DESC"
        limit_sql = ""
        if limit is not None:
            limit_sql = "LIMIT ?"
            params.append(int(limit))
        rows = conn.execute(
            f"""
            SELECT id, drive_file_id, file_name, mime_type, caption, album, tags, taken_date, created_at, updated_at
            FROM memories_photos
            {where_sql}
            ORDER BY taken_date {order_sql}, id {order_sql}
            {limit_sql}
            """,
            params,
        ).fetchall()
    return [dict(r) for r in rows]


def get_memory_photo(db_path: Path, photo_id: int) -> dict | None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT id, drive_file_id, file_name, mime_type, caption, album, tags, taken_date, created_at, updated_at
            FROM memories_photos
            WHERE id = ?
            """,
            (int(photo_id),),
        ).fetchone()
    return dict(row) if row else None


def get_memory_photo_by_drive_id(db_path: Path, drive_file_id: str) -> dict | None:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT id, drive_file_id, file_name, mime_type, caption, album, tags, taken_date, created_at, updated_at
            FROM memories_photos
            WHERE drive_file_id = ?
            """,
            (drive_file_id,),
        ).fetchone()
    return dict(row) if row else None


def update_memory_photo(
    db_path: Path,
    photo_id: int,
    *,
    caption: str,
    album: str,
    tags: str,
    taken_date: str | None,
) -> bool:
    init_db(db_path)
    tags_clean = _normalize_tags(tags)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            """
            UPDATE memories_photos
            SET caption = ?,
                album = ?,
                tags = ?,
                taken_date = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (caption, album, tags_clean, taken_date, int(photo_id)),
        )
        conn.commit()
        return cur.rowcount == 1


def delete_memory_photo(db_path: Path, photo_id: int) -> bool:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute("DELETE FROM memories_photos WHERE id = ?", (int(photo_id),))
        conn.commit()
        return cur.rowcount == 1


def fetch_memory_albums(db_path: Path) -> list[str]:
    init_db(db_path)
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT album
            FROM memories_photos
            WHERE album IS NOT NULL AND album <> ''
            ORDER BY album ASC
            """
        ).fetchall()
    return [str(row[0]) for row in rows]

