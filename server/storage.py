"""SQLite + filesystem persistence for analyses, comments, and storage accounting."""
import shutil
import sqlite3
import time
import uuid
from threading import RLock

from .config import ANALYSES_DIR, DB_PATH, STORAGE_LIMIT_BYTES

_lock = RLock()


def _conn():
    c = sqlite3.connect(str(DB_PATH))
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with _lock, _conn() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS analyses (
            id TEXT PRIMARY KEY,
            title TEXT,
            source TEXT,
            source_url TEXT,
            duration_secs REAL DEFAULT 0,
            created_at INTEGER NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT,
            size_bytes INTEGER DEFAULT 0,
            sections_count INTEGER DEFAULT 0,
            beats_count INTEGER DEFAULT 0,
            yt_id TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_analyses_created ON analyses(created_at DESC);

        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_id TEXT NOT NULL,
            beat_index INTEGER NOT NULL,
            author TEXT,
            body TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY (analysis_id) REFERENCES analyses(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_comments_lookup ON comments(analysis_id, beat_index);
        """)

        # Anchored-comment columns. SQLite can't ADD COLUMN IF NOT EXISTS, so try
        # each individually and ignore failures from prior runs.
        for ddl in (
            "ALTER TABLE comments ADD COLUMN field TEXT",
            "ALTER TABLE comments ADD COLUMN quote TEXT",
            "ALTER TABLE comments ADD COLUMN start_offset INTEGER",
            "ALTER TABLE comments ADD COLUMN end_offset INTEGER",
            "ALTER TABLE comments ADD COLUMN resolved INTEGER DEFAULT 0",
            # Track who ran each analysis so the History list can show
            # 'Run by <name>'. Older rows will simply have NULL here.
            "ALTER TABLE analyses ADD COLUMN created_by_name TEXT",
            "ALTER TABLE analyses ADD COLUMN created_by_email TEXT",
        ):
            try:
                c.execute(ddl)
            except sqlite3.OperationalError:
                pass


# ── Analyses ──────────────────────────────────────────────────────────────────

def list_analyses():
    with _lock, _conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT * FROM analyses ORDER BY created_at DESC"
        ).fetchall()]


def get_analysis(analysis_id):
    with _lock, _conn() as c:
        row = c.execute("SELECT * FROM analyses WHERE id = ?", (analysis_id,)).fetchone()
        return dict(row) if row else None


def create_analysis(source, source_url, title='Pending analysis...',
                    created_by_name=None, created_by_email=None):
    aid = uuid.uuid4().hex[:12]
    with _lock, _conn() as c:
        c.execute(
            "INSERT INTO analyses "
            "(id, title, source, source_url, created_at, status, "
            " created_by_name, created_by_email) "
            "VALUES (?, ?, ?, ?, ?, 'queued', ?, ?)",
            (aid, title, source, source_url, int(time.time()),
             created_by_name, created_by_email)
        )
    (ANALYSES_DIR / aid).mkdir(exist_ok=True, parents=True)
    return aid


def update_analysis(analysis_id, **fields):
    if not fields:
        return
    with _lock, _conn() as c:
        cols = ", ".join(f"{k} = ?" for k in fields)
        c.execute(f"UPDATE analyses SET {cols} WHERE id = ?",
                  list(fields.values()) + [analysis_id])


def delete_analysis(analysis_id):
    folder = ANALYSES_DIR / analysis_id
    if folder.exists():
        shutil.rmtree(folder, ignore_errors=True)
    with _lock, _conn() as c:
        c.execute("DELETE FROM comments WHERE analysis_id = ?", (analysis_id,))
        c.execute("DELETE FROM analyses WHERE id = ?", (analysis_id,))


def mark_orphan_jobs_failed():
    """Called at startup — any 'running' or 'queued' jobs from a previous process are dead."""
    with _lock, _conn() as c:
        c.execute(
            "UPDATE analyses SET status = 'error', "
            "error_message = 'Server restarted before analysis completed' "
            "WHERE status IN ('queued', 'running')"
        )


# ── Storage accounting ────────────────────────────────────────────────────────

def get_storage_used():
    total = 0
    if ANALYSES_DIR.exists():
        for p in ANALYSES_DIR.rglob('*'):
            if p.is_file():
                try:
                    total += p.stat().st_size
                except OSError:
                    pass
    return total


def storage_check():
    used = get_storage_used()
    limit = STORAGE_LIMIT_BYTES
    return {
        "used_bytes": used,
        "limit_bytes": limit,
        "used_gb": round(used / (1024 ** 3), 2),
        "limit_gb": round(limit / (1024 ** 3), 2),
        "available_bytes": max(0, limit - used),
        "available_gb": round(max(0, limit - used) / (1024 ** 3), 2),
        "percent_used": round((used / limit) * 100, 1) if limit else 0,
    }


def folder_for(analysis_id):
    return ANALYSES_DIR / analysis_id


def folder_size(analysis_id):
    folder = folder_for(analysis_id)
    if not folder.exists():
        return 0
    return sum(p.stat().st_size for p in folder.rglob('*') if p.is_file())


# ── Comments ──────────────────────────────────────────────────────────────────

def add_comment(analysis_id, beat_index, body, author='',
                field=None, quote=None, start_offset=None, end_offset=None):
    with _lock, _conn() as c:
        cur = c.execute(
            "INSERT INTO comments "
            "(analysis_id, beat_index, author, body, created_at, "
            " field, quote, start_offset, end_offset, resolved) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
            (analysis_id, beat_index, author or 'Anonymous',
             body, int(time.time()),
             field, quote, start_offset, end_offset)
        )
        return cur.lastrowid


def list_comments(analysis_id):
    with _lock, _conn() as c:
        return [dict(r) for r in c.execute(
            "SELECT id, beat_index, author, body, created_at, "
            "       field, quote, start_offset, end_offset, resolved "
            "FROM comments WHERE analysis_id = ? "
            "ORDER BY beat_index, created_at",
            (analysis_id,)
        ).fetchall()]


def update_comment(comment_id, body=None, resolved=None):
    sets, vals = [], []
    if body is not None:
        sets.append("body = ?"); vals.append(body)
    if resolved is not None:
        sets.append("resolved = ?"); vals.append(1 if resolved else 0)
    if not sets:
        return
    vals.append(comment_id)
    with _lock, _conn() as c:
        c.execute(f"UPDATE comments SET {', '.join(sets)} WHERE id = ?", vals)


def get_comment(comment_id):
    with _lock, _conn() as c:
        row = c.execute(
            "SELECT id, analysis_id, beat_index, author, body, created_at, "
            "       field, quote, start_offset, end_offset, resolved "
            "FROM comments WHERE id = ?", (comment_id,)
        ).fetchone()
        return dict(row) if row else None


def delete_comment(comment_id):
    with _lock, _conn() as c:
        c.execute("DELETE FROM comments WHERE id = ?", (comment_id,))
