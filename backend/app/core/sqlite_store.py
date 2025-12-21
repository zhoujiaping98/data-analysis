from __future__ import annotations

import os
import sqlite3
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

DB_PATH = os.path.abspath("./data/app.sqlite3")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

_lock = asyncio.Lock()

def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

async def init_sqlite() -> None:
    async with _lock:
        conn = _connect()
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS conversations (
                id TEXT PRIMARY KEY,
                owner_username TEXT NOT NULL,
                title TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                role TEXT NOT NULL, -- 'user' | 'assistant' | 'system'
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS file_uploads (
                id TEXT PRIMARY KEY,
                owner_username TEXT NOT NULL,
                datasource_id TEXT,
                filename TEXT NOT NULL,
                sheet_name TEXT,
                table_name TEXT NOT NULL,
                row_count INTEGER NOT NULL,
                columns_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS message_artifacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                conversation_id TEXT NOT NULL,
                user_message_id INTEGER NOT NULL,
                sql_text TEXT NOT NULL,
                columns_json TEXT NOT NULL,
                rows_json TEXT NOT NULL,
                chart_json TEXT,
                analysis_text TEXT,
                explain_text TEXT,
                suggest_text TEXT,
                safety_text TEXT,
                fix_text TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS data_sources (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                config_json TEXT NOT NULL,
                is_default INTEGER NOT NULL,
                training_ok INTEGER,
                training_error TEXT,
                last_trained_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sql_audits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_username TEXT NOT NULL,
                conversation_id TEXT,
                message_id INTEGER,
                datasource_id TEXT,
                sql_text TEXT NOT NULL,
                row_count INTEGER,
                elapsed_ms INTEGER,
                success INTEGER NOT NULL,
                error_message TEXT,
                slow INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        cols = {r["name"] for r in cur.execute("PRAGMA table_info(file_uploads)").fetchall()}
        if "sheet_name" not in cols:
            cur.execute("ALTER TABLE file_uploads ADD COLUMN sheet_name TEXT")
        if "datasource_id" not in cols:
            cur.execute("ALTER TABLE file_uploads ADD COLUMN datasource_id TEXT")
            cur.execute("UPDATE file_uploads SET datasource_id='default' WHERE datasource_id IS NULL")
        artifact_cols = {r["name"] for r in cur.execute("PRAGMA table_info(message_artifacts)").fetchall()}
        if "analysis_text" not in artifact_cols:
            cur.execute("ALTER TABLE message_artifacts ADD COLUMN analysis_text TEXT")
        if "explain_text" not in artifact_cols:
            cur.execute("ALTER TABLE message_artifacts ADD COLUMN explain_text TEXT")
        if "suggest_text" not in artifact_cols:
            cur.execute("ALTER TABLE message_artifacts ADD COLUMN suggest_text TEXT")
        if "safety_text" not in artifact_cols:
            cur.execute("ALTER TABLE message_artifacts ADD COLUMN safety_text TEXT")
        if "fix_text" not in artifact_cols:
            cur.execute("ALTER TABLE message_artifacts ADD COLUMN fix_text TEXT")
        audit_cols = {r["name"] for r in cur.execute("PRAGMA table_info(sql_audits)").fetchall()}
        if audit_cols:
            if "elapsed_ms" not in audit_cols:
                cur.execute("ALTER TABLE sql_audits ADD COLUMN elapsed_ms INTEGER")
            if "slow" not in audit_cols:
                cur.execute("ALTER TABLE sql_audits ADD COLUMN slow INTEGER NOT NULL DEFAULT 0")
        ds_cols = {r["name"] for r in cur.execute("PRAGMA table_info(data_sources)").fetchall()}
        if "training_ok" not in ds_cols:
            cur.execute("ALTER TABLE data_sources ADD COLUMN training_ok INTEGER")
        if "training_error" not in ds_cols:
            cur.execute("ALTER TABLE data_sources ADD COLUMN training_error TEXT")
        if "last_trained_at" not in ds_cols:
            cur.execute("ALTER TABLE data_sources ADD COLUMN last_trained_at TEXT")
        conn.commit()
        conn.close()

async def create_user(username: str, password_hash: str) -> None:
    async with _lock:
        conn = _connect()
        conn.execute(
            "INSERT INTO users(username, password_hash, created_at) VALUES(?,?,?)",
            (username, password_hash, datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()

async def get_user(username: str) -> Optional[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        row = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
        conn.close()
        return dict(row) if row else None

async def upsert_conversation(conv_id: str, owner_username: str, title: str | None = None) -> None:
    async with _lock:
        conn = _connect()
        conn.execute(
            "INSERT OR IGNORE INTO conversations(id, owner_username, title, created_at) VALUES(?,?,?,?)",
            (conv_id, owner_username, title, datetime.utcnow().isoformat()),
        )
        if title is not None:
            conn.execute("UPDATE conversations SET title=? WHERE id=?", (title, conv_id))
        conn.commit()
        conn.close()

async def list_conversations(owner_username: str) -> List[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        rows = conn.execute(
            "SELECT * FROM conversations WHERE owner_username=? ORDER BY created_at DESC",
            (owner_username,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

async def add_file_upload(
    file_id: str,
    owner_username: str,
    datasource_id: str,
    filename: str,
    sheet_name: str | None,
    table_name: str,
    row_count: int,
    columns_json: str,
) -> None:
    async with _lock:
        conn = _connect()
        conn.execute(
            "INSERT INTO file_uploads(id, owner_username, datasource_id, filename, sheet_name, table_name, row_count, columns_json, created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (
                file_id,
                owner_username,
                datasource_id,
                filename,
                sheet_name,
                table_name,
                row_count,
                columns_json,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        conn.close()

async def list_file_uploads(owner_username: str, datasource_id: str) -> List[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        rows = conn.execute(
            "SELECT * FROM file_uploads WHERE owner_username=? AND datasource_id=? ORDER BY created_at DESC",
            (owner_username, datasource_id),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

async def get_file_upload(file_id: str) -> Optional[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        row = conn.execute("SELECT * FROM file_uploads WHERE id=?", (file_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

async def get_file_upload_by_table(
    owner_username: str, datasource_id: str, table_name: str
) -> Optional[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        row = conn.execute(
            "SELECT * FROM file_uploads WHERE owner_username=? AND datasource_id=? AND table_name=?",
            (owner_username, datasource_id, table_name),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

async def delete_file_upload(file_id: str) -> None:
    async with _lock:
        conn = _connect()
        conn.execute("DELETE FROM file_uploads WHERE id=?", (file_id,))
        conn.commit()
        conn.close()

async def delete_file_uploads(file_ids: List[str]) -> None:
    if not file_ids:
        return
    async with _lock:
        conn = _connect()
        placeholders = ",".join(["?"] * len(file_ids))
        conn.execute(f"DELETE FROM file_uploads WHERE id IN ({placeholders})", file_ids)
        conn.commit()
        conn.close()

async def list_expired_file_uploads(ttl_hours: int) -> List[Dict[str, Any]]:
    if ttl_hours <= 0:
        return []
    cutoff = datetime.utcnow() - timedelta(hours=ttl_hours)
    async with _lock:
        conn = _connect()
        rows = conn.execute(
            "SELECT * FROM file_uploads WHERE created_at < ? ORDER BY created_at ASC",
            (cutoff.isoformat(),),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

async def get_conversation(conv_id: str) -> Optional[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        row = conn.execute("SELECT * FROM conversations WHERE id=?", (conv_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

async def delete_conversation(conv_id: str) -> None:
    async with _lock:
        conn = _connect()
        conn.execute("DELETE FROM messages WHERE conversation_id=?", (conv_id,))
        conn.execute("DELETE FROM message_artifacts WHERE conversation_id=?", (conv_id,))
        conn.execute("DELETE FROM conversations WHERE id=?", (conv_id,))
        conn.commit()
        conn.close()

async def add_message(conv_id: str, role: str, content: str) -> int:
    async with _lock:
        conn = _connect()
        cur = conn.execute(
            "INSERT INTO messages(conversation_id, role, content, created_at) VALUES(?,?,?,?)",
            (conv_id, role, content, datetime.utcnow().isoformat()),
        )
        conn.commit()
        msg_id = int(cur.lastrowid)
        conn.close()
        return msg_id

async def get_messages(conv_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        rows = conn.execute(
            "SELECT id, role, content, created_at FROM messages WHERE conversation_id=? "
            "ORDER BY id DESC LIMIT ?",
            (conv_id, limit),
        ).fetchall()
        conn.close()
        # reverse to chronological
        return [dict(r) for r in reversed(rows)]

async def get_message_by_id(message_id: int) -> Optional[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        row = conn.execute(
            "SELECT id, conversation_id, role, content, created_at FROM messages WHERE id=?",
            (message_id,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

async def add_message_artifact(
    conv_id: str,
    user_message_id: int,
    sql_text: str,
    columns_json: str,
    rows_json: str,
    chart_json: str | None,
    analysis_text: str | None,
    explain_text: str | None = None,
    suggest_text: str | None = None,
    safety_text: str | None = None,
    fix_text: str | None = None,
) -> None:
    async with _lock:
        conn = _connect()
        conn.execute(
            "INSERT INTO message_artifacts(conversation_id, user_message_id, sql_text, columns_json, rows_json, chart_json, analysis_text, explain_text, suggest_text, safety_text, fix_text, created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                conv_id,
                user_message_id,
                sql_text,
                columns_json,
                rows_json,
                chart_json,
                analysis_text,
                explain_text,
                suggest_text,
                safety_text,
                fix_text,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        conn.close()

async def get_message_artifact(conv_id: str, user_message_id: int) -> Optional[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        row = conn.execute(
            "SELECT * FROM message_artifacts WHERE conversation_id=? AND user_message_id=? ORDER BY id DESC LIMIT 1",
            (conv_id, user_message_id),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

async def add_sql_audit(
    *,
    user_username: str,
    conversation_id: str | None,
    message_id: int | None,
    datasource_id: str | None,
    sql_text: str,
    row_count: int | None,
    elapsed_ms: int | None,
    success: bool,
    error_message: str | None,
    slow: bool,
) -> None:
    async with _lock:
        conn = _connect()
        conn.execute(
            "INSERT INTO sql_audits(user_username, conversation_id, message_id, datasource_id, sql_text, row_count, elapsed_ms, success, error_message, slow, created_at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                user_username,
                conversation_id,
                message_id,
                datasource_id,
                sql_text,
                row_count,
                elapsed_ms,
                1 if success else 0,
                error_message,
                1 if slow else 0,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        conn.close()

async def list_sql_audits(username: str, limit: int = 200) -> List[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        rows = conn.execute(
            "SELECT * FROM sql_audits WHERE user_username=? ORDER BY id DESC LIMIT ?",
            (username, limit),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

async def add_datasource(
    ds_id: str,
    name: str,
    ds_type: str,
    config_json: str,
    is_default: bool,
) -> None:
    async with _lock:
        conn = _connect()
        if is_default:
            conn.execute("UPDATE data_sources SET is_default=0")
        conn.execute(
            "INSERT INTO data_sources(id, name, type, config_json, is_default, created_at) "
            "VALUES(?,?,?,?,?,?)",
            (ds_id, name, ds_type, config_json, 1 if is_default else 0, datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()

async def list_datasources() -> List[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        rows = conn.execute("SELECT * FROM data_sources ORDER BY created_at DESC").fetchall()
        conn.close()
        return [dict(r) for r in rows]

async def get_datasource(ds_id: str) -> Optional[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        row = conn.execute("SELECT * FROM data_sources WHERE id=?", (ds_id,)).fetchone()
        conn.close()
        return dict(row) if row else None

async def get_default_datasource() -> Optional[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        row = conn.execute("SELECT * FROM data_sources WHERE is_default=1 LIMIT 1").fetchone()
        conn.close()
        return dict(row) if row else None

async def set_default_datasource(ds_id: str) -> None:
    async with _lock:
        conn = _connect()
        conn.execute("UPDATE data_sources SET is_default=0")
        conn.execute("UPDATE data_sources SET is_default=1 WHERE id=?", (ds_id,))
        conn.commit()
        conn.close()

async def update_datasource_training(ds_id: str, ok: bool, error: str | None) -> None:
    async with _lock:
        conn = _connect()
        conn.execute(
            "UPDATE data_sources SET training_ok=?, training_error=?, last_trained_at=? WHERE id=?",
            (1 if ok else 0, error, datetime.utcnow().isoformat(), ds_id),
        )
        conn.commit()
        conn.close()
