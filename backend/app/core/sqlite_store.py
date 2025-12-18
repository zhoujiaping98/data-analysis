from __future__ import annotations

import os
import sqlite3
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime

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
            """
        )
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

async def add_message(conv_id: str, role: str, content: str) -> None:
    async with _lock:
        conn = _connect()
        conn.execute(
            "INSERT INTO messages(conversation_id, role, content, created_at) VALUES(?,?,?,?)",
            (conv_id, role, content, datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()

async def get_messages(conv_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    async with _lock:
        conn = _connect()
        rows = conn.execute(
            "SELECT role, content, created_at FROM messages WHERE conversation_id=? "
            "ORDER BY id DESC LIMIT ?",
            (conv_id, limit),
        ).fetchall()
        conn.close()
        # reverse to chronological
        return [dict(r) for r in reversed(rows)]
