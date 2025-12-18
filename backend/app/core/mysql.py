from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Tuple

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import text

from backend.app.core.config import settings

log = logging.getLogger("mysql")

_engine: AsyncEngine | None = None
_IDENT = re.compile(r"^[A-Za-z0-9_]+$")

def _get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        if not settings.has_mysql_config:
            raise RuntimeError("MySQL config missing")
        _engine = create_async_engine(settings.mysql_dsn, pool_pre_ping=True)
    return _engine


async def close_engine() -> None:
    global _engine
    if _engine is None:
        return
    await _engine.dispose()
    _engine = None


_BAD_SQL = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE|GRANT|REVOKE)\b", re.I)
_GOOD_PREFIX = re.compile(r"^\s*(SELECT|WITH)\b", re.I)

def validate_readonly_sql(sql: str) -> None:
    if not _GOOD_PREFIX.search(sql):
        raise ValueError("Only SELECT/WITH queries are allowed.")
    if _BAD_SQL.search(sql):
        raise ValueError("Write/DDL statements are not allowed.")


async def run_sql(sql: str, max_rows: int) -> Tuple[List[str], List[List[Any]]]:
    validate_readonly_sql(sql)
    engine = _get_engine()
    async with engine.connect() as conn:
        res = await conn.execute(text(sql))
        rows = res.fetchmany(size=max_rows)
        cols = list(res.keys())
    return cols, [list(r) for r in rows]


async def fetch_schema_documents() -> List[Dict[str, Any]]:
    """Fetch schema info from information_schema and return docs for vector store."""
    engine = _get_engine()
    sql = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_COMMENT
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :db
    ORDER BY TABLE_NAME, ORDINAL_POSITION
    """
    async with engine.connect() as conn:
        res = await conn.execute(text(sql), {"db": settings.MYSQL_DATABASE})
        rows = res.fetchall()

    by_table: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_table.setdefault(r.TABLE_NAME, []).append(
            {
                "column": r.COLUMN_NAME,
                "data_type": r.DATA_TYPE,
                "column_type": r.COLUMN_TYPE,
                "nullable": r.IS_NULLABLE,
                "key": r.COLUMN_KEY,
                "comment": r.COLUMN_COMMENT or "",
            }
        )

    docs: List[Dict[str, Any]] = []
    for table, cols in by_table.items():
        lines = [f"TABLE {table}:"]
        for c in cols:
            extra = []
            if c["key"]:
                extra.append(f"key={c['key']}")
            if c["nullable"]:
                extra.append(f"nullable={c['nullable']}")
            if c["comment"]:
                extra.append(f"comment={c['comment']}")
            meta = ", ".join(extra)
            lines.append(f"  - {c['column']} ({c['column_type']}) {meta}".rstrip())
        text_blob = "\n".join(lines)

        docs.append(
            {
                "id": f"table::{table}",
                "text": text_blob,
                "metadata": {"table": table},
            }
        )
    return docs


def _quote_ident(name: str) -> str:
    if not _IDENT.fullmatch(name or ""):
        raise ValueError("Invalid table name")
    return f"`{name}`"


async def list_tables() -> List[Dict[str, Any]]:
    engine = _get_engine()
    sql = """
    SELECT TABLE_NAME, TABLE_TYPE, TABLE_COMMENT
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = :db
    ORDER BY TABLE_NAME
    """
    async with engine.connect() as conn:
        res = await conn.execute(text(sql), {"db": settings.MYSQL_DATABASE})
        rows = res.fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "name": r.TABLE_NAME,
                "type": r.TABLE_TYPE,
                "comment": r.TABLE_COMMENT or "",
            }
        )
    return out


async def preview_table(table_name: str, *, limit: int = 10) -> Tuple[List[str], List[List[Any]]]:
    if limit < 1:
        limit = 1
    if limit > 100:
        limit = 100

    tables = await list_tables()
    allowed = {t["name"] for t in tables}
    if table_name not in allowed:
        raise ValueError("Table not found")

    sql = f"SELECT * FROM {_quote_ident(table_name)} LIMIT :limit"
    engine = _get_engine()
    async with engine.connect() as conn:
        res = await conn.execute(text(sql), {"limit": limit})
        rows = res.fetchmany(size=limit)
        cols = list(res.keys())
    return cols, [list(r) for r in rows]
