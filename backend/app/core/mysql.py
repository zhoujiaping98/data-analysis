from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, List, Tuple

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError, TimeoutError as SATimeoutError

from backend.app.core.config import settings
from backend.app.core.resilience import CircuitBreaker, async_retry

log = logging.getLogger("mysql")

_engine_cache: Dict[str, AsyncEngine] = {}
_sync_engine_cache: Dict[str, Any] = {}
_IDENT = re.compile(r"^[A-Za-z0-9_]+$")
_mysql_breaker = CircuitBreaker(
    "mysql",
    failure_threshold=settings.MYSQL_CB_FAILURES,
    recovery_timeout_s=settings.MYSQL_CB_RECOVERY_SECONDS,
)

def _normalize_config(config: Dict[str, Any] | None) -> Dict[str, Any]:
    if config is not None:
        return config
    if not settings.has_mysql_config:
        raise RuntimeError("MySQL config missing")
    return {
        "host": settings.MYSQL_HOST,
        "port": settings.MYSQL_PORT,
        "database": settings.MYSQL_DATABASE,
        "user": settings.MYSQL_USER,
        "password": settings.MYSQL_PASSWORD,
    }


def _dsn_from_config(config: Dict[str, Any], async_driver: bool) -> str:
    prefix = "mysql+aiomysql" if async_driver else "mysql+pymysql"
    return (
        f"{prefix}://{config['user']}:{config['password']}"
        f"@{config['host']}:{config['port']}/{config['database']}"
    )


def _get_engine(config: Dict[str, Any] | None, cache_key: str) -> AsyncEngine:
    cfg = _normalize_config(config)
    key = cache_key or "default"
    if key not in _engine_cache:
        dsn = _dsn_from_config(cfg, True)
        _engine_cache[key] = create_async_engine(
            dsn,
            pool_pre_ping=True,
            pool_size=settings.MYSQL_POOL_SIZE,
            max_overflow=settings.MYSQL_MAX_OVERFLOW,
            pool_recycle=settings.MYSQL_POOL_RECYCLE_SECONDS,
            connect_args={"connect_timeout": settings.MYSQL_CONNECT_TIMEOUT_SECONDS},
        )
    return _engine_cache[key]


def _get_sync_engine(config: Dict[str, Any] | None, cache_key: str):
    cfg = _normalize_config(config)
    key = cache_key or "default"
    if key not in _sync_engine_cache:
        dsn = _dsn_from_config(cfg, False)
        _sync_engine_cache[key] = create_engine(
            dsn,
            pool_pre_ping=True,
            pool_size=settings.MYSQL_POOL_SIZE,
            max_overflow=settings.MYSQL_MAX_OVERFLOW,
            pool_recycle=settings.MYSQL_POOL_RECYCLE_SECONDS,
            connect_args={"connect_timeout": settings.MYSQL_CONNECT_TIMEOUT_SECONDS},
        )
    return _sync_engine_cache[key]


async def close_engine() -> None:
    for eng in list(_engine_cache.values()):
        await eng.dispose()
    _engine_cache.clear()
    for eng in list(_sync_engine_cache.values()):
        eng.dispose()
    _sync_engine_cache.clear()


_BAD_SQL = re.compile(r"\b(INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|REPLACE|GRANT|REVOKE)\b", re.I)
_GOOD_PREFIX = re.compile(r"^\s*(SELECT|WITH)\b", re.I)

def _is_retryable_mysql(err: BaseException) -> bool:
    if isinstance(err, (asyncio.TimeoutError, SATimeoutError)):
        return True
    if isinstance(err, DBAPIError):
        if getattr(err, "connection_invalidated", False):
            return True
        return isinstance(err, OperationalError)
    return False

async def _apply_query_timeout(conn) -> None:
    if settings.MYSQL_QUERY_TIMEOUT_SECONDS <= 0:
        return
    try:
        ms = int(settings.MYSQL_QUERY_TIMEOUT_SECONDS * 1000)
        await conn.execute(text("SET SESSION MAX_EXECUTION_TIME=:ms"), {"ms": ms})
    except Exception:
        # Best effort: not all MySQL flavors support this.
        pass

async def _with_timeout(coro):
    if settings.MYSQL_QUERY_TIMEOUT_SECONDS <= 0:
        return await coro
    return await asyncio.wait_for(coro, timeout=settings.MYSQL_QUERY_TIMEOUT_SECONDS)

async def _execute_fetchmany(
    sql: str,
    params: Dict[str, Any] | None,
    max_rows: int,
    config: Dict[str, Any] | None,
    cache_key: str,
) -> Tuple[List[str], List[Any]]:
    engine = _get_engine(config, cache_key)
    async with engine.connect() as conn:
        await _apply_query_timeout(conn)
        res = await conn.execute(text(sql), params or {})
        rows = res.fetchmany(size=max_rows)
        cols = list(res.keys())
    return cols, rows

async def _execute_fetchall(
    sql: str,
    params: Dict[str, Any] | None,
    config: Dict[str, Any] | None,
    cache_key: str,
) -> Tuple[List[str], List[Any]]:
    engine = _get_engine(config, cache_key)
    async with engine.connect() as conn:
        await _apply_query_timeout(conn)
        res = await conn.execute(text(sql), params or {})
        rows = res.fetchall()
        cols = list(res.keys())
    return cols, rows

async def _execute_noresult(
    sql: str,
    params: Dict[str, Any] | None,
    config: Dict[str, Any] | None,
    cache_key: str,
) -> None:
    engine = _get_engine(config, cache_key)
    async with engine.connect() as conn:
        await _apply_query_timeout(conn)
        await conn.execute(text(sql), params or {})

async def _with_mysql_retry(op):
    _mysql_breaker.check()
    try:
        result = await async_retry(
            op,
            retries=settings.MYSQL_MAX_RETRIES,
            base_delay_s=settings.MYSQL_RETRY_BASE_SECONDS,
            should_retry=_is_retryable_mysql,
        )
        _mysql_breaker.record_success()
        return result
    except Exception:
        _mysql_breaker.record_failure()
        raise

def validate_readonly_sql(sql: str) -> None:
    if not _GOOD_PREFIX.search(sql):
        raise ValueError("Only SELECT/WITH queries are allowed.")
    if _BAD_SQL.search(sql):
        raise ValueError("Write/DDL statements are not allowed.")


async def run_sql(
    sql: str,
    max_rows: int,
    config: Dict[str, Any] | None = None,
    cache_key: str = "default",
) -> Tuple[List[str], List[List[Any]]]:
    validate_readonly_sql(sql)
    async def _op():
        return await _with_timeout(_execute_fetchmany(sql, None, max_rows, config, cache_key))

    cols, rows = await _with_mysql_retry(_op)
    return cols, [list(r) for r in rows]


async def fetch_schema_documents(
    config: Dict[str, Any] | None = None,
    cache_key: str = "default",
) -> List[Dict[str, Any]]:
    """Fetch schema info from information_schema and return docs for vector store."""
    sql = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_COMMENT
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :db
    ORDER BY TABLE_NAME, ORDINAL_POSITION
    """
    cfg = _normalize_config(config)
    async def _op():
        return await _with_timeout(_execute_fetchall(sql, {"db": cfg["database"]}, config, cache_key))
    _, rows = await _with_mysql_retry(_op)

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


async def fetch_schema_documents_for_table(
    table_name: str,
    config: Dict[str, Any] | None = None,
    cache_key: str = "default",
) -> List[Dict[str, Any]]:
    if not _IDENT.fullmatch(table_name or ""):
        raise ValueError("Invalid table name")
    sql = """
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_COMMENT
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :table
    ORDER BY ORDINAL_POSITION
    """
    cfg = _normalize_config(config)
    async def _op():
        return await _with_timeout(
            _execute_fetchall(sql, {"db": cfg["database"], "table": table_name}, config, cache_key)
        )
    _, rows = await _with_mysql_retry(_op)

    if not rows:
        return []

    lines = [f"TABLE {table_name}:"]
    for r in rows:
        extra = []
        if r.COLUMN_KEY:
            extra.append(f"key={r.COLUMN_KEY}")
        if r.IS_NULLABLE:
            extra.append(f"nullable={r.IS_NULLABLE}")
        if r.COLUMN_COMMENT:
            extra.append(f"comment={r.COLUMN_COMMENT}")
        meta = ", ".join(extra)
        lines.append(f"  - {r.COLUMN_NAME} ({r.COLUMN_TYPE}) {meta}".rstrip())
    text_blob = "\n".join(lines)

    return [
        {"id": f"table::{table_name}", "text": text_blob, "metadata": {"table": table_name}}
    ]


def _quote_ident(name: str) -> str:
    if not _IDENT.fullmatch(name or ""):
        raise ValueError("Invalid table name")
    return f"`{name}`"


async def list_tables(
    config: Dict[str, Any] | None = None,
    cache_key: str = "default",
) -> List[Dict[str, Any]]:
    sql = """
    SELECT TABLE_NAME, TABLE_TYPE, TABLE_COMMENT
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = :db
    ORDER BY TABLE_NAME
    """
    cfg = _normalize_config(config)
    async def _op():
        return await _with_timeout(_execute_fetchall(sql, {"db": cfg["database"]}, config, cache_key))
    _, rows = await _with_mysql_retry(_op)
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


async def preview_table(
    table_name: str,
    *,
    limit: int = 10,
    config: Dict[str, Any] | None = None,
    cache_key: str = "default",
) -> Tuple[List[str], List[List[Any]]]:
    if limit < 1:
        limit = 1
    if limit > 100:
        limit = 100

    tables = await list_tables(config, cache_key)
    allowed = {t["name"] for t in tables}
    if table_name not in allowed:
        raise ValueError("Table not found")

    sql = f"SELECT * FROM {_quote_ident(table_name)} LIMIT :limit"
    async def _op():
        return await _with_timeout(_execute_fetchmany(sql, {"limit": limit}, limit, config, cache_key))
    cols, rows = await _with_mysql_retry(_op)
    return cols, [list(r) for r in rows]


async def preview_table_page(
    table_name: str,
    *,
    page: int = 1,
    page_size: int = 50,
    config: Dict[str, Any] | None = None,
    cache_key: str = "default",
) -> Tuple[List[str], List[List[Any]], int]:
    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 1
    if page_size > 200:
        page_size = 200

    tables = await list_tables(config, cache_key)
    allowed = {t["name"] for t in tables}
    if table_name not in allowed:
        raise ValueError("Table not found")

    count_sql = f"SELECT COUNT(*) AS cnt FROM {_quote_ident(table_name)}"
    async def _op_count():
        return await _with_timeout(_execute_fetchall(count_sql, None, config, cache_key))
    _, count_rows = await _with_mysql_retry(_op_count)
    total = int(count_rows[0].cnt) if count_rows else 0

    offset = (page - 1) * page_size
    data_sql = f"SELECT * FROM {_quote_ident(table_name)} LIMIT :limit OFFSET :offset"
    async def _op_data():
        return await _with_timeout(_execute_fetchmany(data_sql, {"limit": page_size, "offset": offset}, page_size, config, cache_key))
    cols, rows = await _with_mysql_retry(_op_data)
    return cols, [list(r) for r in rows], total


def import_dataframe(
    table_name: str,
    df,
    config: Dict[str, Any] | None = None,
    cache_key: str = "default",
) -> None:
    if not _IDENT.fullmatch(table_name or ""):
        raise ValueError("Invalid table name")
    engine = _get_sync_engine(config, cache_key)
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)


async def drop_table(
    table_name: str,
    config: Dict[str, Any] | None = None,
    cache_key: str = "default",
) -> None:
    sql = f"DROP TABLE IF EXISTS {_quote_ident(table_name)}"
    async def _op():
        return await _with_timeout(_execute_noresult(sql, None, config, cache_key))
    await _with_mysql_retry(_op)


_TABLE_REF_RE = re.compile(r"\b(from|join)\s+([`\"\\[]?[\w]+[`\"\\]]?(?:\.[`\"\\[]?[\w]+[`\"\\]]?)?)", re.I)

def extract_table_names(sql: str) -> List[str]:
    names: List[str] = []
    for m in _TABLE_REF_RE.finditer(sql or ""):
        ident = m.group(2)
        if not ident:
            continue
        if ident.startswith("("):
            continue
        ident = ident.strip("`\"[]")
        if "." in ident:
            ident = ident.split(".")[-1]
        names.append(ident)
    return names


async def ping(config: Dict[str, Any] | None = None, cache_key: str = "default") -> bool:
    try:
        async def _op():
            return await _with_timeout(_execute_fetchall("SELECT 1", None, config, cache_key))
        await _with_mysql_retry(_op)
        return True
    except Exception:
        return False
