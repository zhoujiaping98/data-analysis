from __future__ import annotations

import logging
import uuid
from datetime import datetime
import time
import orjson
from typing import AsyncGenerator, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Header
from fastapi.responses import StreamingResponse

from backend.app.api.deps import get_current_user
from backend.app.schemas.chat import ChatRequest
from backend.app.core.sse import sse_event, sse_stream
from backend.app.core.config import settings
from backend.app.core.sqlite_store import (
    add_message,
    add_message_artifact,
    upsert_conversation,
    get_messages,
    get_conversation,
    add_sql_audit,
)
from backend.app.core.mysql import run_sql, extract_table_names, list_tables
from backend.app.services.schema_context import build_schema_context
from backend.app.services.sql_generator import generate_sql
from backend.app.services.charting import suggest_echarts_option
from backend.app.services.analyzer import analyze_stream
from backend.app.services.sql_assistant import (
    explain_sql,
    suggest_sql_improvement,
    suggest_sql_fix,
    generate_safety_tips,
)
from backend.app.core.resilience import CircuitOpenError
from backend.app.core.sqlite_store import list_file_uploads
from backend.app.core.datasources import resolve_datasource
from backend.app.core.uploads import cleanup_expired_uploads
from backend.app.core.audit import mask_sensitive_rows

router = APIRouter()
log = logging.getLogger("chat")


def _json_default(obj):
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    return str(obj)


@router.post("/chat/sse")
async def chat_sse(
    req: ChatRequest,
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    if not settings.has_llm_config:
        raise HTTPException(status_code=500, detail="LLM config missing (.env)")
    # datasource config resolved below
    await cleanup_expired_uploads()

    async def gen() -> AsyncGenerator[str, None]:
        request_id = str(uuid.uuid4())

        # Ensure conversation exists
        await upsert_conversation(req.conversation_id, owner_username=user["username"])
        user_msg_id = await add_message(req.conversation_id, "user", req.message)
        yield sse_event("message", {"user_message_id": user_msg_id, "request_id": request_id})
        conv = await get_conversation(req.conversation_id)
        existing_title = (conv.get("title") or "").strip() if conv else ""
        if conv and (not existing_title or existing_title in {"New Conversation", "新会话"}):
            title = " ".join(req.message.strip().split())
            if title:
                await upsert_conversation(req.conversation_id, owner_username=user["username"], title=title)

        # Build history (for SQL generation & analysis)
        history_rows = await get_messages(req.conversation_id, limit=20)
        history = [{"role": r["role"], "content": r["content"]} for r in history_rows if r["role"] in ("user","assistant")]

        yield sse_event("status", {"stage": "schema_retrieval", "request_id": request_id})
        ds_id, ds_cfg = await resolve_datasource(x_datasource_id)
        if not all([ds_cfg.get("host"), ds_cfg.get("database"), ds_cfg.get("user"), ds_cfg.get("password")]):
            raise HTTPException(status_code=500, detail="MySQL datasource config missing")
        # Build allowed table scope (optional user-selected list)
        try:
            base_tables = await list_tables(ds_cfg, ds_id)
        except Exception as e:
            yield sse_event("error", {"message": str(e), "request_id": request_id, "where": "schema_tables"})
            yield sse_event("done", {"ok": False, "request_id": request_id})
            return
        uploads = await list_file_uploads(user["username"], ds_id)
        upload_names = {u["table_name"] for u in uploads}
        available_tables = {t["name"] for t in base_tables if not t["name"].startswith("tmp_")} | upload_names

        requested_tables = {t for t in (req.allowed_tables or []) if t}
        if requested_tables:
            invalid = sorted(list(requested_tables - available_tables))
            if invalid:
                yield sse_event(
                    "error",
                    {
                        "message": "Selected tables are not available.",
                        "request_id": request_id,
                        "where": "scope_validation",
                        "tables": invalid,
                    },
                )
                yield sse_event("done", {"ok": False, "request_id": request_id})
                return
            allowed_tables = requested_tables
        else:
            allowed_tables = available_tables

        schema_context = build_schema_context(req.message, ds_id, allowed_tables=allowed_tables)

        yield sse_event("status", {"stage": "sql_generation", "request_id": request_id})
        try:
            sql = await generate_sql(
                req.message,
                schema_context,
                history,
                allowed_tables=sorted(list(allowed_tables)),
                table_lock=bool(req.table_lock),
            )
        except CircuitOpenError as e:
            yield sse_event("error", {"message": str(e), "request_id": request_id, "where": "llm_sql"})
            yield sse_event("done", {"ok": False, "request_id": request_id})
            return
        yield sse_event("sql", {"sql": sql, "request_id": request_id})
        explain_text = await explain_sql(sql)
        if explain_text:
            yield sse_event("sql_explain", {"text": explain_text, "request_id": request_id})

        # Enforce table allowlist to avoid cross-schema access.

        # Execute (retry: regenerate SQL if SQL error)
        cols = []
        rows = []
        last_err: str | None = None
        elapsed_ms = None
        for attempt in range(settings.MAX_SQL_RETRY + 1):
            try:
                used_tables = set(extract_table_names(sql))
                if used_tables and not used_tables.issubset(allowed_tables):
                    last_err = "SQL references tables not allowed for this user."
                    yield sse_event(
                        "error",
                        {
                            "message": last_err,
                            "request_id": request_id,
                            "where": "sql_allowlist",
                            "tables": sorted(list(used_tables - allowed_tables)),
                        },
                    )
                    break
                yield sse_event("status", {"stage": "sql_execution", "attempt": attempt, "request_id": request_id})
                t0 = time.perf_counter()
                cols, rows = await run_sql(sql, max_rows=settings.MAX_ROWS, config=ds_cfg, cache_key=ds_id)
                elapsed_ms = int((time.perf_counter() - t0) * 1000)
                last_err = None
                break
            except CircuitOpenError as e:
                last_err = str(e)
                yield sse_event("error", {"message": last_err, "request_id": request_id, "where": "sql_execution"})
                break
            except Exception as e:
                last_err = str(e)
                yield sse_event("error", {"message": last_err, "request_id": request_id, "where": "sql_execution"})
                if attempt >= settings.MAX_SQL_RETRY:
                    break
                # try to fix by asking LLM to rewrite SQL
                fix_prompt = (
                    "The SQL failed to run. Please rewrite a correct MySQL SELECT query. "
                    "Only output SQL. "
                    f"Error: {last_err}\nSQL: {sql}"
                )
                try:
                    sql = await generate_sql(
                        fix_prompt,
                        schema_context,
                        history,
                        allowed_tables=sorted(list(allowed_tables)),
                        table_lock=bool(req.table_lock),
                    )
                except CircuitOpenError as e:
                    last_err = str(e)
                    yield sse_event("error", {"message": last_err, "request_id": request_id, "where": "llm_sql"})
                    break
                yield sse_event("sql", {"sql": sql, "request_id": request_id, "note": "retry_rewrite"})

        if last_err is not None:
            fix_text = await suggest_sql_fix(sql, last_err)
            if fix_text:
                yield sse_event("sql_fix", {"text": fix_text, "request_id": request_id})
            else:
                yield sse_event("sql_fix", {"text": "无法自动修复，请检查 SQL 或调整问题描述。", "request_id": request_id})
            try:
                await add_sql_audit(
                    user_username=user["username"],
                    conversation_id=req.conversation_id,
                    message_id=user_msg_id,
                    datasource_id=ds_id,
                    sql_text=sql,
                    row_count=None,
                    elapsed_ms=elapsed_ms,
                    success=False,
                    error_message=last_err,
                    slow=False,
                )
            except Exception:
                pass
            yield sse_event("done", {"ok": False, "request_id": request_id})
            return

        cols, rows = mask_sensitive_rows(
            cols,
            rows,
            keep_start=settings.SENSITIVE_MASK_KEEP_START,
            keep_end=settings.SENSITIVE_MASK_KEEP_END,
        )
        yield sse_event("table", {"columns": cols, "rows": rows, "request_id": request_id, "row_count": len(rows)})
        slow = bool(elapsed_ms is not None and elapsed_ms >= settings.SLOW_QUERY_THRESHOLD_MS)
        if slow:
            yield sse_event(
                "warning",
                {"message": "本次查询耗时较长，建议增加过滤条件或限制返回行数。", "elapsed_ms": elapsed_ms},
            )
        safety_tips = generate_safety_tips(sql, len(rows), elapsed_ms)
        if safety_tips:
            yield sse_event("sql_safety", {"tips": safety_tips, "request_id": request_id})

        yield sse_event("status", {"stage": "chart_generation", "request_id": request_id})
        option = suggest_echarts_option(cols, rows)
        if option:
            yield sse_event("chart", {"echarts_option": option, "request_id": request_id})
        else:
            yield sse_event("chart", {"echarts_option": None, "request_id": request_id})

        yield sse_event("status", {"stage": "analysis_generation", "request_id": request_id})
        analysis_parts: list[str] = []
        async for chunk in analyze_stream(req.message, sql, cols, rows):
            analysis_parts.append(chunk)
            yield sse_event("analysis", {"delta": chunk, "request_id": request_id})
        analysis = "".join(analysis_parts).strip()
        yield sse_event("analysis", {"text": analysis, "request_id": request_id, "done": True})

        suggest_text = await suggest_sql_improvement(req.message, sql, len(rows), elapsed_ms)
        if suggest_text:
            yield sse_event("sql_suggest", {"text": suggest_text, "request_id": request_id})

        try:
            await add_message_artifact(
                conv_id=req.conversation_id,
                user_message_id=user_msg_id,
                sql_text=sql,
                columns_json=orjson.dumps(cols, default=_json_default).decode("utf-8"),
                rows_json=orjson.dumps(rows, default=_json_default).decode("utf-8"),
                chart_json=orjson.dumps(option, default=_json_default).decode("utf-8") if option else None,
                analysis_text=analysis,
                explain_text=explain_text,
                suggest_text=suggest_text,
                safety_text="\n".join(safety_tips) if safety_tips else None,
                fix_text=None,
                view_json=None,
            )
        except Exception:
            pass

        try:
            await add_sql_audit(
                user_username=user["username"],
                conversation_id=req.conversation_id,
                message_id=user_msg_id,
                datasource_id=ds_id,
                sql_text=sql,
                row_count=len(rows),
                elapsed_ms=elapsed_ms,
                success=True,
                error_message=None,
                slow=slow,
            )
        except Exception:
            pass

        await add_message(req.conversation_id, "assistant", f"[SQL]\n{sql}\n\n[Analysis]\n{analysis}")

        yield sse_event("done", {"ok": True, "request_id": request_id})

    return StreamingResponse(sse_stream(gen()), media_type="text/event-stream")
