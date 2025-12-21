from __future__ import annotations

import logging
import uuid
from datetime import datetime
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
)
from backend.app.core.mysql import run_sql, extract_table_names, list_tables
from backend.app.services.schema_context import build_schema_context
from backend.app.services.sql_generator import generate_sql
from backend.app.services.charting import suggest_echarts_option
from backend.app.services.analyzer import analyze_stream
from backend.app.core.resilience import CircuitOpenError
from backend.app.core.sqlite_store import list_file_uploads
from backend.app.core.datasources import resolve_datasource
from backend.app.core.uploads import cleanup_expired_uploads

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
        schema_context = build_schema_context(req.message, ds_id)

        yield sse_event("status", {"stage": "sql_generation", "request_id": request_id})
        try:
            sql = await generate_sql(req.message, schema_context, history)
        except CircuitOpenError as e:
            yield sse_event("error", {"message": str(e), "request_id": request_id, "where": "llm_sql"})
            yield sse_event("done", {"ok": False, "request_id": request_id})
            return
        yield sse_event("sql", {"sql": sql, "request_id": request_id})

        # Enforce table allowlist (base tables + user uploads) to avoid cross-schema access.
        try:
            base_tables = await list_tables(ds_cfg, ds_id)
        except Exception as e:
            yield sse_event("error", {"message": str(e), "request_id": request_id, "where": "schema_tables"})
            yield sse_event("done", {"ok": False, "request_id": request_id})
            return
        uploads = await list_file_uploads(user["username"], ds_id)
        upload_names = {u["table_name"] for u in uploads}
        allowed_tables = {t["name"] for t in base_tables if not t["name"].startswith("tmp_")} | upload_names

        # Execute (retry: regenerate SQL if SQL error)
        cols = []
        rows = []
        last_err: str | None = None
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
                cols, rows = await run_sql(sql, max_rows=settings.MAX_ROWS, config=ds_cfg, cache_key=ds_id)
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
                    sql = await generate_sql(fix_prompt, schema_context, history)
                except CircuitOpenError as e:
                    last_err = str(e)
                    yield sse_event("error", {"message": last_err, "request_id": request_id, "where": "llm_sql"})
                    break
                yield sse_event("sql", {"sql": sql, "request_id": request_id, "note": "retry_rewrite"})

        if last_err is not None:
            yield sse_event("done", {"ok": False, "request_id": request_id})
            return

        yield sse_event("table", {"columns": cols, "rows": rows, "request_id": request_id, "row_count": len(rows)})

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

        try:
            await add_message_artifact(
                conv_id=req.conversation_id,
                user_message_id=user_msg_id,
                sql_text=sql,
                columns_json=orjson.dumps(cols, default=_json_default).decode("utf-8"),
                rows_json=orjson.dumps(rows, default=_json_default).decode("utf-8"),
                chart_json=orjson.dumps(option, default=_json_default).decode("utf-8") if option else None,
                analysis_text=analysis,
            )
        except Exception:
            pass

        await add_message(req.conversation_id, "assistant", f"[SQL]\n{sql}\n\n[Analysis]\n{analysis}")

        yield sse_event("done", {"ok": True, "request_id": request_id})

    return StreamingResponse(sse_stream(gen()), media_type="text/event-stream")
