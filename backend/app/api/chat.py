from __future__ import annotations

import logging
import uuid
from typing import AsyncGenerator, Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from backend.app.api.deps import get_current_user
from backend.app.schemas.chat import ChatRequest
from backend.app.core.sse import sse_event, sse_stream
from backend.app.core.config import settings
from backend.app.core.sqlite_store import add_message, upsert_conversation, get_messages
from backend.app.core.mysql import run_sql
from backend.app.services.schema_context import build_schema_context
from backend.app.services.sql_generator import generate_sql
from backend.app.services.charting import suggest_echarts_option
from backend.app.services.analyzer import analyze

router = APIRouter()
log = logging.getLogger("chat")


@router.post("/chat/sse")
async def chat_sse(req: ChatRequest, user=Depends(get_current_user)):
    if not settings.has_llm_config:
        raise HTTPException(status_code=500, detail="LLM config missing (.env)")
    if not settings.has_mysql_config:
        raise HTTPException(status_code=500, detail="MySQL config missing (.env)")

    async def gen() -> AsyncGenerator[str, None]:
        request_id = str(uuid.uuid4())

        # Ensure conversation exists
        await upsert_conversation(req.conversation_id, owner_username=user["username"])
        await add_message(req.conversation_id, "user", req.message)

        # Build history (for SQL generation & analysis)
        history_rows = await get_messages(req.conversation_id, limit=20)
        history = [{"role": r["role"], "content": r["content"]} for r in history_rows if r["role"] in ("user","assistant")]

        yield sse_event("status", {"stage": "schema_retrieval", "request_id": request_id})
        schema_context = build_schema_context(req.message)

        yield sse_event("status", {"stage": "sql_generation", "request_id": request_id})
        sql = await generate_sql(req.message, schema_context, history)
        yield sse_event("sql", {"sql": sql, "request_id": request_id})

        # Execute (retry: regenerate SQL if SQL error)
        cols = []
        rows = []
        last_err: str | None = None
        for attempt in range(settings.MAX_SQL_RETRY + 1):
            try:
                yield sse_event("status", {"stage": "sql_execution", "attempt": attempt, "request_id": request_id})
                cols, rows = await run_sql(sql, max_rows=settings.MAX_ROWS)
                last_err = None
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
                sql = await generate_sql(fix_prompt, schema_context, history)
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
        analysis = await analyze(req.message, sql, cols, rows)
        yield sse_event("analysis", {"text": analysis, "request_id": request_id})

        await add_message(req.conversation_id, "assistant", f"[SQL]\n{sql}\n\n[Analysis]\n{analysis}")

        yield sse_event("done", {"ok": True, "request_id": request_id})

    return StreamingResponse(sse_stream(gen()), media_type="text/event-stream")
