from __future__ import annotations

from datetime import datetime
import orjson
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Header

from backend.app.api.deps import get_current_user
from backend.app.schemas.sql import SqlExecuteRequest
from backend.app.core.config import settings
from backend.app.core.sqlite_store import (
    add_message_artifact,
    get_conversation,
    get_message_by_id,
    list_file_uploads,
)
from backend.app.core.mysql import run_sql, extract_table_names, list_tables
from backend.app.core.datasources import resolve_datasource
from backend.app.services.charting import suggest_echarts_option
from backend.app.services.analyzer import analyze_stream

router = APIRouter()


def _json_default(obj: Any):
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    return str(obj)


@router.post("/sql/execute")
async def execute_sql(
    req: SqlExecuteRequest,
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    sql = (req.sql or "").strip()
    if not sql:
        raise HTTPException(status_code=400, detail="SQL is required")

    conv = await get_conversation(req.conversation_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")
    if conv.get("owner_username") != user["username"]:
        raise HTTPException(status_code=403, detail="Forbidden")

    msg = await get_message_by_id(req.message_id)
    if not msg or msg.get("conversation_id") != req.conversation_id:
        raise HTTPException(status_code=400, detail="Message not found in conversation")
    if msg.get("role") != "user":
        raise HTTPException(status_code=400, detail="Only user messages can be re-run")

    ds_id, ds_cfg = await resolve_datasource(x_datasource_id)
    if not all([ds_cfg.get("host"), ds_cfg.get("database"), ds_cfg.get("user"), ds_cfg.get("password")]):
        raise HTTPException(status_code=500, detail="MySQL datasource config missing")

    # Enforce table allowlist (base tables + user uploads)
    base_tables = await list_tables(ds_cfg, ds_id)
    uploads = await list_file_uploads(user["username"], ds_id)
    upload_names = {u["table_name"] for u in uploads}
    allowed_tables = {t["name"] for t in base_tables if not t["name"].startswith("tmp_")} | upload_names
    used_tables = set(extract_table_names(sql))
    if used_tables and not used_tables.issubset(allowed_tables):
        raise HTTPException(
            status_code=400,
            detail="SQL references tables not allowed for this user.",
        )

    try:
        cols, rows = await run_sql(sql, max_rows=settings.MAX_ROWS, config=ds_cfg, cache_key=ds_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    option = suggest_echarts_option(cols, rows)

    analysis = ""
    if req.with_analysis:
        if not settings.has_llm_config:
            raise HTTPException(status_code=500, detail="LLM config missing (.env)")
        analysis_parts: list[str] = []
        async for chunk in analyze_stream(msg.get("content") or "", sql, cols, rows):
            analysis_parts.append(chunk)
        analysis = "".join(analysis_parts).strip()

    try:
        await add_message_artifact(
            conv_id=req.conversation_id,
            user_message_id=req.message_id,
            sql_text=sql,
            columns_json=orjson.dumps(cols, default=_json_default).decode("utf-8"),
            rows_json=orjson.dumps(rows, default=_json_default).decode("utf-8"),
            chart_json=orjson.dumps(option, default=_json_default).decode("utf-8") if option else None,
            analysis_text=analysis,
        )
    except Exception:
        pass

    return {
        "sql": sql,
        "columns": cols,
        "rows": rows,
        "chart": option,
        "analysis": analysis,
    }
