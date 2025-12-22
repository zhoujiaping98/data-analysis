from __future__ import annotations

import uuid
import json
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Header

from backend.app.api.deps import get_current_user
from backend.app.core.datasources import resolve_datasource
from backend.app.core.mysql import validate_readonly_sql
from backend.app.core.sqlite_store import (
    list_qa_pairs,
    add_qa_pair,
    update_qa_pair,
    delete_qa_pair,
    get_qa_pair,
)
from backend.app.core.training import get_store
from backend.app.core.qa_docs import build_qa_doc

router = APIRouter()


def _build_qa_doc(qa_id: str, ds_id: str, question: str, sql: str, note: str | None, tables: list[str], tags: list[str]) -> Dict[str, Any]:
    return build_qa_doc(qa_id, ds_id, question, sql, note, tables, tags)


@router.get("/qa")
async def list_qas(
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    ds_id, _ = await resolve_datasource(x_datasource_id)
    rows = await list_qa_pairs(ds_id)
    out = []
    for r in rows:
        try:
            tables = json.loads(r.get("tables_json") or "[]")
        except Exception:
            tables = []
        try:
            tags = json.loads(r.get("tags_json") or "[]")
        except Exception:
            tags = []
        out.append(
            {
                "id": r.get("id"),
                "question": r.get("question"),
                "sql": r.get("sql"),
                "note": r.get("note"),
                "tables": tables,
                "tags": tags,
                "enabled": bool(r.get("enabled")),
                "created_at": r.get("created_at"),
            }
        )
    return out


@router.post("/qa")
async def create_qa(
    payload: Dict[str, Any],
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    question = (payload.get("question") or "").strip()
    sql = (payload.get("sql") or "").strip()
    note = (payload.get("note") or "").strip() or None
    tables = payload.get("tables") or []
    tags = payload.get("tags") or []
    enabled = bool(payload.get("enabled", True))
    if not question or not sql:
        raise HTTPException(status_code=400, detail="Question and SQL are required")
    try:
        validate_readonly_sql(sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if not isinstance(tables, list):
        raise HTTPException(status_code=400, detail="Tables must be a list")
    if not isinstance(tags, list):
        raise HTTPException(status_code=400, detail="Tags must be a list")

    ds_id, _ = await resolve_datasource(x_datasource_id)
    qa_id = str(uuid.uuid4())
    await add_qa_pair(
        qa_id,
        ds_id,
        question,
        sql,
        note,
        json.dumps(tables, ensure_ascii=False),
        json.dumps(tags, ensure_ascii=False),
        enabled,
    )
    if enabled:
        store = get_store(ds_id)
        store.upsert_qa_docs([_build_qa_doc(qa_id, ds_id, question, sql, note, tables, tags)])
    return {"id": qa_id, "question": question, "sql": sql, "tables": tables, "tags": tags, "enabled": enabled}


@router.put("/qa/{qa_id}")
async def update_qa(
    qa_id: str,
    payload: Dict[str, Any],
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    row = await get_qa_pair(qa_id)
    if not row:
        raise HTTPException(status_code=404, detail="QA not found")
    ds_id, _ = await resolve_datasource(x_datasource_id)
    if row.get("datasource_id") != ds_id:
        raise HTTPException(status_code=403, detail="QA not in current datasource")
    question = (payload.get("question") or row.get("question") or "").strip()
    sql = (payload.get("sql") or row.get("sql") or "").strip()
    note = (payload.get("note") or "").strip() or None
    tables = payload.get("tables")
    tags = payload.get("tags")
    enabled = bool(payload.get("enabled", row.get("enabled")))
    if not question or not sql:
        raise HTTPException(status_code=400, detail="Question and SQL are required")
    try:
        validate_readonly_sql(sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    if tables is None:
        try:
            tables = json.loads(row.get("tables_json") or "[]")
        except Exception:
            tables = []
    if tags is None:
        try:
            tags = json.loads(row.get("tags_json") or "[]")
        except Exception:
            tags = []
    if not isinstance(tables, list) or not isinstance(tags, list):
        raise HTTPException(status_code=400, detail="Tables/Tags must be lists")

    await update_qa_pair(
        qa_id,
        question,
        sql,
        note,
        json.dumps(tables, ensure_ascii=False),
        json.dumps(tags, ensure_ascii=False),
        enabled,
    )
    store = get_store(ds_id)
    store.delete_qa_docs([qa_id])
    if enabled:
        store.upsert_qa_docs([_build_qa_doc(qa_id, ds_id, question, sql, note, tables, tags)])
    return {"ok": True}


@router.delete("/qa/{qa_id}")
async def remove_qa(
    qa_id: str,
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    ds_id, _ = await resolve_datasource(x_datasource_id)
    row = await get_qa_pair(qa_id)
    if not row:
        return {"ok": True}
    if row.get("datasource_id") != ds_id:
        raise HTTPException(status_code=403, detail="QA not in current datasource")
    await delete_qa_pair(qa_id)
    store = get_store(ds_id)
    store.delete_qa_docs([qa_id])
    return {"ok": True}


@router.post("/qa/bulk")
async def bulk_create_qa(
    payload: Dict[str, Any],
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="Items is required")
    if len(items) > 500:
        raise HTTPException(status_code=400, detail="Too many items")

    ds_id, _ = await resolve_datasource(x_datasource_id)
    store = get_store(ds_id)
    created = 0
    errors = []
    qa_docs = []
    for idx, item in enumerate(items):
        question = (item.get("question") or "").strip()
        sql = (item.get("sql") or "").strip()
        note = (item.get("note") or "").strip() or None
        tables = item.get("tables") or []
        tags = item.get("tags") or []
        enabled = bool(item.get("enabled", True))
        if not question or not sql:
            errors.append({"index": idx, "error": "Question and SQL are required"})
            continue
        try:
            validate_readonly_sql(sql)
        except ValueError as e:
            errors.append({"index": idx, "error": str(e)})
            continue
        if not isinstance(tables, list) or not isinstance(tags, list):
            errors.append({"index": idx, "error": "Tables/Tags must be lists"})
            continue
        qa_id = str(uuid.uuid4())
        await add_qa_pair(
            qa_id,
            ds_id,
            question,
            sql,
            note,
            json.dumps(tables, ensure_ascii=False),
            json.dumps(tags, ensure_ascii=False),
            enabled,
        )
        if enabled:
            qa_docs.append(build_qa_doc(qa_id, ds_id, question, sql, note, tables, tags))
        created += 1
    if qa_docs:
        store.upsert_qa_docs(qa_docs)
    return {"created": created, "errors": errors}


@router.post("/qa/batch")
async def batch_update_qa(
    payload: Dict[str, Any],
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    action = (payload.get("action") or "").strip().lower()
    ids = payload.get("ids") or []
    if action not in {"enable", "disable", "delete"}:
        raise HTTPException(status_code=400, detail="Invalid action")
    if not isinstance(ids, list) or not ids:
        raise HTTPException(status_code=400, detail="Ids is required")

    ds_id, _ = await resolve_datasource(x_datasource_id)
    store = get_store(ds_id)
    updated = 0
    for qa_id in ids:
        row = await get_qa_pair(qa_id)
        if not row:
            continue
        if row.get("datasource_id") != ds_id:
            continue
        if action == "delete":
            await delete_qa_pair(qa_id)
            store.delete_qa_docs([qa_id])
            updated += 1
            continue

        enabled = action == "enable"
        try:
            tables = json.loads(row.get("tables_json") or "[]")
        except Exception:
            tables = []
        try:
            tags = json.loads(row.get("tags_json") or "[]")
        except Exception:
            tags = []
        await update_qa_pair(
            qa_id,
            row.get("question") or "",
            row.get("sql") or "",
            row.get("note"),
            json.dumps(tables, ensure_ascii=False),
            json.dumps(tags, ensure_ascii=False),
            enabled,
        )
        store.delete_qa_docs([qa_id])
        if enabled:
            store.upsert_qa_docs([build_qa_doc(qa_id, ds_id, row.get("question") or "", row.get("sql") or "", row.get("note"), tables, tags)])
        updated += 1
    return {"updated": updated}
