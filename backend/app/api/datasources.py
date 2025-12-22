from __future__ import annotations

import json
import uuid
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException

from backend.app.api.deps import get_current_user
from backend.app.core.datasources import ensure_default_datasource
from backend.app.core.mysql import ping, fetch_schema_documents
from backend.app.core.sqlite_store import (
    add_datasource,
    list_datasources,
    get_datasource,
    set_default_datasource,
    update_datasource_training,
    list_qa_pairs,
)
from backend.app.core.training import get_store
from backend.app.core.qa_docs import build_qa_doc

router = APIRouter()


@router.get("/datasources")
async def list_ds(user=Depends(get_current_user)):
    await ensure_default_datasource()
    rows = await list_datasources()
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "type": r["type"],
            "is_default": bool(r["is_default"]),
            "training_ok": r.get("training_ok"),
            "training_error": r.get("training_error"),
            "last_trained_at": r.get("last_trained_at"),
        }
        for r in rows
    ]


@router.post("/datasources")
async def create_ds(payload: Dict[str, Any], user=Depends(get_current_user)):
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Missing datasource name")
    ds_type = (payload.get("type") or "mysql").strip().lower()
    if ds_type != "mysql":
        raise HTTPException(status_code=400, detail="Only mysql is supported for now")
    config = {
        "host": payload.get("host", ""),
        "port": int(payload.get("port") or 3306),
        "database": payload.get("database", ""),
        "user": payload.get("user", ""),
        "password": payload.get("password", ""),
    }
    if not all([config["host"], config["database"], config["user"], config["password"]]):
        raise HTTPException(status_code=400, detail="Missing mysql config fields")
    ds_id = str(uuid.uuid4())
    is_default = bool(payload.get("is_default"))
    await add_datasource(
        ds_id=ds_id,
        name=name,
        ds_type=ds_type,
        config_json=json.dumps(config),
        is_default=is_default,
    )
    training_ok = True
    training_error = None
    try:
        docs = await fetch_schema_documents(config, ds_id)
        if docs:
            store = get_store(ds_id)
            store.upsert_schema_docs(docs)
            qa_rows = await list_qa_pairs(ds_id)
            qa_docs = []
            for r in qa_rows:
                if not r.get("enabled"):
                    continue
                try:
                    tables = json.loads(r.get("tables_json") or "[]")
                except Exception:
                    tables = []
                try:
                    tags = json.loads(r.get("tags_json") or "[]")
                except Exception:
                    tags = []
                qa_docs.append(
                    build_qa_doc(
                        r["id"],
                        ds_id,
                        r.get("question") or "",
                        r.get("sql") or "",
                        r.get("note"),
                        tables,
                        tags,
                    )
                )
            if qa_docs:
                store.upsert_qa_docs(qa_docs)
        else:
            training_ok = False
            training_error = "No schema docs fetched"
    except Exception as e:
        training_ok = False
        training_error = str(e)
    await update_datasource_training(ds_id, training_ok, training_error)
    return {"id": ds_id, "training_ok": training_ok, "training_error": training_error}


@router.post("/datasources/{ds_id}/test")
async def test_ds(ds_id: str, user=Depends(get_current_user)):
    ds = await get_datasource(ds_id)
    if ds is None:
        raise HTTPException(status_code=404, detail="Datasource not found")
    config = json.loads(ds["config_json"])
    ok = await ping(config, ds_id)
    if not ok:
        raise HTTPException(status_code=400, detail="Connection failed")
    return {"ok": True}


@router.put("/datasources/{ds_id}/default")
async def set_default(ds_id: str, user=Depends(get_current_user)):
    ds = await get_datasource(ds_id)
    if ds is None:
        raise HTTPException(status_code=404, detail="Datasource not found")
    await set_default_datasource(ds_id)
    return {"ok": True}


@router.post("/datasources/{ds_id}/train")
async def train_ds(ds_id: str, user=Depends(get_current_user)):
    ds = await get_datasource(ds_id)
    if ds is None:
        raise HTTPException(status_code=404, detail="Datasource not found")
    config = json.loads(ds["config_json"])
    training_ok = True
    training_error = None
    try:
        docs = await fetch_schema_documents(config, ds_id)
        if docs:
            store = get_store(ds_id)
            store.reset()
            store.upsert_schema_docs(docs)
            qa_rows = await list_qa_pairs(ds_id)
            qa_docs = []
            for r in qa_rows:
                if not r.get("enabled"):
                    continue
                try:
                    tables = json.loads(r.get("tables_json") or "[]")
                except Exception:
                    tables = []
                try:
                    tags = json.loads(r.get("tags_json") or "[]")
                except Exception:
                    tags = []
                qa_docs.append(
                    build_qa_doc(
                        r["id"],
                        ds_id,
                        r.get("question") or "",
                        r.get("sql") or "",
                        r.get("note"),
                        tables,
                        tags,
                    )
                )
            if qa_docs:
                store.upsert_qa_docs(qa_docs)
        else:
            training_ok = False
            training_error = "No schema docs fetched"
    except Exception as e:
        training_ok = False
        training_error = str(e)
    await update_datasource_training(ds_id, training_ok, training_error)
    return {"ok": training_ok, "error": training_error}
