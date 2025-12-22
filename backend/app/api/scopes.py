from __future__ import annotations

import uuid
import json
from typing import List, Dict

from fastapi import APIRouter, Depends, HTTPException, Header

from backend.app.api.deps import get_current_user
from backend.app.core.sqlite_store import list_table_scopes, add_table_scope, delete_table_scope
from backend.app.core.datasources import resolve_datasource

router = APIRouter()


@router.get("/scopes")
async def list_scopes(
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    ds_id, _ = await resolve_datasource(x_datasource_id)
    scopes = await list_table_scopes(user["username"], ds_id)
    out: List[Dict] = []
    for s in scopes:
        try:
            tables = json.loads(s.get("tables_json") or "[]")
        except Exception:
            tables = []
        out.append(
            {
                "id": s.get("id"),
                "name": s.get("name"),
                "tables": tables,
                "created_at": s.get("created_at"),
            }
        )
    return out


@router.post("/scopes")
async def create_scope(
    payload: Dict,
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    name = (payload.get("name") or "").strip()
    tables = payload.get("tables") or []
    if not name:
        raise HTTPException(status_code=400, detail="Scope name is required")
    if not isinstance(tables, list) or not tables:
        raise HTTPException(status_code=400, detail="Tables list is required")
    if len(name) > 40:
        raise HTTPException(status_code=400, detail="Scope name too long")
    ds_id, _ = await resolve_datasource(x_datasource_id)
    scope_id = str(uuid.uuid4())
    await add_table_scope(scope_id, user["username"], ds_id, name, json.dumps(tables, ensure_ascii=False))
    return {"id": scope_id, "name": name, "tables": tables}


@router.delete("/scopes/{scope_id}")
async def remove_scope(
    scope_id: str,
    user=Depends(get_current_user),
):
    await delete_table_scope(scope_id, user["username"])
    return {"ok": True}
