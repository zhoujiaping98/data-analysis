from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from backend.app.api.deps import get_current_user
from backend.app.core.sqlite_store import list_sql_audits

router = APIRouter()


@router.get("/audits/sql")
async def get_sql_audits(
    limit: int = Query(default=200, ge=1, le=1000),
    user=Depends(get_current_user),
):
    return await list_sql_audits(user["username"], limit=limit)
