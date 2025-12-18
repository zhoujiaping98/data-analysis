from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.api.deps import get_current_user
from backend.app.core.mysql import list_tables, preview_table

router = APIRouter()


@router.get("/schema/tables")
async def schema_tables(user=Depends(get_current_user)):
    return await list_tables()


@router.get("/schema/tables/{table_name}/preview")
async def schema_table_preview(
    table_name: str,
    limit: int = Query(default=10, ge=1, le=100),
    user=Depends(get_current_user),
):
    try:
        cols, rows = await preview_table(table_name, limit=limit)
        return {"table": table_name, "columns": cols, "rows": rows, "row_count": len(rows)}
    except ValueError as e:
        msg = str(e)
        if msg.lower() == "table not found":
            raise HTTPException(status_code=404, detail=msg) from e
        raise HTTPException(status_code=400, detail=msg) from e
