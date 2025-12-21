from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from fastapi import Header
from backend.app.api.deps import get_current_user
from backend.app.core.mysql import list_tables, preview_table
from backend.app.core.sqlite_store import list_file_uploads
from backend.app.core.resilience import CircuitOpenError
from backend.app.core.uploads import cleanup_expired_uploads
from backend.app.core.datasources import resolve_datasource

router = APIRouter()


@router.get("/schema/tables")
async def schema_tables(
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    try:
        await cleanup_expired_uploads()
        ds_id, ds_cfg = await resolve_datasource(x_datasource_id)
        base = await list_tables(ds_cfg, ds_id)
        uploads = await list_file_uploads(user["username"], ds_id)
        upload_map = {u["table_name"]: u for u in uploads}
        out = []
        for t in base:
            name = t["name"]
            if name.startswith("tmp_") and name not in upload_map:
                continue
            if name in upload_map:
                meta = upload_map[name]
                extra = meta.get("sheet_name") or ""
                suffix = f" / {extra}" if extra else ""
                t = {
                    "name": name,
                    "type": "UPLOAD",
                    "comment": f"{meta['filename']}{suffix}",
                }
            out.append(t)
        return out
    except CircuitOpenError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e


@router.get("/schema/tables/{table_name}/preview")
async def schema_table_preview(
    table_name: str,
    limit: int = Query(default=10, ge=1, le=100),
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    try:
        await cleanup_expired_uploads()
        ds_id, ds_cfg = await resolve_datasource(x_datasource_id)
        cols, rows = await preview_table(table_name, limit=limit, config=ds_cfg, cache_key=ds_id)
        return {"table": table_name, "columns": cols, "rows": rows, "row_count": len(rows)}
    except CircuitOpenError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e
    except ValueError as e:
        msg = str(e)
        if msg.lower() == "table not found":
            raise HTTPException(status_code=404, detail=msg) from e
        raise HTTPException(status_code=400, detail=msg) from e
