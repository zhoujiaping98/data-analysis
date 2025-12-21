from __future__ import annotations

import asyncio
import io
import json
import re
import uuid
from typing import Dict, List

import pandas as pd
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, Header

from backend.app.api.deps import get_current_user
from backend.app.core.config import settings
from backend.app.core.mysql import drop_table, fetch_schema_documents_for_table, import_dataframe
from backend.app.core.training import get_store
from backend.app.core.uploads import cleanup_expired_uploads
from backend.app.core.datasources import resolve_datasource
from backend.app.core.sqlite_store import (
    add_file_upload,
    delete_file_upload,
    get_file_upload,
    list_file_uploads,
)

router = APIRouter()

_SAFE_COL_RE = re.compile(r"[^A-Za-z0-9_]+")


def _normalize_columns(columns: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    used: set[str] = set()
    for idx, col in enumerate(columns):
        base = _SAFE_COL_RE.sub("_", (col or "").strip()).strip("_").lower()
        if not base:
            base = f"col_{idx+1}"
        if base[0].isdigit():
            base = f"col_{base}"
        name = base
        suffix = 1
        while name in used:
            suffix += 1
            name = f"{base}_{suffix}"
        used.add(name)
        out[col] = name
    return out


def _read_sheet_names(filename: str, data: bytes) -> List[str]:
    if filename.lower().endswith(".csv"):
        return ["(csv)"]
    if filename.lower().endswith(".xlsx") or filename.lower().endswith(".xls"):
        try:
            excel = pd.ExcelFile(io.BytesIO(data))
        except ImportError as e:
            msg = str(e)
            if "xlrd" in msg.lower():
                raise HTTPException(status_code=400, detail="Parsing .xls requires xlrd")
            raise HTTPException(status_code=400, detail=f"Missing Excel dependency: {e}") from e
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid Excel file: {e}") from e
        return excel.sheet_names
    raise HTTPException(status_code=400, detail="Unsupported file type. Use .xlsx/.xls/.csv")


def _read_dataframe(filename: str, data: bytes, sheet_name: str | None) -> pd.DataFrame:
    if filename.lower().endswith(".csv"):
        return pd.read_csv(io.BytesIO(data))
    if filename.lower().endswith(".xlsx") or filename.lower().endswith(".xls"):
        if sheet_name:
            sheets = _read_sheet_names(filename, data)
            if sheet_name not in sheets:
                raise HTTPException(status_code=400, detail="Sheet not found in the file")
        try:
            return pd.read_excel(io.BytesIO(data), sheet_name=sheet_name or 0)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to read Excel sheet: {e}") from e
    raise HTTPException(status_code=400, detail="Unsupported file type. Use .xlsx/.xls/.csv")


@router.post("/files/upload")
async def upload_file(
    file: UploadFile = File(...),
    sheet_name: str | None = Form(default=None),
    user=Depends(get_current_user),
    x_datasource_id: str | None = Header(default=None),
):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    await cleanup_expired_uploads()
    ds_id, ds_cfg = await resolve_datasource(x_datasource_id)

    raw = await file.read()
    if len(raw) > settings.FILE_UPLOAD_MAX_BYTES:
        raise HTTPException(status_code=400, detail="File too large")

    effective_sheet = sheet_name
    if file.filename.lower().endswith((".xlsx", ".xls")) and not sheet_name:
        sheets = _read_sheet_names(file.filename, raw)
        effective_sheet = sheets[0] if sheets else None

    df = _read_dataframe(file.filename, raw, effective_sheet)
    if df.empty:
        raise HTTPException(status_code=400, detail="所选 Sheet 没有数据（只有表头）")
    if df.shape[0] > settings.FILE_UPLOAD_MAX_ROWS:
        raise HTTPException(status_code=400, detail="Too many rows")
    if df.shape[1] > settings.FILE_UPLOAD_MAX_COLS:
        raise HTTPException(status_code=400, detail="Too many columns")

    col_map = _normalize_columns([str(c) for c in df.columns.tolist()])
    df = df.rename(columns=col_map)

    file_id = str(uuid.uuid4())
    table_name = f"tmp_{file_id.replace('-', '')}"

    # pandas to_sql is sync, run in a worker thread
    try:
        await asyncio.to_thread(import_dataframe, table_name, df, ds_cfg, ds_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Import failed: {e}") from e

    await add_file_upload(
        file_id=file_id,
        owner_username=user["username"],
        datasource_id=ds_id,
        filename=file.filename,
        sheet_name=effective_sheet,
        table_name=table_name,
        row_count=int(df.shape[0]),
        columns_json=json.dumps(col_map, ensure_ascii=False),
    )

    # Update vector store with the new table schema for SQL generation
    try:
        docs = await fetch_schema_documents_for_table(table_name, ds_cfg, ds_id)
        if docs:
            get_store(ds_id).upsert_schema_docs(docs)
    except Exception:
        pass

    return {
        "file_id": file_id,
        "table_name": table_name,
        "row_count": int(df.shape[0]),
        "columns": list(df.columns),
        "columns_map": col_map,
    }


@router.get("/files")
async def list_files(user=Depends(get_current_user), x_datasource_id: str | None = Header(default=None)):
    await cleanup_expired_uploads()
    ds_id, _ = await resolve_datasource(x_datasource_id)
    return await list_file_uploads(user["username"], ds_id)


@router.delete("/files/{file_id}")
async def delete_file(file_id: str, user=Depends(get_current_user), x_datasource_id: str | None = Header(default=None)):
    meta = await get_file_upload(file_id)
    if meta is None:
        raise HTTPException(status_code=404, detail="File not found")
    if meta["owner_username"] != user["username"]:
        raise HTTPException(status_code=403, detail="Forbidden")

    # drop table
    try:
        ds_id, ds_cfg = await resolve_datasource(x_datasource_id or meta.get("datasource_id"))
        await drop_table(meta["table_name"], ds_cfg, ds_id)
    except Exception:
        pass

    await delete_file_upload(file_id)
    return {"ok": True}


@router.post("/files/sheets")
async def list_sheets(file: UploadFile = File(...), user=Depends(get_current_user)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    raw = await file.read()
    if len(raw) > settings.FILE_UPLOAD_MAX_BYTES:
        raise HTTPException(status_code=400, detail="File too large")

    sheets = _read_sheet_names(file.filename, raw)
    return {"sheets": sheets}
