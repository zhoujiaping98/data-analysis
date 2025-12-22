from __future__ import annotations

import io
import re
from urllib.parse import quote
from typing import Any, Dict, List

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from backend.app.api.deps import get_current_user

router = APIRouter()


@router.post("/export/xlsx")
async def export_xlsx(payload: Dict[str, Any], user=Depends(get_current_user)):
    columns = payload.get("columns")
    rows = payload.get("rows")
    filename = payload.get("filename") or "result.xlsx"
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise HTTPException(status_code=400, detail="Invalid payload")
    if len(columns) == 0:
        raise HTTPException(status_code=400, detail="No columns to export")
    try:
        df = pd.DataFrame(rows, columns=columns)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid data: {e}") from e

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    buf.seek(0)

    if not filename.lower().endswith(".xlsx"):
        filename += ".xlsx"
    ascii_fallback = re.sub(r"[^\x20-\x7E]", "_", filename)
    if not ascii_fallback or ascii_fallback == "____":
        ascii_fallback = "result.xlsx"
    headers = {
        "Content-Disposition": (
            f'attachment; filename="{ascii_fallback}"; '
            f"filename*=UTF-8''{quote(filename)}"
        )
    }
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )
