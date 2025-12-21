from __future__ import annotations

from typing import List

from backend.app.core.config import settings
from backend.app.core.mysql import drop_table
from backend.app.core.sqlite_store import delete_file_uploads, list_expired_file_uploads, get_datasource
import json


async def cleanup_expired_uploads(ttl_hours: int | None = None) -> int:
    ttl_hours = settings.FILE_UPLOAD_TTL_HOURS if ttl_hours is None else ttl_hours
    if ttl_hours <= 0:
        return 0

    expired = await list_expired_file_uploads(ttl_hours)
    if not expired:
        return 0

    for meta in expired:
        try:
            ds_id = meta.get("datasource_id") or "default"
            ds = await get_datasource(ds_id)
            if not ds:
                continue
            cfg = json.loads(ds["config_json"])
            await drop_table(meta["table_name"], cfg, ds_id)
        except Exception:
            pass

    await delete_file_uploads([m["id"] for m in expired])
    return len(expired)
