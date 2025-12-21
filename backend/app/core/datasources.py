from __future__ import annotations

import json
from typing import Any, Dict, Tuple

from backend.app.core.config import settings
from backend.app.core.sqlite_store import (
    add_datasource,
    get_datasource,
    get_default_datasource,
)


def _default_mysql_config() -> Dict[str, Any]:
    return {
        "host": settings.MYSQL_HOST,
        "port": settings.MYSQL_PORT,
        "database": settings.MYSQL_DATABASE,
        "user": settings.MYSQL_USER,
        "password": settings.MYSQL_PASSWORD,
    }


async def ensure_default_datasource() -> None:
    if not settings.has_mysql_config:
        return
    existing = await get_default_datasource()
    if existing:
        return
    await add_datasource(
        ds_id="default",
        name="Default MySQL",
        ds_type="mysql",
        config_json=json.dumps(_default_mysql_config()),
        is_default=True,
    )


async def resolve_datasource(ds_id: str | None) -> Tuple[str, Dict[str, Any]]:
    ds_id = ds_id or "default"
    ds = await get_datasource(ds_id)
    if ds is None:
        ds = await get_default_datasource()
    if ds is None:
        raise RuntimeError("No datasource configured")
    return ds["id"], json.loads(ds["config_json"])
