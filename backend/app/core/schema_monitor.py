from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple

from backend.app.core.config import settings
from backend.app.core.sqlite_store import (
    get_datasource,
    list_datasources,
    get_schema_snapshot,
    set_schema_snapshot,
    add_schema_change_log,
)
from backend.app.core.datasources import resolve_datasource
from backend.app.core.mysql import list_tables, fetch_schema_documents
from backend.app.core.training import get_store
from backend.app.core.mysql import fetch_schema_documents_for_table


def _now() -> datetime:
    return datetime.utcnow()


def _serialize_schema(
    tables: List[Dict[str, Any]],
    columns_by_table: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for t in tables:
        name = t.get("name")
        if not name:
            continue
        out[name] = {
            "comment": t.get("comment") or "",
            "type": t.get("type") or "",
            "columns": columns_by_table.get(name, []),
        }
    return out


def _diff_schema(old: Dict[str, Any], new: Dict[str, Any]) -> Tuple[List[str], List[str], List[str]]:
    old_set = set(old.keys())
    new_set = set(new.keys())
    added = sorted(list(new_set - old_set))
    removed = sorted(list(old_set - new_set))
    changed: List[str] = []
    for k in old_set & new_set:
        if old.get(k) != new.get(k):
            changed.append(k)
    return added, removed, sorted(changed)


def _extract_changed_tables(added: List[str], removed: List[str], changed: List[str]) -> List[str]:
    targets = set(added + changed)
    for t in removed:
        targets.discard(t)
    return sorted(targets)


async def _check_one_datasource(ds_id: str) -> None:
    ds = await get_datasource(ds_id)
    if not ds or (ds.get("type") or "") != "mysql":
        return
    last = await get_schema_snapshot(ds_id)
    if last:
        try:
            last_at = datetime.fromisoformat(last["checked_at"])
            if settings.SCHEMA_CHECK_INTERVAL_HOURS > 0:
                if _now() - last_at < timedelta(hours=settings.SCHEMA_CHECK_INTERVAL_HOURS):
                    return
        except Exception:
            pass

    _, cfg = await resolve_datasource(ds_id)
    if not all([cfg.get("host"), cfg.get("database"), cfg.get("user"), cfg.get("password")]):
        return
    tables = await list_tables(cfg, ds_id)
    docs = await fetch_schema_documents(cfg, ds_id)
    cols_by_table: Dict[str, List[Dict[str, Any]]] = {}
    for d in docs or []:
        table = (d.get("metadata") or {}).get("table")
        if not table:
            continue
        cols_by_table.setdefault(table, []).append({"text": d.get("text", "")})
    current = _serialize_schema(tables, cols_by_table)
    current_json = json.dumps(current, ensure_ascii=False)

    if last:
        prev = json.loads(last["schema_json"] or "{}")
        added, removed, changed = _diff_schema(prev, current)
        if added or removed or changed:
            await add_schema_change_log(ds_id, added, removed, changed)
            # partial retrain: only upsert changed/added tables
            targets = _extract_changed_tables(added, removed, changed)
            for t in targets:
                try:
                    docs = await fetch_schema_documents_for_table(t, cfg, ds_id)
                    if docs:
                        get_store(ds_id).upsert_schema_docs(docs)
                except Exception:
                    continue
            if removed:
                try:
                    get_store(ds_id).delete_schema_docs(removed)
                except Exception:
                    pass

    await set_schema_snapshot(ds_id, current_json)


async def run_schema_check() -> None:
    if not settings.has_mysql_config:
        return
    datasources = await list_datasources()
    if not datasources:
        return
    for ds in datasources:
        try:
            await _check_one_datasource(ds["id"])
        except Exception:
            continue
