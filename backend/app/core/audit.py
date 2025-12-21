from __future__ import annotations

from typing import Any, Dict, List, Tuple

from backend.app.core.config import settings


def mask_sensitive_value(value: Any, keep_start: int, keep_end: int) -> Any:
    if value is None:
        return value
    s = str(value)
    if len(s) <= keep_start + keep_end:
        return "*" * len(s)
    return f"{s[:keep_start]}{'*' * (len(s) - keep_start - keep_end)}{s[-keep_end:]}"


def mask_sensitive_rows(
    columns: List[str],
    rows: List[List[Any]],
    *,
    keywords: List[str] | None = None,
    keep_start: int = 2,
    keep_end: int = 2,
) -> Tuple[List[str], List[List[Any]]]:
    if not columns or not rows:
        return columns, rows
    keywords = keywords or settings.sensitive_field_keywords
    if not keywords:
        return columns, rows
    indices: List[int] = []
    for idx, col in enumerate(columns):
        name = (col or "").lower()
        if any(k in name for k in keywords):
            indices.append(idx)
    if not indices:
        return columns, rows
    masked = []
    for r in rows:
        row = list(r)
        for idx in indices:
            if idx < len(row):
                row[idx] = mask_sensitive_value(row[idx], keep_start, keep_end)
        masked.append(row)
    return columns, masked
