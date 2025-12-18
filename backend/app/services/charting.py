from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import re

def _is_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)

def _is_date_like(s: Any) -> bool:
    if isinstance(s, (datetime,)):
        return True
    if isinstance(s, str):
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}", s))
    return False

def suggest_echarts_option(columns: List[str], rows: List[List[Any]]) -> Optional[Dict[str, Any]]:
    """Heuristic chart suggestion. Returns ECharts option JSON or None."""
    if not columns or not rows:
        return None
    if len(columns) < 2:
        return None

    # Use first 200 rows for chart
    sample = rows[:200]
    # Find a dimension column and a metric column
    dim_idx = None
    val_idx = None

    # Prefer date dimension
    for i, col in enumerate(columns):
        if _is_date_like(sample[0][i]):
            dim_idx = i
            break
    # If no date, pick first non-numeric
    if dim_idx is None:
        for i, col in enumerate(columns):
            if not _is_number(sample[0][i]):
                dim_idx = i
                break
    # metric: first numeric
    for i, col in enumerate(columns):
        if _is_number(sample[0][i]):
            val_idx = i
            break

    if dim_idx is None or val_idx is None or dim_idx == val_idx:
        return None

    x = [r[dim_idx] for r in sample]
    y = [r[val_idx] for r in sample]

    is_time = _is_date_like(sample[0][dim_idx])
    series_type = "line" if is_time else "bar"

    return {
        "title": {"text": f"{columns[val_idx]} by {columns[dim_idx]}"},
        "tooltip": {"trigger": "axis"},
        "xAxis": {"type": "category", "data": x},
        "yAxis": {"type": "value"},
        "series": [{"type": series_type, "data": y, "name": columns[val_idx]}],
    }
