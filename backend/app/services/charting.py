from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
import numbers
import re


def _is_number(x: Any) -> bool:
    return isinstance(x, numbers.Real) and not isinstance(x, bool)


def _is_date_like(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if isinstance(value, str):
        if re.match(r"^\d{4}[-/]\d{2}[-/]\d{2}", value):
            return True
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
            return True
        except Exception:
            return False
    return False


def _summarize_columns(columns: List[str], rows: List[List[Any]]) -> List[Dict[str, Any]]:
    sample = rows[:500]
    summaries: List[Dict[str, Any]] = []
    for idx, col in enumerate(columns):
        non_null = 0
        num_count = 0
        date_count = 0
        uniques: set[Any] = set()
        for r in sample:
            if idx >= len(r):
                continue
            v = r[idx]
            if v is None:
                continue
            non_null += 1
            if _is_number(v):
                num_count += 1
            elif _is_date_like(v):
                date_count += 1
            if len(uniques) < 200:
                uniques.add(v)
        ratio = (lambda c: (c / non_null) if non_null else 0.0)
        summaries.append(
            {
                "index": idx,
                "name": col,
                "non_null": non_null,
                "numeric_ratio": ratio(num_count),
                "date_ratio": ratio(date_count),
                "unique_count": len(uniques),
                "unique_ratio": (len(uniques) / non_null) if non_null else 0.0,
            }
        )
    return summaries


def _ordered_values(rows: List[List[Any]], idx: int) -> List[Any]:
    seen = set()
    ordered = []
    for r in rows:
        if idx >= len(r):
            continue
        v = r[idx]
        if v not in seen:
            seen.add(v)
            ordered.append(v)
    return ordered



def suggest_echarts_option(columns: List[str], rows: List[List[Any]]) -> Optional[Dict[str, Any]]:
    """Heuristic chart suggestion. Returns ECharts option JSON or None."""
    if not columns or not rows or len(columns) < 2:
        return None

    sample = rows[:300]
    summaries = _summarize_columns(columns, sample)

    numeric_cols = [s for s in summaries if s["numeric_ratio"] >= 0.8]
    date_cols = [s for s in summaries if s["date_ratio"] >= 0.6]
    category_cols = [s for s in summaries if s["numeric_ratio"] < 0.6 and s["date_ratio"] < 0.6]

    if len(numeric_cols) >= 2 and not date_cols and not category_cols:
        x_idx = numeric_cols[0]["index"]
        y_idx = numeric_cols[1]["index"]
        data = []
        for r in sample:
            if _is_number(r[x_idx]) and _is_number(r[y_idx]):
                data.append([r[x_idx], r[y_idx]])
        if not data:
            return None
        return {
            "title": {"text": f"{columns[y_idx]} vs {columns[x_idx]}"},
            "tooltip": {"trigger": "item"},
            "xAxis": {"type": "value", "name": columns[x_idx]},
            "yAxis": {"type": "value", "name": columns[y_idx]},
            "series": [{"type": "scatter", "data": data}],
        }

    dim = None
    if date_cols:
        dim = date_cols[0]
    elif category_cols:
        category_cols = sorted(category_cols, key=lambda s: (s["unique_count"], -s["non_null"]))
        dim = category_cols[0]

    metrics = sorted(numeric_cols, key=lambda s: -s["non_null"])
    if not dim or not metrics:
        return None

    dim_idx = dim["index"]
    dim_name = columns[dim_idx]
    dim_values = _ordered_values(sample, dim_idx)

    if dim in date_cols:
        metric_cols = metrics[:3]
        series = []
        for m in metric_cols:
            data = []
            for r in sample:
                data.append(r[m["index"]])
            series.append({"type": "line", "data": data, "name": columns[m["index"]]})
        return {
            "title": {"text": f"{', '.join([columns[m['index']] for m in metric_cols])} over {dim_name}"},
            "tooltip": {"trigger": "axis"},
            "xAxis": {"type": "category", "data": dim_values},
            "yAxis": {"type": "value"},
            "series": series,
        }

    second_cat = None
    for c in category_cols:
        if c["index"] != dim_idx and c["unique_count"] <= 8:
            second_cat = c
            break

    if second_cat and len(metrics) >= 1 and dim["unique_count"] <= 30:
        metric = metrics[0]
        series_map: Dict[Any, Dict[Any, float]] = {}
        for r in sample:
            dim_val = r[dim_idx]
            series_key = r[second_cat["index"]]
            val = r[metric["index"]]
            if not _is_number(val):
                continue
            series_map.setdefault(series_key, {})
            series_map[series_key][dim_val] = float(val)
        series = []
        for s_name, bucket in series_map.items():
            series.append(
                {
                    "type": "bar",
                    "stack": dim_name,
                    "name": str(s_name),
                    "data": [bucket.get(v, 0) for v in dim_values],
                }
            )
        return {
            "title": {"text": f"{columns[metric['index']]} by {dim_name} & {columns[second_cat['index']]}"},
            "tooltip": {"trigger": "axis"},
            "legend": {"type": "scroll"},
            "xAxis": {"type": "category", "data": dim_values},
            "yAxis": {"type": "value"},
            "series": series,
        }

    if len(metrics) == 1:
        metric = metrics[0]
        data = []
        for r in sample:
            v = r[metric["index"]]
            if _is_number(v):
                data.append(v)
        if dim["unique_count"] <= 6:
            pie_data = []
            for i, v in enumerate(dim_values):
                idx = i
                val = data[idx] if idx < len(data) else 0
                pie_data.append({"name": str(v), "value": val})
            return {
                "title": {"text": f"{columns[metric['index']]} by {dim_name}"},
                "tooltip": {"trigger": "item"},
                "series": [{"type": "pie", "radius": ["35%", "65%"], "data": pie_data}],
            }
        return {
            "title": {"text": f"{columns[metric['index']]} by {dim_name}"},
            "tooltip": {"trigger": "axis"},
            "xAxis": {"type": "category", "data": dim_values},
            "yAxis": {"type": "value"},
            "series": [{"type": "bar", "data": data, "name": columns[metric["index"]]}],
        }

    metric_cols = metrics[:3]
    series = []
    for m in metric_cols:
        data = []
        for r in sample:
            data.append(r[m["index"]])
        series.append({"type": "bar", "data": data, "name": columns[m["index"]]})
    return {
        "title": {"text": f"{dim_name} comparison"},
        "tooltip": {"trigger": "axis"},
        "legend": {},
        "xAxis": {"type": "category", "data": dim_values},
        "yAxis": {"type": "value"},
        "series": series,
    }
