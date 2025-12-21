from __future__ import annotations

import re
from typing import List

from backend.app.core.llm import get_chat_client
from backend.app.core.config import settings


def generate_safety_tips(sql: str, row_count: int | None, elapsed_ms: int | None) -> List[str]:
    tips: List[str] = []
    s = (sql or "").lower()
    if "select *" in s:
        tips.append("\u907f\u514d\u4f7f\u7528 SELECT *\uff0c\u5efa\u8bae\u53ea\u9009\u62e9\u5fc5\u8981\u5b57\u6bb5\u3002")
    if " where " not in f" {s} ":
        tips.append("\u672a\u53d1\u73b0 WHERE \u6761\u4ef6\uff0c\u5efa\u8bae\u52a0\u8fc7\u6ee4\u4ee5\u51cf\u5c11\u626b\u63cf\u8303\u56f4\u3002")
    if " limit " not in f" {s} ":
        tips.append("\u672a\u53d1\u73b0 LIMIT\uff0c\u5efa\u8bae\u9650\u5236\u8fd4\u56de\u884c\u6570\u3002")
    if row_count is not None and row_count >= settings.MAX_ROWS:
        tips.append(f"\u8fd4\u56de\u884c\u6570\u5df2\u8fbe\u4e0a\u9650 {settings.MAX_ROWS}\uff0c\u53ef\u5728 SQL \u4e2d\u589e\u52a0 LIMIT \u6216\u8fc7\u6ee4\u6761\u4ef6\u3002")
    if elapsed_ms is not None and elapsed_ms >= settings.SLOW_QUERY_THRESHOLD_MS:
        tips.append("\u67e5\u8be2\u8017\u65f6\u8f83\u957f\uff0c\u5efa\u8bae\u52a0\u8fc7\u6ee4\u3001\u51cf\u5c11\u7ef4\u5ea6\u6216\u68c0\u67e5\u7d22\u5f15\u3002")
    if re.search(r"order\\s+by", s) and " limit " not in f" {s} ":
        tips.append("\u5b58\u5728 ORDER BY \u4f46\u65e0 LIMIT\uff0c\u53ef\u80fd\u5bfc\u81f4\u6392\u5e8f\u5f00\u9500\u8fc7\u5927\u3002")
    return tips


async def explain_sql(sql: str) -> str:
    if not settings.has_llm_config:
        return ""
    client = get_chat_client()
    messages = [
        {"role": "system", "content": "Explain the SQL in Chinese, concise, 1-2 sentences. No markdown."},
        {"role": "user", "content": sql},
    ]
    try:
        content = await client.chat(messages, temperature=0.2)
        return (content or "").strip()
    except Exception:
        return ""


async def suggest_sql_improvement(question: str, sql: str, row_count: int | None, elapsed_ms: int | None) -> str:
    if not settings.has_llm_config:
        return ""
    client = get_chat_client()
    stats = f"row_count={row_count}, elapsed_ms={elapsed_ms}"
    messages = [
        {
            "role": "system",
            "content": (
                "Provide 2-3 actionable SQL improvement suggestions in Chinese. "
                "Focus on filters, LIMIT, correctness, and performance. No markdown."
            ),
        },
        {"role": "user", "content": f"Question: {question}\nSQL: {sql}\nStats: {stats}"},
    ]
    try:
        content = await client.chat(messages, temperature=0.2)
        return (content or "").strip()
    except Exception:
        return ""


async def suggest_sql_fix(sql: str, error_message: str) -> str:
    if not settings.has_llm_config:
        return ""
    client = get_chat_client()
    messages = [
        {
            "role": "system",
            "content": (
                "Fix the SQL error and return a Chinese explanation plus corrected SQL. "
                "Format: suggestion: ...; fixed_sql: ... . No markdown."
            ),
        },
        {"role": "user", "content": f"SQL: {sql}\nError: {error_message}"},
    ]
    try:
        content = await client.chat(messages, temperature=0.2)
        return (content or "").strip()
    except Exception:
        return ""
