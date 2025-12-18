from __future__ import annotations

import json
from typing import Any, Dict, List

from backend.app.core.config import settings
from backend.app.core.llm import get_chat_client

ANALYSIS_PROMPT = """你是一名数据分析师。你会收到：
- 用户问题
- 实际执行的 SQL
- 查询结果的部分样本行（可能被截断）

请用中文输出，并包含：
1）关键结论（要点列表）
2）口径说明 / 数据质量与局限（要点列表）
3）下一步建议（2-3 条可执行的后续查询方向）

要求：简洁、可落地，不要输出 Markdown 代码块。
"""

async def analyze(question: str, sql: str, columns: List[str], rows: List[List[Any]]) -> str:
    client = get_chat_client()
    sample = rows[:50]
    payload = {
        "question": question,
        "sql": sql,
        "columns": columns,
        "sample_rows": sample,
        "row_count_shown": len(rows),
    }
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": ANALYSIS_PROMPT},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    content = await client.chat(messages, temperature=0.2)
    return content.strip()
