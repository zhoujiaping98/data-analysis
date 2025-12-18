from __future__ import annotations

import json
from typing import Any, AsyncGenerator, Dict, Optional


def sse_event(event: str, data: Dict[str, Any] | str, event_id: Optional[str] = None) -> str:
    if isinstance(data, str):
        payload = data
    else:
        payload = json.dumps(data, ensure_ascii=False)

    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    for chunk in payload.splitlines() or [""]:
        lines.append(f"data: {chunk}")
    lines.append("")  # end of message
    return "\n".join(lines) + "\n"


async def sse_stream(generator: AsyncGenerator[str, None]) -> AsyncGenerator[bytes, None]:
    async for msg in generator:
        yield msg.encode("utf-8")
