from pydantic import BaseModel
from typing import List, Optional


class SqlExecuteRequest(BaseModel):
    conversation_id: str
    message_id: int
    sql: str
    with_analysis: bool = True
    view: dict | None = None
    allowed_tables: Optional[List[str]] = None
    table_lock: bool = False
