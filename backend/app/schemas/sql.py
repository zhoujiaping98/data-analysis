from pydantic import BaseModel


class SqlExecuteRequest(BaseModel):
    conversation_id: str
    message_id: int
    sql: str
    with_analysis: bool = True
    view: dict | None = None
