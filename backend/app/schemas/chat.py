from pydantic import BaseModel
from typing import List, Optional

class CreateConversationResponse(BaseModel):
    conversation_id: str

class ChatRequest(BaseModel):
    conversation_id: str
    message: str
    allowed_tables: Optional[List[str]] = None
    table_lock: bool = False
    scope_name: Optional[str] = None
