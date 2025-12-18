from pydantic import BaseModel

class CreateConversationResponse(BaseModel):
    conversation_id: str

class ChatRequest(BaseModel):
    conversation_id: str
    message: str
