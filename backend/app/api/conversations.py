from __future__ import annotations

import uuid
from fastapi import APIRouter, Depends, HTTPException
from backend.app.api.deps import get_current_user
from backend.app.schemas.chat import CreateConversationResponse
from backend.app.core.sqlite_store import (
    upsert_conversation,
    list_conversations,
    get_messages,
    get_conversation,
    delete_conversation,
)

router = APIRouter()

@router.post("/conversations", response_model=CreateConversationResponse)
async def create_conversation(user=Depends(get_current_user)):
    conv_id = str(uuid.uuid4())
    await upsert_conversation(conv_id, owner_username=user["username"], title="New Conversation")
    return CreateConversationResponse(conversation_id=conv_id)

@router.get("/conversations")
async def get_conversations(user=Depends(get_current_user)):
    return await list_conversations(user["username"])

@router.get("/conversations/{conversation_id}/messages")
async def conversation_messages(conversation_id: str, user=Depends(get_current_user)):
    conv = await get_conversation(conversation_id)
    if conv is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    if conv["owner_username"] != user["username"]:
        raise HTTPException(status_code=403, detail="Forbidden")
    return await get_messages(conversation_id, limit=50)

@router.delete("/conversations/{conversation_id}")
async def remove_conversation(conversation_id: str, user=Depends(get_current_user)):
    conv = await get_conversation(conversation_id)
    if conv is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    if conv["owner_username"] != user["username"]:
        raise HTTPException(status_code=403, detail="Forbidden")
    await delete_conversation(conversation_id)
    return {"ok": True}
