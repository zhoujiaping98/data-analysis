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
    get_message_artifact,
)

import orjson

router = APIRouter()

@router.post("/conversations", response_model=CreateConversationResponse)
async def create_conversation(user=Depends(get_current_user)):
    conv_id = str(uuid.uuid4())
    await upsert_conversation(conv_id, owner_username=user["username"])
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
    messages = await get_messages(conversation_id, limit=50)
    for m in messages:
        if m["role"] != "user":
            continue
        artifact = await get_message_artifact(conversation_id, m["id"])
        if artifact:
            try:
                m["artifact"] = {
                    "sql": artifact["sql_text"],
                    "columns": orjson.loads(artifact["columns_json"]),
                    "rows": orjson.loads(artifact["rows_json"]),
                    "chart": orjson.loads(artifact["chart_json"]) if artifact["chart_json"] else None,
                }
            except Exception:
                pass
    return messages

@router.delete("/conversations/{conversation_id}")
async def remove_conversation(conversation_id: str, user=Depends(get_current_user)):
    conv = await get_conversation(conversation_id)
    if conv is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    if conv["owner_username"] != user["username"]:
        raise HTTPException(status_code=403, detail="Forbidden")
    await delete_conversation(conversation_id)
    return {"ok": True}
