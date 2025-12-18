from __future__ import annotations

from fastapi import APIRouter, HTTPException
from backend.app.schemas.auth import LoginRequest, LoginResponse
from backend.app.core.sqlite_store import get_user, create_user
from backend.app.core.security import verify_password, hash_password, create_access_token

router = APIRouter()

@router.post("/auth/login", response_model=LoginResponse)
async def login(req: LoginRequest):
    user = await get_user(req.username)
    # Auto-register on first login (demo-friendly). Replace with your own admin provisioning in production.
    if user is None:
        await create_user(req.username, hash_password(req.password))
        user = await get_user(req.username)

    if user is None or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    token = create_access_token(subject=req.username, extra={"username": req.username, "groups": ["user"]})
    return LoginResponse(access_token=token)
