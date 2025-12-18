from __future__ import annotations

import datetime as dt
from typing import Optional, Dict, Any

from jose import jwt, JWTError
from passlib.context import CryptContext

from backend.app.core.config import settings

# NOTE: passlib's bcrypt handler is incompatible with some newer `bcrypt` releases on Windows
# (it fails backend self-check during initialization). For a demo-friendly default, use
# PBKDF2-SHA256 which is pure-Python and widely supported.
pwd_context = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)

def create_access_token(subject: str, extra: Optional[Dict[str, Any]] = None) -> str:
    expire = dt.datetime.utcnow() + dt.timedelta(minutes=settings.JWT_EXPIRE_MINUTES)
    payload: Dict[str, Any] = {"sub": subject, "exp": expire}
    if extra:
        payload.update(extra)
    return jwt.encode(payload, settings.JWT_SECRET or "dev-secret", algorithm=settings.JWT_ALGORITHM)

def decode_access_token(token: str) -> Dict[str, Any]:
    try:
        payload = jwt.decode(
            token,
            settings.JWT_SECRET or "dev-secret",
            algorithms=[settings.JWT_ALGORITHM],
        )
        return payload
    except JWTError as e:
        raise ValueError(str(e)) from e
