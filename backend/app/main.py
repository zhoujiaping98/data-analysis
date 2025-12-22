from __future__ import annotations

import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from backend.app.core.config import settings
from backend.app.core.logging import setup_logging
from backend.app.api.health import router as health_router
from backend.app.api.auth import router as auth_router
from backend.app.api.conversations import router as conv_router
from backend.app.api.chat import router as chat_router
from backend.app.api.schema import router as schema_router
from backend.app.api.files import router as files_router
from backend.app.api.datasources import router as datasources_router
from backend.app.api.export import router as export_router
from backend.app.api.sql import router as sql_router
from backend.app.api.audits import router as audits_router
from backend.app.api.scopes import router as scopes_router
from backend.app.core.sqlite_store import init_sqlite
from backend.app.core.mysql import close_engine
from backend.app.core.uploads import cleanup_expired_uploads
from backend.app.core.datasources import ensure_default_datasource
from backend.app.core.schema_monitor import run_schema_check

setup_logging()

app = FastAPI(title=settings.APP_NAME)

# CORS (adjust for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(health_router, prefix="/api")
app.include_router(auth_router, prefix="/api")
app.include_router(conv_router, prefix="/api")
app.include_router(chat_router, prefix="/api")
app.include_router(schema_router, prefix="/api")
app.include_router(files_router, prefix="/api")
app.include_router(datasources_router, prefix="/api")
app.include_router(export_router, prefix="/api")
app.include_router(sql_router, prefix="/api")
app.include_router(audits_router, prefix="/api")
app.include_router(scopes_router, prefix="/api")

# Serve frontend
frontend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "frontend"))
app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")

@app.on_event("startup")
async def _startup() -> None:
    # init sqlite for user + conversation store
    await init_sqlite()
    await ensure_default_datasource()
    await cleanup_expired_uploads()
    await run_schema_check()


@app.on_event("shutdown")
async def _shutdown() -> None:
    await close_engine()
