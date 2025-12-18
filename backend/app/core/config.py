from __future__ import annotations

from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    APP_NAME: str = "Vanna2 Analytics Tool"
    APP_ENV: str = "dev"

    JWT_SECRET: str = Field(default="", description="JWT secret (required in production)")
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 60 * 24 * 7

    # Target MySQL (analytics)
    MYSQL_HOST: str = ""
    MYSQL_PORT: int = 3306
    MYSQL_DATABASE: str = ""
    MYSQL_USER: str = ""
    MYSQL_PASSWORD: str = ""

    # LLM (DeepSeek)
    DEEPSEEK_BASE_URL: str = ""
    DEEPSEEK_API_KEY: str = ""
    DEEPSEEK_MODEL: str = "deepseek-chat"
    LLM_TEMPERATURE: float = 0.2
    LLM_TIMEOUT_SECONDS: int = 90

    # Embeddings (Qwen3)
    EMBED_BASE_URL: str = ""
    EMBED_API_KEY: str = ""
    EMBED_MODEL: str = "Qwen3-Embedding-8B"
    EMBED_TIMEOUT_SECONDS: int = 60

    # Vector store
    CHROMA_PERSIST_DIR: str = "./data/chroma"
    CHROMA_COLLECTION: str = "mysql_schema"

    # limits
    MAX_ROWS: int = 500
    MAX_SQL_RETRY: int = 2

    @staticmethod
    def _strip_inline_comment(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        # Be forgiving: accept "123 # comment" and "123#comment" in .env values.
        return value.split("#", 1)[0].strip()

    @field_validator(
        "JWT_EXPIRE_MINUTES",
        "MYSQL_PORT",
        "LLM_TIMEOUT_SECONDS",
        "EMBED_TIMEOUT_SECONDS",
        "MAX_ROWS",
        "MAX_SQL_RETRY",
        mode="before",
    )
    @classmethod
    def _strip_int_comments(cls, v: Any) -> Any:
        return cls._strip_inline_comment(v)

    @field_validator("LLM_TEMPERATURE", mode="before")
    @classmethod
    def _strip_float_comments(cls, v: Any) -> Any:
        return cls._strip_inline_comment(v)

    @property
    def mysql_dsn(self) -> str:
        # SQLAlchemy async DSN for aiomysql
        return (
            f"mysql+aiomysql://{self.MYSQL_USER}:{self.MYSQL_PASSWORD}"
            f"@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{self.MYSQL_DATABASE}"
        )

    @property
    def has_mysql_config(self) -> bool:
        return all([self.MYSQL_HOST, self.MYSQL_DATABASE, self.MYSQL_USER, self.MYSQL_PASSWORD])

    @property
    def has_llm_config(self) -> bool:
        return all([self.DEEPSEEK_BASE_URL, self.DEEPSEEK_API_KEY, self.DEEPSEEK_MODEL])

    @property
    def has_embed_config(self) -> bool:
        return all([self.EMBED_BASE_URL, self.EMBED_API_KEY, self.EMBED_MODEL])


settings = Settings()
