from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


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
