from __future__ import annotations

import json
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx

from backend.app.core.config import settings

log = logging.getLogger("llm")


class OpenAICompatChatClient:
    def __init__(self, base_url: str, api_key: str, model: str, timeout_s: int):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout_s = timeout_s

    async def chat(self, messages: List[Dict[str, str]], *, temperature: float = 0.2) -> str:
        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
        }
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()

        # Some OpenAI-compatible providers return additional nested usage fields.
        # Vanna 2.0 users often need to sanitize those (see related issue). citeturn6view0
        try:
            return data["choices"][0]["message"]["content"]
        except Exception as e:
            log.error("Unexpected chat response: %s", data)
            raise RuntimeError(f"Unexpected chat response: {e}") from e

    async def chat_stream(
        self, messages: List[Dict[str, str]], *, temperature: float = 0.2
    ) -> AsyncGenerator[str, None]:
        url = f"{self.base_url}/chat/completions"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "stream": True,
        }

        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as resp:
                resp.raise_for_status()
                async for line in resp.aiter_lines():
                    if not line or line.startswith(":"):
                        continue
                    if not line.startswith("data:"):
                        continue
                    data_str = line[5:].strip()
                    if not data_str:
                        continue
                    if data_str == "[DONE]":
                        return

                    try:
                        data = json.loads(data_str)
                    except Exception:
                        continue

                    choices = data.get("choices") or []
                    if not choices:
                        continue
                    choice = choices[0] or {}
                    delta = choice.get("delta") or {}
                    chunk = delta.get("content")
                    if chunk is None:
                        msg = choice.get("message") or {}
                        chunk = msg.get("content")
                    if chunk:
                        yield chunk


class OpenAICompatEmbeddingClient:
    def __init__(self, base_url: str, api_key: str, model: str, timeout_s: int):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout_s = timeout_s

    async def embed(self, texts: List[str]) -> List[List[float]]:
        url = f"{self.base_url}/embeddings"
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        payload: Dict[str, Any] = {"model": self.model, "input": texts}
        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            resp = await client.post(url, headers=headers, json=payload)
            resp.raise_for_status()
            data = resp.json()
        # OpenAI shape: {"data":[{"embedding":[...], "index":0}, ...]}
        return [item["embedding"] for item in data["data"]]


def get_chat_client() -> OpenAICompatChatClient:
    if not settings.has_llm_config:
        raise RuntimeError("LLM config missing: set DEEPSEEK_BASE_URL/DEEPSEEK_API_KEY/DEEPSEEK_MODEL")
    return OpenAICompatChatClient(
        base_url=settings.DEEPSEEK_BASE_URL,
        api_key=settings.DEEPSEEK_API_KEY,
        model=settings.DEEPSEEK_MODEL,
        timeout_s=settings.LLM_TIMEOUT_SECONDS,
    )


def get_embed_client() -> OpenAICompatEmbeddingClient:
    if not settings.has_embed_config:
        raise RuntimeError("Embedding config missing: set EMBED_BASE_URL/EMBED_API_KEY/EMBED_MODEL")
    return OpenAICompatEmbeddingClient(
        base_url=settings.EMBED_BASE_URL,
        api_key=settings.EMBED_API_KEY,
        model=settings.EMBED_MODEL,
        timeout_s=settings.EMBED_TIMEOUT_SECONDS,
    )
