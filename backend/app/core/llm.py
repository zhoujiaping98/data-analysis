from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx

from backend.app.core.config import settings
from backend.app.core.resilience import CircuitBreaker, CircuitOpenError, async_retry

log = logging.getLogger("llm")

_chat_breaker = CircuitBreaker(
    "llm_chat",
    failure_threshold=settings.LLM_CB_FAILURES,
    recovery_timeout_s=settings.LLM_CB_RECOVERY_SECONDS,
)
_embed_breaker = CircuitBreaker(
    "llm_embed",
    failure_threshold=settings.EMBED_CB_FAILURES,
    recovery_timeout_s=settings.EMBED_CB_RECOVERY_SECONDS,
)


def _is_retryable_http(err: BaseException) -> bool:
    if isinstance(err, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(err, httpx.HTTPStatusError):
        code = err.response.status_code
        return code in (408, 429) or 500 <= code < 600
    return False


class OpenAICompatChatClient:
    def __init__(self, base_url: str, api_key: str, model: str, timeout_s: int):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout_s = timeout_s

    async def _post_json(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        _chat_breaker.check()
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}

        async def _do():
            async with httpx.AsyncClient(timeout=self.timeout_s) as client:
                resp = await client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                return resp.json()

        try:
            data = await async_retry(
                _do,
                retries=settings.LLM_MAX_RETRIES,
                base_delay_s=settings.LLM_RETRY_BASE_SECONDS,
                should_retry=_is_retryable_http,
            )
            _chat_breaker.record_success()
            return data
        except Exception:
            _chat_breaker.record_failure()
            raise

    async def chat(self, messages: List[Dict[str, str]], *, temperature: float = 0.2) -> str:
        url = f"{self.base_url}/chat/completions"
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
        }
        data = await self._post_json(url, payload)

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

        attempt = 0
        while True:
            yielded = False
            try:
                _chat_breaker.check()
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
                                _chat_breaker.record_success()
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
                                yielded = True
                                yield chunk
                _chat_breaker.record_success()
                return
            except CircuitOpenError:
                raise
            except Exception as e:
                _chat_breaker.record_failure()
                if yielded:
                    return
                if attempt >= settings.LLM_MAX_RETRIES or not _is_retryable_http(e):
                    raise
                delay = min(4.0, settings.LLM_RETRY_BASE_SECONDS * (2 ** attempt))
                delay *= 0.5 + (hash(e) % 100) / 200.0
                await asyncio.sleep(delay)
                attempt += 1


class OpenAICompatEmbeddingClient:
    def __init__(self, base_url: str, api_key: str, model: str, timeout_s: int):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout_s = timeout_s

    async def embed(self, texts: List[str]) -> List[List[float]]:
        url = f"{self.base_url}/embeddings"
        payload: Dict[str, Any] = {"model": self.model, "input": texts}
        _embed_breaker.check()

        async def _do():
            headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
            async with httpx.AsyncClient(timeout=self.timeout_s) as client:
                resp = await client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                return resp.json()

        try:
            data = await async_retry(
                _do,
                retries=settings.EMBED_MAX_RETRIES,
                base_delay_s=settings.EMBED_RETRY_BASE_SECONDS,
                should_retry=_is_retryable_http,
            )
            _embed_breaker.record_success()
        except Exception:
            _embed_breaker.record_failure()
            raise
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
