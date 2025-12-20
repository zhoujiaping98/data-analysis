from __future__ import annotations

import asyncio
import random
import time
from typing import Any, Awaitable, Callable, Optional, Type


class CircuitOpenError(RuntimeError):
    pass


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout_s: int = 30):
        self.name = name
        self.failure_threshold = max(1, int(failure_threshold))
        self.recovery_timeout_s = max(1, int(recovery_timeout_s))
        self._state = "closed"
        self._failure_count = 0
        self._last_failure_ts = 0.0

    @property
    def state(self) -> str:
        return self._state

    def check(self) -> None:
        if self._state != "open":
            return
        now = time.time()
        if (now - self._last_failure_ts) >= self.recovery_timeout_s:
            self._state = "half_open"
            return
        raise CircuitOpenError(f"circuit {self.name} is open")

    def record_success(self) -> None:
        self._failure_count = 0
        self._state = "closed"

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_ts = time.time()
        if self._failure_count >= self.failure_threshold:
            self._state = "open"


async def async_retry(
    op: Callable[[], Awaitable],
    *,
    retries: int = 2,
    base_delay_s: float = 0.4,
    max_delay_s: float = 4.0,
    retry_on: Optional[tuple[Type[BaseException], ...]] = None,
    should_retry: Optional[Callable[[BaseException], bool]] = None,
) -> Any:
    attempt = 0
    while True:
        try:
            return await op()
        except Exception as e:
            if retry_on and not isinstance(e, retry_on):
                raise
            if should_retry and not should_retry(e):
                raise
            if attempt >= retries:
                raise

            delay = min(max_delay_s, base_delay_s * (2 ** attempt))
            delay *= 0.5 + random.random() * 0.5
            await asyncio.sleep(delay)
            attempt += 1
