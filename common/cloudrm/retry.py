from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    attempts: int,
    delay_seconds: float,
    name: str,
) -> T:
    logger = logging.getLogger("cloudrm.retry")
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await operation()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning(
                "Зависимость пока недоступна",
                extra={"cloudrm_dependency": name, "cloudrm_attempt": attempt, "cloudrm_error": str(exc)},
            )
            await asyncio.sleep(delay_seconds)
    raise RuntimeError(f"Зависимость {name} не стала доступна после {attempts} попыток") from last_error
