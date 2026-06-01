"""A reusable, immutable bundle of concurrency settings."""

from __future__ import annotations

import dataclasses
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from typing import Any, TypeVar

from tasklane import _core
from tasklane._core import DEFAULT_LIMIT, RetryOn
from tasklane._progress import Progress
from tasklane._retry import Backoff

__all__ = ["Lane"]

T = TypeVar("T")
R = TypeVar("R")


@dataclasses.dataclass(frozen=True, slots=True)
class Lane:
    """Configure concurrency once, then reuse it across many calls.

    A ``Lane`` is an immutable bundle of the knobs you would otherwise repeat on
    every :func:`tasklane.amap` call — handy when, say, one downstream API should
    always be hit with the same concurrency, retry, and rate-limit policy::

        api = Lane(limit=8, retries=3, rate_limit=20)
        users = await api.map(fetch_user, user_ids)
        async for post in api.stream(fetch_post, post_ids):
            ...

    Lanes are frozen; use :meth:`replace` to derive a tweaked copy. Each call runs
    its own independent worker pool.
    """

    limit: int = DEFAULT_LIMIT
    retries: int = 0
    backoff: Backoff | None = None
    retry_on: RetryOn = Exception
    timeout: float | None = None
    rate_limit: float | None = None
    on_progress: Callable[[Progress], None] | None = None

    def __post_init__(self) -> None:
        _core._validate(self.limit, self.retries, self.timeout, self.rate_limit)

    async def map(
        self,
        func: Callable[[T], Awaitable[R]],
        items: Iterable[T] | AsyncIterable[T],
    ) -> list[R]:
        """Run :func:`tasklane.amap` with this lane's settings (fail-fast)."""
        return await _core.amap(
            func,
            items,
            limit=self.limit,
            retries=self.retries,
            backoff=self.backoff,
            retry_on=self.retry_on,
            timeout=self.timeout,
            rate_limit=self.rate_limit,
            on_progress=self.on_progress,
        )

    def stream(
        self,
        func: Callable[[T], Awaitable[R]],
        items: Iterable[T] | AsyncIterable[T],
    ) -> AsyncIterator[R]:
        """Run :func:`tasklane.stream` with this lane's settings (fail-fast)."""
        return _core.stream(
            func,
            items,
            limit=self.limit,
            retries=self.retries,
            backoff=self.backoff,
            retry_on=self.retry_on,
            timeout=self.timeout,
            rate_limit=self.rate_limit,
            on_progress=self.on_progress,
        )

    async def gather(self, *coros: Awaitable[T]) -> list[T]:
        """Run :func:`tasklane.gather` with this lane's limit/timeout/rate policy.

        Note that ``retries`` and ``backoff`` do not apply: a coroutine can only
        be awaited once, so failed coroutines cannot be retried.
        """
        return await _core.gather(
            *coros,
            limit=self.limit,
            timeout=self.timeout,
            rate_limit=self.rate_limit,
            on_progress=self.on_progress,
        )

    def replace(self, **changes: Any) -> Lane:
        """Return a copy of this lane with the given fields overridden."""
        return dataclasses.replace(self, **changes)
