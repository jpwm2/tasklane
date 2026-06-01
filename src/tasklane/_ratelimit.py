"""An async token-bucket rate limiter used to cap how fast tasks start."""

from __future__ import annotations

import asyncio
from time import monotonic

__all__ = ["RateLimiter"]


class RateLimiter:
    """Admits at most ``rate`` acquisitions per second on average.

    Implemented as a token bucket. ``burst`` controls how many acquisitions may
    happen back-to-back before the steady rate kicks in; the default of ``1``
    enforces strict, evenly spaced starts.
    """

    __slots__ = ("_capacity", "_lock", "_rate", "_tokens", "_updated")

    def __init__(self, rate: float, *, burst: int = 1) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if burst < 1:
            raise ValueError("burst must be >= 1")
        self._rate = rate
        self._capacity = float(burst)
        self._tokens = float(burst)
        self._updated = monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until a token is available, then consume it."""
        async with self._lock:
            while True:
                now = monotonic()
                self._tokens = min(
                    self._capacity, self._tokens + (now - self._updated) * self._rate
                )
                self._updated = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                await asyncio.sleep((1 - self._tokens) / self._rate)
