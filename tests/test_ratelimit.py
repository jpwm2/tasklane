from __future__ import annotations

from time import monotonic

import pytest

from tasklane import amap
from tasklane._ratelimit import RateLimiter


async def test_rate_limit_spaces_out_task_starts() -> None:
    async def work(x: int) -> int:
        return x

    start = monotonic()
    # 20 near-instant tasks at 100/s with strict spacing => ~19 * 0.01s.
    await amap(work, range(20), limit=20, rate_limit=100)
    elapsed = monotonic() - start
    assert elapsed >= 0.15
    assert elapsed < 2.0


async def test_burst_allows_initial_concurrency() -> None:
    starts: list[float] = []
    base = monotonic()

    async def work(x: int) -> int:
        starts.append(monotonic() - base)
        return x

    limiter = RateLimiter(50, burst=5)
    for _ in range(5):
        await limiter.acquire()
    # The burst of 5 tokens is consumed without waiting.
    assert monotonic() - base < 0.05
    # Re-use to keep `starts`/`work` referenced for clarity of intent.
    await amap(work, [], limit=1)
    assert starts == []


def test_invalid_rate() -> None:
    with pytest.raises(ValueError, match="rate"):
        RateLimiter(0)


def test_invalid_burst() -> None:
    with pytest.raises(ValueError, match="burst"):
        RateLimiter(10, burst=0)


async def test_average_rate_is_bounded() -> None:
    limiter = RateLimiter(200)
    start = monotonic()
    for _ in range(40):
        await limiter.acquire()
    elapsed = monotonic() - start
    # 40 acquisitions at 200/s (burst=1) => at least ~39/200s.
    assert elapsed >= 0.15
