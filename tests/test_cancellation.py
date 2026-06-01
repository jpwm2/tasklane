from __future__ import annotations

import asyncio

import pytest

from tasklane import amap


async def test_fail_fast_cancels_inflight_tasks() -> None:
    cancelled: list[int] = []

    async def work(x: int) -> int:
        if x == 0:
            await asyncio.sleep(0.01)
            raise ValueError("boom")
        try:
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            cancelled.append(x)
            raise
        return x

    with pytest.raises(ValueError, match="boom"):
        await amap(work, range(6), limit=6)
    await asyncio.sleep(0.05)
    assert cancelled, "sibling tasks should be cancelled on fail-fast"


async def test_timeout_per_attempt() -> None:
    async def slow(x: int) -> int:
        await asyncio.sleep(1)
        return x

    with pytest.raises(TimeoutError):
        await amap(slow, [1], timeout=0.05)


async def test_timeout_is_retryable() -> None:
    attempts = {"n": 0}

    async def sometimes_slow(x: int) -> int:
        attempts["n"] += 1
        if attempts["n"] == 1:
            await asyncio.sleep(1)
        return x

    from tasklane import Backoff

    out = await amap(sometimes_slow, [5], timeout=0.05, retries=2, backoff=Backoff.constant(0))
    assert out == [5]
    assert attempts["n"] == 2


async def test_external_cancellation_is_clean() -> None:
    started: list[int] = []

    async def work(x: int) -> int:
        started.append(x)
        await asyncio.sleep(10)
        return x

    task = asyncio.create_task(amap(work, range(100), limit=5))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Concurrency was bounded, so we never started anywhere near all 100.
    assert len(started) <= 10
