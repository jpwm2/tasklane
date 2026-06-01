from __future__ import annotations

import asyncio

import pytest

from tasklane import stream


async def test_yields_in_completion_order() -> None:
    async def work(x: int) -> int:
        await asyncio.sleep(x / 100)
        return x

    got = [r async for r in stream(work, [3, 1, 2, 0], limit=4)]
    assert got == [0, 1, 2, 3]


async def test_all_results_present_regardless_of_order() -> None:
    async def work(x: int) -> int:
        await asyncio.sleep(0.001)
        return x * 10

    got = sorted([r async for r in stream(work, range(20), limit=4)])
    assert got == [x * 10 for x in range(20)]


async def test_return_exceptions() -> None:
    async def work(x: int) -> int:
        if x == 1:
            raise ValueError("boom")
        return x

    results = [r async for r in stream(work, [0, 1, 2], limit=1, return_exceptions=True)]
    assert {type(r).__name__ if isinstance(r, Exception) else r for r in results} == {
        0,
        2,
        "ValueError",
    }


async def test_fail_fast_raises() -> None:
    async def work(x: int) -> int:
        if x == 2:
            raise RuntimeError("boom")
        await asyncio.sleep(0.01)
        return x

    with pytest.raises(RuntimeError):
        async for _ in stream(work, range(10), limit=10):
            pass


async def test_early_break_cancels_inflight() -> None:
    cancelled: list[int] = []

    async def work(x: int) -> int:
        try:
            await asyncio.sleep(x / 50)
            return x
        except asyncio.CancelledError:
            cancelled.append(x)
            raise

    async for r in stream(work, range(20), limit=20):
        # Take the first result, then bail out.
        assert r == 0
        break
    await asyncio.sleep(0.05)
    assert cancelled, "breaking out of stream should cancel in-flight tasks"
