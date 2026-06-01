from __future__ import annotations

import asyncio
import gc
import warnings

import pytest

from tasklane import gather


async def test_preserves_order() -> None:
    async def work(x: int) -> int:
        await asyncio.sleep((5 - x) / 200)
        return x

    assert await gather(*(work(i) for i in range(5)), limit=2) == [0, 1, 2, 3, 4]


async def test_concurrency_limit() -> None:
    active = 0
    peak = 0

    async def work(x: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return x

    await gather(*(work(i) for i in range(20)), limit=3)
    assert peak <= 3


async def test_return_exceptions() -> None:
    async def ok(x: int) -> int:
        return x

    async def bad() -> int:
        raise ValueError("nope")

    out = await gather(ok(1), bad(), ok(2), return_exceptions=True)
    assert out[0] == 1
    assert isinstance(out[1], ValueError)
    assert out[2] == 2


async def test_fail_fast_does_not_leak_never_awaited_warnings() -> None:
    async def work(x: int) -> int:
        if x == 0:
            raise ValueError("boom")
        await asyncio.sleep(0.05)
        return x

    coros = [work(i) for i in range(12)]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(ValueError):
            await gather(*coros, limit=2)
    gc.collect()
    messages = [str(w.message) for w in caught]
    assert not any("never awaited" in m for m in messages), messages


async def test_timeout() -> None:
    async def slow() -> int:
        await asyncio.sleep(1)
        return 1

    with pytest.raises(TimeoutError):
        await gather(slow(), timeout=0.05)
