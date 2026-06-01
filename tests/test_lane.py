from __future__ import annotations

import asyncio

import pytest

from tasklane import Backoff, Lane


async def test_lane_map_applies_settings() -> None:
    active = 0
    peak = 0

    async def work(x: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return x * 2

    lane = Lane(limit=3)
    assert await lane.map(work, range(10)) == [x * 2 for x in range(10)]
    assert peak <= 3


async def test_lane_retries() -> None:
    attempts = {"n": 0}

    async def flaky(x: int) -> int:
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise ValueError("transient")
        return x

    lane = Lane(retries=3, backoff=Backoff.constant(0))
    assert await lane.map(flaky, [99]) == [99]


async def test_lane_stream() -> None:
    async def work(x: int) -> int:
        await asyncio.sleep(x / 100)
        return x

    lane = Lane(limit=4)
    got = [r async for r in lane.stream(work, [2, 0, 1])]
    assert got == [0, 1, 2]


async def test_lane_gather() -> None:
    async def work(x: int) -> int:
        return x

    lane = Lane(limit=2)
    assert await lane.gather(work(1), work(2), work(3)) == [1, 2, 3]


async def test_lane_replace_is_immutable() -> None:
    base = Lane(limit=4, retries=1)
    derived = base.replace(limit=8)
    assert base.limit == 4
    assert derived.limit == 8
    assert derived.retries == 1


def test_lane_validates_on_construction() -> None:
    with pytest.raises(ValueError, match="limit"):
        Lane(limit=0)
