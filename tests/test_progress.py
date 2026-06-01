from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from tasklane import Progress, amap


def test_progress_properties_with_total() -> None:
    p = Progress(completed=2, total=8, succeeded=2, failed=0, in_flight=3, elapsed=1.0)
    assert p.remaining == 6
    assert p.fraction == 0.25
    assert p.rate == 2.0


def test_progress_properties_without_total() -> None:
    p = Progress(completed=5, total=None, succeeded=5, failed=0, in_flight=1, elapsed=0.0)
    assert p.remaining is None
    assert p.fraction is None
    assert p.rate == 0.0


async def test_on_progress_called_for_each_task() -> None:
    snapshots: list[Progress] = []

    async def work(x: int) -> int:
        await asyncio.sleep(0.005)
        return x

    await amap(work, range(10), limit=3, on_progress=snapshots.append)
    assert len(snapshots) == 10
    assert [s.completed for s in snapshots] == list(range(1, 11))
    assert snapshots[-1].total == 10
    assert snapshots[-1].succeeded == 10
    assert snapshots[-1].failed == 0


async def test_progress_counts_failures() -> None:
    snapshots: list[Progress] = []

    async def work(x: int) -> int:
        if x == 0:
            raise ValueError("boom")
        return x

    await amap(work, range(4), on_progress=snapshots.append, return_exceptions=True)
    assert snapshots[-1].failed == 1
    assert snapshots[-1].succeeded == 3


async def test_progress_total_none_for_unsized() -> None:
    snapshots: list[Progress] = []

    async def gen() -> AsyncIterator[int]:
        for i in range(3):
            yield i

    async def work(x: int) -> int:
        return x

    await amap(work, gen(), on_progress=snapshots.append)
    assert all(s.total is None for s in snapshots)
