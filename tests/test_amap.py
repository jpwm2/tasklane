from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from tasklane import amap


async def test_preserves_input_order() -> None:
    async def work(x: int) -> int:
        # Smaller numbers finish later, so completion order != input order.
        await asyncio.sleep((10 - x) / 200)
        return x * x

    assert await amap(work, range(10), limit=4) == [x * x for x in range(10)]


async def test_concurrency_limit_is_respected() -> None:
    active = 0
    peak = 0

    async def work(x: int) -> int:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return x

    await amap(work, range(50), limit=5)
    assert peak <= 5


async def test_empty_input() -> None:
    async def work(x: int) -> int:
        return x

    assert await amap(work, [], limit=4) == []


async def test_limit_one_is_sequential() -> None:
    order: list[int] = []

    async def work(x: int) -> int:
        order.append(x)
        await asyncio.sleep(0.01)
        return x

    await amap(work, range(5), limit=1)
    assert order == [0, 1, 2, 3, 4]


async def test_async_iterable_input() -> None:
    async def gen() -> AsyncIterator[int]:
        for i in range(6):
            await asyncio.sleep(0)
            yield i

    async def work(x: int) -> int:
        return x + 1

    assert await amap(work, gen(), limit=3) == [1, 2, 3, 4, 5, 6]


async def test_unsized_generator_input() -> None:
    def gen() -> object:
        return (i for i in range(7))

    async def work(x: int) -> int:
        await asyncio.sleep((7 - x) / 200)
        return x

    assert await amap(work, gen(), limit=3) == list(range(7))  # type: ignore[call-overload]


async def test_return_exceptions_places_errors() -> None:
    async def work(x: int) -> int:
        if x % 2 == 0:
            raise ValueError(x)
        return x

    out = await amap(work, range(4), return_exceptions=True)
    assert out[1] == 1
    assert out[3] == 3
    assert isinstance(out[0], ValueError)
    assert isinstance(out[2], ValueError)


async def test_fail_fast_raises_first_error() -> None:
    async def work(x: int) -> int:
        if x == 3:
            raise RuntimeError("boom")
        await asyncio.sleep(0.01)
        return x

    with pytest.raises(RuntimeError, match="boom"):
        await amap(work, range(10), limit=10)


async def test_propagates_iterable_error() -> None:
    async def gen() -> AsyncIterator[int]:
        yield 1
        raise KeyError("bad source")

    async def work(x: int) -> int:
        return x

    with pytest.raises(KeyError, match="bad source"):
        await amap(work, gen(), limit=2)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"limit": 0}, "limit"),
        ({"retries": -1}, "retries"),
        ({"timeout": 0}, "timeout"),
        ({"rate_limit": -5}, "rate_limit"),
    ],
)
async def test_validation(kwargs: dict[str, int], match: str) -> None:
    async def work(x: int) -> int:
        return x

    with pytest.raises(ValueError, match=match):
        await amap(work, [1], **kwargs)  # type: ignore[call-overload]
