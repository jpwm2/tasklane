"""tasklane: bounded-concurrency async for Python.

Run, map, and stream awaitables with a concurrency limit, retries, backoff,
rate limiting, and progress reporting — in one typed call.

Quickstart::

    import asyncio
    import tasklane

    async def fetch(url: str) -> int:
        await asyncio.sleep(0.1)
        return len(url)

    async def main() -> None:
        urls = ["https://example.com"] * 100
        # At most 10 concurrent calls, each retried up to 3 times.
        sizes = await tasklane.amap(fetch, urls, limit=10, retries=3)
        print(sum(sizes))

    asyncio.run(main())
"""

from __future__ import annotations

from tasklane._core import amap, gather, stream
from tasklane._lane import Lane
from tasklane._progress import Progress
from tasklane._retry import Backoff

__all__ = [
    "Backoff",
    "Lane",
    "Progress",
    "amap",
    "gather",
    "stream",
]

__version__ = "0.1.0"
