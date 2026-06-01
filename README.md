# tasklane

**Bounded-concurrency async for Python — run, map, and stream awaitables with a
concurrency limit, retries, backoff, rate limiting, and progress, in one typed call.**

[![CI](https://github.com/jpwm2/tasklane/actions/workflows/ci.yml/badge.svg)](https://github.com/jpwm2/tasklane/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/tasklane.svg)](https://pypi.org/project/tasklane/)
[![Python](https://img.shields.io/pypi/pyversions/tasklane.svg)](https://pypi.org/project/tasklane/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Types: typed](https://img.shields.io/badge/types-100%25-blue.svg)](src/tasklane/py.typed)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Every Python project that fans out async work eventually rewrites the same block:
an `asyncio.Semaphore` to cap concurrency, a `try/except` retry loop, a counter
for progress, maybe a sleep to stay under a rate limit. `tasklane` is that block,
done once — correct, fully typed, and **zero runtime dependencies**.

```python
import asyncio
import httpx
import tasklane

async def fetch(url: str) -> int:
    async with httpx.AsyncClient() as client:
        return len((await client.get(url)).text)

async def main() -> None:
    urls = [f"https://example.com/{i}" for i in range(1000)]

    sizes = await tasklane.amap(
        fetch, urls,
        limit=20,          # at most 20 requests in flight
        retries=3,         # retry failures up to 3x with exponential backoff
        rate_limit=50,     # start at most 50 requests per second
        timeout=10,        # per-attempt timeout (seconds)
    )
    print(sum(sizes))

asyncio.run(main())
```

## Install

```bash
pip install tasklane
# or
uv add tasklane
```

Requires Python 3.10+. No third-party dependencies.

## Why not just `asyncio.gather`?

`asyncio.gather` starts **everything at once**. Fan out 10,000 requests and you
open 10,000 sockets, trip rate limits, and OOM. The usual fixes are scattered
across the stdlib and third-party libs; `tasklane` brings them together:

|                              | `asyncio.gather` | `Semaphore` + `gather` | `aiometer` | **tasklane** |
| ---------------------------- | :--------------: | :--------------------: | :--------: | :----------: |
| Concurrency limit            |        ✗         |        manual          |     ✓      |      ✓       |
| Results in input order       |        ✓         |          ✓             |     ✓      |      ✓       |
| Stream results as completed  |   `as_completed` |        manual          |     ✓      |      ✓       |
| Retries + backoff            |        ✗         |          ✗             |     ✗      |      ✓       |
| Rate limiting (per second)   |        ✗         |          ✗             |     ✓      |      ✓       |
| Progress callbacks           |        ✗         |          ✗             |     ✗      |      ✓       |
| Per-task timeout             |        ✗         |        manual          |     ✗      |      ✓       |
| Backpressure on huge inputs  |        ✗         |        manual          |     ✓      |      ✓       |
| Runtime dependencies         |      stdlib      |        stdlib          |  `anyio`   |  **none**    |

## Features

### `amap` — concurrent map, results in order

```python
results = await tasklane.amap(fetch, urls, limit=10)
# results[i] corresponds to urls[i]
```

Accepts both sync and **async** iterables, and works in constant memory thanks to
a bounded internal queue — you can map over a million-item generator without
materializing a million tasks.

### `stream` — react to results as they finish

```python
async for size in tasklane.stream(fetch, urls, limit=10):
    print(size)  # arrives in completion order, fastest first
```

### `gather` — a drop-in `asyncio.gather` with a limit

```python
results = await tasklane.gather(*(fetch(u) for u in urls), limit=10)
```

On fail-fast, the remaining coroutines are cancelled **and closed**, so you never
see a `coroutine was never awaited` warning.

### Retries with backoff

```python
from tasklane import Backoff

await tasklane.amap(
    fetch, urls,
    retries=5,
    backoff=Backoff.exponential(0.2, factor=2, max_delay=30),  # 0.2, 0.4, 0.8, ... + jitter
    retry_on=(TimeoutError, ConnectionError),                  # type, tuple, or predicate
)
```

`Backoff.exponential()` (the default when `retries > 0`), `Backoff.linear()`, and
`Backoff.constant()` cover the common cases. `retry_on` accepts an exception type,
a tuple of types, or a `Callable[[BaseException], bool]` predicate.

### Rate limiting

```python
# Never start more than 100 tasks per second, regardless of the concurrency limit.
await tasklane.amap(call_api, items, limit=50, rate_limit=100)
```

### Progress

```python
from tasklane import Progress

def show(p: Progress) -> None:
    print(f"{p.completed}/{p.total}  ({p.failed} failed)  {p.rate:.0f}/s")

await tasklane.amap(fetch, urls, limit=10, on_progress=show)
```

`Progress` carries `completed`, `total`, `succeeded`, `failed`, `in_flight`, and
`elapsed`, plus `remaining`, `fraction`, and `rate` helpers. Plug it into `tqdm`,
a logger, or a web UI — no progress-bar dependency is imposed on you.

### Collect errors instead of raising

```python
results = await tasklane.amap(fetch, urls, return_exceptions=True)
ok = [r for r in results if not isinstance(r, Exception)]
```

### `Lane` — configure once, reuse everywhere

```python
from tasklane import Lane

# One policy for a specific downstream API.
github = Lane(limit=8, retries=3, rate_limit=20, timeout=10)

repos = await github.map(fetch_repo, repo_names)
async for issue in github.stream(fetch_issue, issue_ids):
    ...

# Lanes are immutable; derive a variant with .replace()
bulk = github.replace(limit=32)
```

## How it works

`tasklane` runs a fixed pool of `limit` worker coroutines that pull items off a
bounded `asyncio.Queue`. The bounded queue is what gives you backpressure and
constant memory; the worker pool is what enforces the concurrency limit exactly.
Retries, per-attempt timeouts, and rate limiting are applied inside each worker,
and completions are streamed back to the caller — collected into order for
`amap`, or yielded as-they-finish for `stream`. On any early exit (fail-fast,
`break`, or external cancellation) every in-flight task is cancelled and awaited,
so nothing leaks.

## API reference

| Symbol | Description |
| ------ | ----------- |
| `amap(func, items, *, limit, retries, backoff, retry_on, timeout, return_exceptions, rate_limit, on_progress)` | Concurrent map; returns a list in input order. |
| `stream(func, items, *, ...)` | Async iterator yielding results in completion order. |
| `gather(*coros, limit, timeout, rate_limit, return_exceptions, on_progress)` | Concurrency-limited `asyncio.gather`. |
| `Lane(...)` | Reusable, immutable bundle of settings with `.map`, `.stream`, `.gather`, `.replace`. |
| `Backoff` | Retry delay strategy: `.exponential`, `.linear`, `.constant`. |
| `Progress` | Immutable progress snapshot passed to `on_progress`. |

Full signatures and docstrings ship with the package and are surfaced by your
editor (the library is fully typed and marked with `py.typed`).

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). In short:

```bash
uv sync
uv run pytest          # tests
uv run ruff check .    # lint
uv run mypy            # types
```

## License

[MIT](LICENSE) © tasklane contributors
