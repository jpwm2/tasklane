"""Core engine: a bounded worker pool over an async queue.

Both :func:`amap` and :func:`stream` are thin policy layers over the private
``_imap_unordered`` generator, which runs ``limit`` workers that pull items off
a bounded input queue, apply retries/timeout/rate-limiting, and emit completions
as they finish. The bounded input queue gives natural backpressure, so even an
infinite async iterable is processed in constant memory.
"""

from __future__ import annotations

import asyncio
from collections.abc import (
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Sized,
)
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, Generic, Literal, TypeVar, overload

from tasklane._progress import Progress
from tasklane._ratelimit import RateLimiter
from tasklane._retry import Backoff

__all__ = ["amap", "gather", "stream"]

T = TypeVar("T")
R = TypeVar("R")

#: A type, a tuple of types, or a predicate deciding whether to retry an error.
RetryOn = type[BaseException] | tuple[type[BaseException], ...] | Callable[[BaseException], bool]

DEFAULT_LIMIT = 16
_DEFAULT_BACKOFF = Backoff()


class _WorkerExit:
    """Sentinel a worker emits when it has consumed its stop signal."""

    __slots__ = ()


_WORKER_EXIT = _WorkerExit()


@dataclass(slots=True)
class _Completed:
    index: int
    value: Any
    exc: BaseException | None


@dataclass(slots=True)
class _Counters:
    #: Incremented by workers as they pick up items; read by the consumer to
    #: derive ``in_flight``. Completed/succeeded/failed are counted consumer-side
    #: so progress snapshots advance one-per-completion instead of in worker-batches.
    started: int = 0


@dataclass(slots=True)
class _Settings(Generic[T, R]):
    func: Callable[[T], Awaitable[R]]
    limit: int
    retries: int
    backoff: Backoff
    retry_on: RetryOn
    timeout: float | None
    rate_limiter: RateLimiter | None
    on_progress: Callable[[Progress], None] | None = field(default=None)


def _validate(limit: int, retries: int, timeout: float | None, rate_limit: float | None) -> None:
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if retries < 0:
        raise ValueError("retries must be >= 0")
    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be > 0")
    if rate_limit is not None and rate_limit <= 0:
        raise ValueError("rate_limit must be > 0")


def _make_settings(
    func: Callable[[T], Awaitable[R]],
    *,
    limit: int,
    retries: int,
    backoff: Backoff | None,
    retry_on: RetryOn,
    timeout: float | None,
    rate_limit: float | None,
    on_progress: Callable[[Progress], None] | None,
) -> _Settings[T, R]:
    _validate(limit, retries, timeout, rate_limit)
    return _Settings(
        func=func,
        limit=limit,
        retries=retries,
        backoff=backoff if backoff is not None else _DEFAULT_BACKOFF,
        retry_on=retry_on,
        timeout=timeout,
        rate_limiter=RateLimiter(rate_limit) if rate_limit is not None else None,
        on_progress=on_progress,
    )


def _len_or_none(items: Iterable[T] | AsyncIterable[T]) -> int | None:
    return len(items) if isinstance(items, Sized) else None


async def _aiter(items: Iterable[T] | AsyncIterable[T]) -> AsyncIterator[T]:
    if isinstance(items, AsyncIterable):
        async for item in items:
            yield item
    else:
        for item in items:
            yield item


def _should_retry(exc: BaseException, retry_on: RetryOn) -> bool:
    if isinstance(retry_on, (tuple, type)):
        return isinstance(exc, retry_on)
    return bool(retry_on(exc))


async def _run_one(item: T, s: _Settings[T, R]) -> tuple[Any, BaseException | None]:
    """Run ``func(item)`` with retries, backoff, timeout, and rate limiting."""
    attempt = 0
    while True:
        try:
            if s.rate_limiter is not None:
                await s.rate_limiter.acquire()
            if s.timeout is not None:
                return await asyncio.wait_for(s.func(item), s.timeout), None
            return await s.func(item), None
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            if attempt >= s.retries or not _should_retry(exc, s.retry_on):
                return None, exc
            delay = s.backoff.delay_for(attempt)
            attempt += 1
            if delay > 0:
                await asyncio.sleep(delay)


async def _imap_unordered(
    items: Iterable[T] | AsyncIterable[T],
    s: _Settings[T, R],
) -> AsyncGenerator[_Completed, None]:
    """Yield completions in the order tasks finish (not input order)."""
    start = monotonic()
    total = _len_or_none(items)
    counters = _Counters()
    input_q: asyncio.Queue[tuple[int, T] | None] = asyncio.Queue(maxsize=s.limit)
    output_q: asyncio.Queue[_Completed | _WorkerExit] = asyncio.Queue()
    feeder_error: BaseException | None = None

    async def feeder() -> None:
        nonlocal feeder_error
        index = 0
        try:
            async for item in _aiter(items):
                await input_q.put((index, item))
                index += 1
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            feeder_error = exc
        finally:
            for _ in range(s.limit):
                await input_q.put(None)

    async def worker() -> None:
        while True:
            got = await input_q.get()
            if got is None:
                await output_q.put(_WORKER_EXIT)
                return
            index, item = got
            counters.started += 1
            value, exc = await _run_one(item, s)
            await output_q.put(_Completed(index, value, exc))

    tasks = [asyncio.create_task(feeder())]
    tasks.extend(asyncio.create_task(worker()) for _ in range(s.limit))
    exited = 0
    completed = 0
    succeeded = 0
    failed = 0
    try:
        while exited < s.limit:
            msg = await output_q.get()
            if isinstance(msg, _WorkerExit):
                exited += 1
                continue
            completed += 1
            if msg.exc is None:
                succeeded += 1
            else:
                failed += 1
            if s.on_progress is not None:
                s.on_progress(
                    Progress(
                        completed=completed,
                        total=total,
                        succeeded=succeeded,
                        failed=failed,
                        in_flight=counters.started - completed,
                        elapsed=monotonic() - start,
                    )
                )
            yield msg
        if feeder_error is not None:
            raise feeder_error
    finally:
        for task in tasks:
            task.cancel()
        while not input_q.empty():
            try:
                input_q.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover - defensive
                break
        await asyncio.gather(*tasks, return_exceptions=True)


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


@overload
async def amap(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    limit: int = ...,
    retries: int = ...,
    backoff: Backoff | None = ...,
    retry_on: RetryOn = ...,
    timeout: float | None = ...,
    return_exceptions: Literal[False] = ...,
    rate_limit: float | None = ...,
    on_progress: Callable[[Progress], None] | None = ...,
) -> list[R]: ...


@overload
async def amap(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    limit: int = ...,
    retries: int = ...,
    backoff: Backoff | None = ...,
    retry_on: RetryOn = ...,
    timeout: float | None = ...,
    return_exceptions: Literal[True],
    rate_limit: float | None = ...,
    on_progress: Callable[[Progress], None] | None = ...,
) -> list[R | BaseException]: ...


async def amap(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    limit: int = DEFAULT_LIMIT,
    retries: int = 0,
    backoff: Backoff | None = None,
    retry_on: RetryOn = Exception,
    timeout: float | None = None,
    return_exceptions: bool = False,
    rate_limit: float | None = None,
    on_progress: Callable[[Progress], None] | None = None,
) -> list[Any]:
    """Apply ``func`` to every item concurrently and return results in input order.

    Args:
        func: An async function called once per item.
        items: A sync or async iterable of inputs.
        limit: Maximum number of tasks running at once.
        retries: How many times to retry a failing task (0 disables retries).
        backoff: Delay strategy between retries (defaults to exponential w/ jitter).
        retry_on: Exception type(s) or a predicate selecting which errors retry.
        timeout: Per-attempt timeout in seconds.
        return_exceptions: If true, failures are returned in place of values
            instead of being raised (mirrors ``asyncio.gather``).
        rate_limit: Maximum task starts per second across the whole run.
        on_progress: Callback invoked with a :class:`Progress` snapshot after
            each task finishes.

    Returns:
        A list of results aligned with ``items``. With ``return_exceptions=True``
        the list may contain exception instances.
    """
    s = _make_settings(
        func,
        limit=limit,
        retries=retries,
        backoff=backoff,
        retry_on=retry_on,
        timeout=timeout,
        rate_limit=rate_limit,
        on_progress=on_progress,
    )
    total = _len_or_none(items)
    out: list[Any] = [None] * total if total is not None else []
    buffer: dict[int, Any] = {}
    gen = _imap_unordered(items, s)
    try:
        async for c in gen:
            result: Any
            if c.exc is not None:
                if not return_exceptions:
                    raise c.exc
                result = c.exc
            else:
                result = c.value
            if total is not None:
                out[c.index] = result
            else:
                buffer[c.index] = result
    finally:
        await gen.aclose()
    if total is None:
        out = [buffer[i] for i in range(len(buffer))]
    return out


@overload
def stream(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    limit: int = ...,
    retries: int = ...,
    backoff: Backoff | None = ...,
    retry_on: RetryOn = ...,
    timeout: float | None = ...,
    return_exceptions: Literal[False] = ...,
    rate_limit: float | None = ...,
    on_progress: Callable[[Progress], None] | None = ...,
) -> AsyncIterator[R]: ...


@overload
def stream(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    limit: int = ...,
    retries: int = ...,
    backoff: Backoff | None = ...,
    retry_on: RetryOn = ...,
    timeout: float | None = ...,
    return_exceptions: Literal[True],
    rate_limit: float | None = ...,
    on_progress: Callable[[Progress], None] | None = ...,
) -> AsyncIterator[R | BaseException]: ...


async def stream(
    func: Callable[[T], Awaitable[R]],
    items: Iterable[T] | AsyncIterable[T],
    *,
    limit: int = DEFAULT_LIMIT,
    retries: int = 0,
    backoff: Backoff | None = None,
    retry_on: RetryOn = Exception,
    timeout: float | None = None,
    return_exceptions: bool = False,
    rate_limit: float | None = None,
    on_progress: Callable[[Progress], None] | None = None,
) -> AsyncIterator[Any]:
    """Like :func:`amap`, but yield each result as soon as it is ready.

    Results arrive in completion order (not input order), which lets you react to
    fast tasks without waiting for slow ones. Memory stays bounded even for very
    large or infinite inputs.
    """
    s = _make_settings(
        func,
        limit=limit,
        retries=retries,
        backoff=backoff,
        retry_on=retry_on,
        timeout=timeout,
        rate_limit=rate_limit,
        on_progress=on_progress,
    )
    gen = _imap_unordered(items, s)
    try:
        async for c in gen:
            if c.exc is not None:
                if not return_exceptions:
                    raise c.exc
                yield c.exc
            else:
                yield c.value
    finally:
        await gen.aclose()


@overload
async def gather(
    *coros: Awaitable[T],
    limit: int = ...,
    timeout: float | None = ...,
    rate_limit: float | None = ...,
    return_exceptions: Literal[False] = ...,
    on_progress: Callable[[Progress], None] | None = ...,
) -> list[T]: ...


@overload
async def gather(
    *coros: Awaitable[T],
    limit: int = ...,
    timeout: float | None = ...,
    rate_limit: float | None = ...,
    return_exceptions: Literal[True],
    on_progress: Callable[[Progress], None] | None = ...,
) -> list[T | BaseException]: ...


async def gather(
    *coros: Awaitable[T],
    limit: int = DEFAULT_LIMIT,
    timeout: float | None = None,
    rate_limit: float | None = None,
    return_exceptions: bool = False,
    on_progress: Callable[[Progress], None] | None = None,
) -> list[Any]:
    """A drop-in for :func:`asyncio.gather` with a concurrency ``limit``.

    Awaits the given awaitables with at most ``limit`` running at once, preserving
    result order. On fail-fast (the default), remaining awaitables are cancelled
    and closed so no "coroutine was never awaited" warnings leak.
    """
    pending = list(coros)

    async def _await(c: Awaitable[T]) -> T:
        return await c

    try:
        if return_exceptions:
            return await amap(
                _await,
                pending,
                limit=limit,
                timeout=timeout,
                rate_limit=rate_limit,
                return_exceptions=True,
                on_progress=on_progress,
            )
        return await amap(
            _await,
            pending,
            limit=limit,
            timeout=timeout,
            rate_limit=rate_limit,
            return_exceptions=False,
            on_progress=on_progress,
        )
    finally:
        for c in pending:
            close = getattr(c, "close", None)
            if callable(close):
                close()
