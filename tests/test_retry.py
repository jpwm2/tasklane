from __future__ import annotations

import pytest

from tasklane import Backoff, amap


async def test_retries_until_success() -> None:
    attempts = {"n": 0}

    async def flaky(x: int) -> int:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ValueError("transient")
        return x

    out = await amap(flaky, [42], retries=5, backoff=Backoff.constant(0))
    assert out == [42]
    assert attempts["n"] == 3


async def test_retries_exhausted_raises() -> None:
    async def always_fails(x: int) -> int:
        raise ValueError("permanent")

    with pytest.raises(ValueError, match="permanent"):
        await amap(always_fails, [1], retries=2, backoff=Backoff.constant(0))


async def test_retry_on_type_filters() -> None:
    calls = {"n": 0}

    async def work(x: int) -> int:
        calls["n"] += 1
        raise KeyError("not retried")

    # Only ValueError is retryable, so KeyError fails immediately (1 call).
    with pytest.raises(KeyError):
        await amap(work, [1], retries=5, retry_on=ValueError, backoff=Backoff.constant(0))
    assert calls["n"] == 1


async def test_retry_on_predicate() -> None:
    attempts = {"n": 0}

    def is_retryable(exc: BaseException) -> bool:
        return "retry" in str(exc)

    async def work(x: int) -> int:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("please retry")
        return x

    out = await amap(work, [7], retries=3, retry_on=is_retryable, backoff=Backoff.constant(0))
    assert out == [7]
    assert attempts["n"] == 2


def test_backoff_exponential_math() -> None:
    bo = Backoff.exponential(0.1, factor=2.0, max_delay=1.0, jitter=False)
    assert bo.delay_for(0) == pytest.approx(0.1)
    assert bo.delay_for(1) == pytest.approx(0.2)
    assert bo.delay_for(2) == pytest.approx(0.4)
    assert bo.delay_for(5) == pytest.approx(1.0)  # capped at max_delay


def test_backoff_linear_math() -> None:
    bo = Backoff.linear(0.1, max_delay=10.0, jitter=False)
    assert bo.delay_for(0) == pytest.approx(0.1)
    assert bo.delay_for(1) == pytest.approx(0.2)
    assert bo.delay_for(2) == pytest.approx(0.3)


def test_backoff_constant() -> None:
    bo = Backoff.constant(0.5)
    assert bo.delay_for(0) == pytest.approx(0.5)
    assert bo.delay_for(9) == pytest.approx(0.5)


def test_backoff_jitter_within_bounds() -> None:
    bo = Backoff.exponential(0.1, factor=2.0, max_delay=5.0, jitter=True)
    for attempt in range(6):
        cap = min(0.1 * 2**attempt, 5.0)
        for _ in range(50):
            assert 0.0 <= bo.delay_for(attempt) <= cap


@pytest.mark.parametrize(
    "kwargs",
    [{"base": -1.0}, {"factor": 0.0}, {"max_delay": -1.0}],
)
def test_backoff_validation(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        Backoff(**kwargs)  # type: ignore[arg-type]


def test_backoff_negative_attempt_rejected() -> None:
    with pytest.raises(ValueError):
        Backoff().delay_for(-1)
