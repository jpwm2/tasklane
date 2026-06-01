"""Retry backoff strategies."""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Literal

__all__ = ["Backoff"]

Mode = Literal["exponential", "linear", "constant"]


@dataclass(frozen=True, slots=True)
class Backoff:
    """Computes the delay between retry attempts.

    The default is exponential backoff with full jitter, which is a sane choice
    for most network-bound work. Use the :meth:`constant` and :meth:`linear`
    constructors for the other common strategies.

    Args:
        base: The base delay in seconds.
        factor: Growth factor for ``"exponential"`` mode.
        max_delay: Upper bound applied to every computed delay.
        jitter: If true, the delay is randomized in ``[0, delay]`` (full
            jitter), which spreads out retries from many callers.
        mode: How the delay grows with the attempt number.
    """

    base: float = 0.1
    factor: float = 2.0
    max_delay: float = 30.0
    jitter: bool = True
    mode: Mode = "exponential"

    def __post_init__(self) -> None:
        if self.base < 0:
            raise ValueError("base must be >= 0")
        if self.factor <= 0:
            raise ValueError("factor must be > 0")
        if self.max_delay < 0:
            raise ValueError("max_delay must be >= 0")

    @classmethod
    def exponential(
        cls,
        base: float = 0.1,
        *,
        factor: float = 2.0,
        max_delay: float = 30.0,
        jitter: bool = True,
    ) -> Backoff:
        """Exponential backoff: ``base * factor ** attempt`` (capped)."""
        return cls(base=base, factor=factor, max_delay=max_delay, jitter=jitter, mode="exponential")

    @classmethod
    def linear(
        cls,
        step: float = 0.1,
        *,
        max_delay: float = 30.0,
        jitter: bool = True,
    ) -> Backoff:
        """Linear backoff: ``step * (attempt + 1)`` (capped)."""
        return cls(base=step, max_delay=max_delay, jitter=jitter, mode="linear")

    @classmethod
    def constant(cls, delay: float = 0.1, *, jitter: bool = False) -> Backoff:
        """Constant delay between every attempt."""
        return cls(base=delay, max_delay=delay, jitter=jitter, mode="constant")

    def delay_for(self, attempt: int) -> float:
        """Return the delay in seconds before retry ``attempt`` (0-indexed).

        ``attempt=0`` is the first retry (i.e. after the initial call failed).
        """
        if attempt < 0:
            raise ValueError("attempt must be >= 0")
        if self.mode == "exponential":
            raw = self.base * (self.factor**attempt)
        elif self.mode == "linear":
            raw = self.base * (attempt + 1)
        else:  # constant
            raw = self.base
        delay = min(raw, self.max_delay)
        if self.jitter:
            delay = random.uniform(0, delay)
        return delay
