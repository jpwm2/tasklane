"""Progress reporting types."""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["Progress"]


@dataclass(frozen=True, slots=True)
class Progress:
    """A snapshot of progress, passed to an ``on_progress`` callback.

    A new snapshot is produced each time a task finishes (successfully or not).
    """

    completed: int
    """Number of tasks that have finished (succeeded + failed)."""

    total: int | None
    """Total number of tasks, or ``None`` if the input size is not known
    (e.g. an unsized iterator or async iterable)."""

    succeeded: int
    """Number of tasks that finished without raising."""

    failed: int
    """Number of tasks that finished by raising (after exhausting retries)."""

    in_flight: int
    """Number of tasks currently running."""

    elapsed: float
    """Seconds elapsed since the run started (monotonic clock)."""

    @property
    def remaining(self) -> int | None:
        """Tasks not yet completed, or ``None`` if ``total`` is unknown."""
        if self.total is None:
            return None
        return self.total - self.completed

    @property
    def fraction(self) -> float | None:
        """Completion ratio in ``[0, 1]``, or ``None`` if ``total`` is unknown."""
        if self.total is None or self.total == 0:
            return None
        return self.completed / self.total

    @property
    def rate(self) -> float:
        """Average completed tasks per second since the run started."""
        if self.elapsed <= 0:
            return 0.0
        return self.completed / self.elapsed
