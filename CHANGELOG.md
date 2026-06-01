# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-06-02

### Added

- `amap()` — concurrent map with a bounded concurrency limit, returning results
  in input order. Supports sync and async iterables in constant memory.
- `stream()` — async iterator yielding results in completion order.
- `gather()` — a concurrency-limited drop-in for `asyncio.gather`, closing any
  un-awaited coroutines on fail-fast.
- `Lane` — an immutable, reusable bundle of settings.
- Retries with `Backoff` strategies (`exponential`, `linear`, `constant`) and a
  flexible `retry_on` (type, tuple, or predicate).
- Per-task `timeout` and per-second `rate_limit`.
- `Progress` snapshots via an `on_progress` callback.
- `return_exceptions` to collect failures instead of raising.
- Fully typed, `py.typed`, zero runtime dependencies, Python 3.10–3.14.

### Fixed

- `timeout` always surfaces the builtin `TimeoutError`. On Python 3.10,
  `asyncio.wait_for` raises the distinct `asyncio.TimeoutError` (the two types
  were unified in 3.11), so `except TimeoutError` and `retry_on=TimeoutError`
  now behave consistently across Python 3.10–3.14.

[Unreleased]: https://github.com/jpwm2/tasklane/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jpwm2/tasklane/releases/tag/v0.1.0
