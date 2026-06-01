# Contributing to tasklane

Thanks for your interest in improving `tasklane`! This project aims to be small,
correct, and a pleasure to depend on, so contributions of all sizes are welcome —
bug reports, docs, tests, and features alike.

## Development setup

`tasklane` uses [uv](https://docs.astral.sh/uv/) for environment management.

```bash
git clone https://github.com/jpwm2/tasklane
cd tasklane
uv sync
```

## The checks that CI runs

Please make sure these pass locally before opening a pull request:

```bash
uv run pytest                 # test suite
uv run ruff check .           # lint
uv run ruff format --check .  # formatting
uv run mypy                   # static types (strict)
```

To auto-fix formatting and lint:

```bash
uv run ruff format .
uv run ruff check --fix .
```

## Automated review

Pull requests opened from this repository receive an **advisory** Codex review
that posts a single sticky comment (it only runs when a maintainer has configured
the `OPENAI_API_KEY` secret, so it never runs on fork PRs). It is a helper, not a
gate — a green CI run is what's required to merge.

## Guidelines

- **Keep the runtime dependency-free.** A core promise of `tasklane` is zero
  third-party runtime dependencies. Dev/test dependencies are fine.
- **Stay typed.** The codebase passes `mypy --strict`. New public API needs full
  type annotations.
- **Add tests.** Bug fixes should come with a regression test; features with
  tests covering the happy path and the obvious edge cases.
- **Update the docs.** If you change behavior, update the README and docstrings.
- **Add a changelog entry** under the `Unreleased` heading in
  [CHANGELOG.md](CHANGELOG.md).

## Reporting bugs and requesting features

Open an issue using one of the templates. For bugs, a minimal reproducible
example and your Python version go a long way.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating, you are expected to uphold it.
