#!/usr/bin/env python3
"""Post an advisory AI code review on the current pull request.

Self-contained: standard library only, no third-party packages or actions. The
workflow runs this only when ``OPENAI_API_KEY`` is configured, so it never runs
for pull requests from forks (which cannot see repository secrets). The review is
advisory and this script always exits 0 — it must never block a merge.

Environment:
  OPENAI_API_KEY     required; enables the review
  OPENAI_MODEL       optional, default "gpt-5-codex" (override via repo variable)
  OPENAI_BASE_URL    optional, default "https://api.openai.com/v1"
  GITHUB_TOKEN       required to post the PR comment
  GITHUB_REPOSITORY  "owner/repo" (provided by Actions)
  GITHUB_STEP_SUMMARY optional; the review is also written here when set
  PR_NUMBER          the pull request number
  DIFF_FILE          path to a file holding the unified diff (else read stdin)
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

MARKER = "<!-- tasklane-ai-review -->"
MAX_DIFF_CHARS = 60_000

SYSTEM_PROMPT = """\
You are an experienced Python reviewer for `tasklane`, a zero-dependency asyncio
concurrency library (bounded amap/stream/gather with retries, timeouts, rate
limiting, and progress reporting). Review the unified diff supplied by the user.

Prioritise, in order:
- Correctness and asyncio pitfalls: cancellation handling, task/coroutine leaks,
  cleanup on the error path, deadlocks, backpressure, and ordering guarantees.
- Public API clarity and type correctness (the project is `mypy --strict`, fully typed).
- Test coverage for new behaviour and edge cases.
- Docs / CHANGELOG drift when observable behaviour changes.

Be concise and specific; use a short bulleted list and reference symbols or files.
Only raise real issues. If the change looks solid, say so in one line. Do not
comment on formatting — ruff enforces it."""


def _request(url: str, token: str, *, accept: str, payload: dict | None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method="POST" if data else "GET")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", accept)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.load(resp)


def _read_diff() -> str:
    path = os.environ.get("DIFF_FILE")
    text = ""
    if path and os.path.exists(path):
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
    else:
        text = sys.stdin.read()
    text = text.strip()
    if len(text) > MAX_DIFF_CHARS:
        text = text[:MAX_DIFF_CHARS] + "\n\n[diff truncated for length]"
    return text


def _generate_review(diff: str) -> str:
    api_key = os.environ["OPENAI_API_KEY"]
    base = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    model = os.environ.get("OPENAI_MODEL", "gpt-5-codex")
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Review this pull request diff:\n\n{diff}"},
        ],
    }
    result = _request(
        f"{base}/chat/completions",
        api_key,
        accept="application/json",
        payload=payload,
    )
    return result["choices"][0]["message"]["content"].strip()


def _find_existing_comment(repo: str, pr: str, token: str) -> int | None:
    url = f"https://api.github.com/repos/{repo}/issues/{pr}/comments?per_page=100"
    comments = _request(url, token, accept="application/vnd.github+json", payload=None)
    for comment in comments:
        if MARKER in comment.get("body", ""):
            return int(comment["id"])
    return None


def _upsert_comment(repo: str, pr: str, token: str, body: str) -> None:
    existing = _find_existing_comment(repo, pr, token)
    payload = {"body": body}
    if existing is not None:
        url = f"https://api.github.com/repos/{repo}/issues/comments/{existing}"
        # PATCH to update the sticky comment in place.
        data = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=data, method="PATCH")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Accept", "application/vnd.github+json")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=60):
            pass
    else:
        url = f"https://api.github.com/repos/{repo}/issues/{pr}/comments"
        _request(url, token, accept="application/vnd.github+json", payload=payload)


def main() -> int:
    diff = _read_diff()
    if not diff:
        print("No diff to review; nothing to do.")
        return 0
    try:
        review = _generate_review(diff)
    except (urllib.error.URLError, KeyError, OSError) as exc:
        print(f"AI review skipped (model call failed): {exc}", file=sys.stderr)
        return 0

    body = f"{MARKER}\n## 🤖 Codex review\n\n{review}\n\n_Advisory only — not a merge gate._"

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as fh:
            fh.write(body + "\n")

    repo = os.environ.get("GITHUB_REPOSITORY")
    pr = os.environ.get("PR_NUMBER")
    token = os.environ.get("GITHUB_TOKEN")
    if not (repo and pr and token):
        print("Missing GitHub context; printing review instead of posting.\n")
        print(review)
        return 0
    try:
        _upsert_comment(repo, pr, token, body)
        print("Posted Codex review comment.")
    except (urllib.error.URLError, OSError) as exc:
        print(f"Could not post comment: {exc}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
