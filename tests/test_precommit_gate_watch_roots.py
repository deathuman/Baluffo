"""The commit gate's path filter decides which guardrail groups a commit pays for.

These live beside `test_precommit_gate.py` rather than inside it: that file is at
its 403-line ceiling in `tools/repo_health/test_line_budget_baseline.json`, and
the ceiling exists to stop it growing further.
"""

from __future__ import annotations

from scripts import precommit_gate


def test_hook_changes_take_the_full_repo_guardrail_set() -> None:
    """A `.githooks/` edit must reach the group that asserts on hooks.

    The hook assertions -- release-tag gating, the no-backticks-in-a-case-branch
    guard, and the timed-lint shape check -- all read `.githooks/*` and live in
    `repo guardrails: workflow`. The path sat outside the watched roots, so a
    hook-only commit took the release-only fallback and could land a hook the
    push gate had never judged.
    """
    assert precommit_gate.should_run_repo_guardrails([".githooks/pre-push"]) is True


def test_unwatched_shipped_paths_still_take_the_release_only_fallback() -> None:
    """The watched roots are a cost filter, not the set of shipped paths.

    `Dockerfile` ships but is deliberately outside them, so it must keep taking
    the release-only branch that still runs the container version gate every
    commit needs.
    """
    assert precommit_gate.should_run_repo_guardrails(["Dockerfile"]) is False
