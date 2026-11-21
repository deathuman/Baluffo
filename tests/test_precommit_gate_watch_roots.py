"""The commit gate's path filter decides which guardrail groups a commit pays for.

These live beside `test_precommit_gate.py` rather than inside it: that file is at
its 403-line ceiling in `tools/repo_health/test_line_budget_baseline.json`, and
the ceiling exists to stop it growing further.
"""

from __future__ import annotations

from scripts import precommit_gate
from tools.repo_health.container_version_policy import is_shipped_path


def test_hook_changes_take_the_full_repo_guardrail_set() -> None:
    """A `.githooks/` edit must reach the group that asserts on hooks.

    The hook assertions -- release-tag gating, the no-backticks-in-a-case-branch
    guard, and the timed-lint shape check -- all read `.githooks/*` and live in
    `repo guardrails: workflow`. The policy classifies the path as shipped, so a
    hook-only commit reaches that group instead of the release-only fallback.
    """
    assert precommit_gate.should_run_repo_guardrails([".githooks/pre-push"]) is True


def test_shipped_paths_outside_the_old_watched_roots_take_the_full_set() -> None:
    """Shippedness comes from NON_SHIPPED_PATTERNS, not a hand-maintained list.

    `styles/`, `Dockerfile`, and the runtime `data/` files all ship but sat
    outside the watched roots, so a commit touching only one of them skipped
    every group but `release`.
    """
    shipped = (
        "styles/admin.css",
        "Dockerfile",
        "requirements.txt",
        ".dockerignore",
        "data/social-sources-config.json",
        "data/defaults/source-registry-active.seed.json",
        ".gitattributes",
        "src/jobs/common/http.py",
    )
    for path in shipped:
        assert precommit_gate.should_run_repo_guardrails([path]) is True, path


def test_non_shipped_paths_that_own_a_group_keep_the_full_set() -> None:
    """Docs, tests, CI config, the root manifests, and the root release/tool files."""
    judged = (
        "docs/RELEASE.md",
        "tests/test_precommit_gate.py",
        ".github/workflows/lint.yml",
        "AGENTS.md",
        "CONTRIBUTING.md",
        "README.md",
        "opencode.json",
        "package.json",
        "package-lock.json",
        "release-notes.md",
    )
    for path in judged:
        assert precommit_gate.should_run_repo_guardrails([path]) is True, path


def test_policy_module_changes_take_the_full_repo_guardrail_set() -> None:
    """A `tools/repo_health/` edit is judged by the group that runs its checks.

    `repo guardrails: workflow` executes the `policy_wiring_policy` checks defined
    in `tools/repo_health/`, so a policy-module edit can change that group's
    verdict. The container policy calls `tools/` non-shipped, so without the
    explicit root the commit would run only `release` and never reach them.
    """
    policy_modules = (
        "tools/repo_health/policy_wiring_policy.py",
        "tools/repo_health/repo_guardrails.py",
        "tools/repo_health/container_version_policy.py",
    )
    for path in policy_modules:
        assert precommit_gate.should_run_repo_guardrails([path]) is True, path


def test_non_shipped_paths_no_group_reads_keep_the_release_only_fallback() -> None:
    """The policy's other exceptions do not pay for sixteen unrelated groups.

    The release group still runs the container version gate every commit needs.
    """
    unjudged = (
        "memory/notes.md",
        ".agents/STATE.md",
        "LICENSE",
        "scripts/bump_version.py",
        "data/adapter-audit-report.md",
    )
    for path in unjudged:
        assert precommit_gate.should_run_repo_guardrails([path]) is False, path


def test_judged_entries_are_only_paths_the_policy_calls_non_shipped() -> None:
    """An entry the policy already calls shipped would be dead weight.

    The list closes gaps in the policy's non-shipped classification, so every
    entry must be a gap. If the policy later classifies one as shipped, this
    fails and the entry comes out instead of silently doing nothing.
    """
    entries = (
        *precommit_gate._GUARDRAIL_JUDGED_NON_SHIPPED_ROOTS,
        *precommit_gate._GUARDRAIL_JUDGED_NON_SHIPPED_FILES,
    )
    shipped = [entry for entry in entries if is_shipped_path(entry)]
    assert shipped == []


def test_a_mixed_commit_takes_the_full_set_when_any_path_needs_it() -> None:
    assert (
        precommit_gate.should_run_repo_guardrails(["memory/notes.md", "styles/admin.css"]) is True
    )
