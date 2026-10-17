"""The lint gate must be scheduled for stacked pull requests, not only main-targeted ones."""

from __future__ import annotations

from pathlib import Path

from tools.repo_health import workflow_policy


def _repo_with_lint_trigger(tmp_path: Path, pull_request_block: str) -> Path:
    workflow = tmp_path / ".github" / "workflows" / "lint.yml"
    workflow.parent.mkdir(parents=True)
    workflow.write_text(
        f"name: Lint\n\non:\n  push:\n    branches: [main]\n  {pull_request_block}\n",
        encoding="utf-8",
    )
    return tmp_path


def test_repository_lint_workflow_runs_for_stacked_pull_requests(repo_root: Path) -> None:
    workflow_policy.test_lint_workflow_runs_for_every_pull_request(repo_root)


def test_main_only_pull_request_filter_is_rejected(tmp_path: Path) -> None:
    """The regression this pins: a base-branch filter silently unschedules the gate."""
    repo = _repo_with_lint_trigger(tmp_path, "pull_request:\n    branches: [main]")
    try:
        workflow_policy.test_lint_workflow_runs_for_every_pull_request(repo)
    except AssertionError as exc:
        assert "stacked PR" in str(exc)
    else:
        raise AssertionError("a main-only pull_request filter must fail the policy")
