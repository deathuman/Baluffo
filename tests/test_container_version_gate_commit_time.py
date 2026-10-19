"""The container version gate must fire on the commit that causes the problem.

The window check can only see commits that already exist, so at commit time it
always passed for the commit being created. The pre-commit hook also skipped the
gate outright, so a shipped change landed on a released version and was only
flagged afterwards -- by which point the only remedies were a new version or a
rewritten history. These tests pin the index-scoped check and its wiring: the
staged paths are exactly what the next commit adds, and the staged version is
what it would carry.
"""

from __future__ import annotations

from pathlib import Path

from scripts import precommit_gate
from tests.test_container_version_policy import (
    _commit_bump,
    _init_git_repo,
    _run,
    _write_version_files,
)
from tools.repo_health import repo_guardrails
from tools.repo_health.container_version_policy import (
    check_pending_shipped_code_version_gate,
)


def _stage_all(repo: Path) -> None:
    completed = _run(repo, "add", "-A")
    assert completed.returncode == 0, completed.stderr


def test_pending_gate_blocks_staged_shipped_code_on_a_released_version(tmp_path: Path) -> None:
    """The commit that republishes a live tag must die before it exists."""
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    _run(repo, "tag", "v0.2.141")
    (repo / "src" / "feature.py").write_text("x = 1\n", encoding="utf-8")
    _stage_all(repo)

    failures = check_pending_shipped_code_version_gate(repo)
    assert failures, "staged shipped code against a released version must be blocked"
    assert "src/feature.py" in failures[0]
    assert "v0.2.141" in failures[0]
    assert "Bump the version" in failures[0]


def test_pending_gate_passes_when_the_bump_is_staged_in_the_same_commit(
    tmp_path: Path,
) -> None:
    """A bump staged alongside the code is the release the gate asks for."""
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    _run(repo, "tag", "v0.2.141")
    (repo / "src" / "feature.py").write_text("x = 1\n", encoding="utf-8")
    _write_version_files(repo, "0.2.142")
    _stage_all(repo)

    assert check_pending_shipped_code_version_gate(repo) == []


def test_pending_gate_ignores_work_that_does_not_touch_the_image(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    _run(repo, "tag", "v0.2.141")
    (repo / "docs").mkdir()
    (repo / "docs" / "plan.md").write_text("notes\n", encoding="utf-8")
    _stage_all(repo)

    assert check_pending_shipped_code_version_gate(repo) == []


def test_pending_gate_ignores_unreleased_versions(tmp_path: Path) -> None:
    """Before the tag, republishing the bumped version is inert, so it stays allowed."""
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    (repo / "src" / "feature.py").write_text("x = 1\n", encoding="utf-8")
    _stage_all(repo)

    assert check_pending_shipped_code_version_gate(repo) == []


def test_pending_gate_ignores_unstaged_work(tmp_path: Path) -> None:
    """Only the index becomes the commit, so a dirty worktree alone must not block."""
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    _run(repo, "tag", "v0.2.141")
    (repo / "src" / "feature.py").write_text("x = 1\n", encoding="utf-8")

    assert check_pending_shipped_code_version_gate(repo) == []


def test_pending_gate_is_quiet_outside_a_repository(tmp_path: Path) -> None:
    assert check_pending_shipped_code_version_gate(tmp_path) == []


def test_release_group_checks_the_pending_commit_from_the_commit_hook(monkeypatch) -> None:
    """Selecting the window at commit time was the hole: it cannot see the commit."""
    for name in (
        "check_ship_bundle_embedded_version",
        "check_portable_zip_embedded_version",
        "check_desktop_update_manifest_version",
    ):
        monkeypatch.setattr(repo_guardrails, name, lambda: [])
    monkeypatch.setattr(
        repo_guardrails, "check_pending_shipped_code_version_gate", lambda: ["pending"]
    )
    monkeypatch.setattr(
        repo_guardrails, "check_container_shipped_code_version_gate", lambda: ["window"]
    )
    monkeypatch.setattr(repo_guardrails, "_PENDING_CONTAINER_VERSION_GATE", True)

    assert repo_guardrails.run_release_group() == [
        repo_guardrails.GuardFailure(
            "release", "check_pending_shipped_code_version_gate", "pending"
        )
    ]


def test_release_group_checks_the_window_at_push_time(monkeypatch) -> None:
    for name in (
        "check_ship_bundle_embedded_version",
        "check_portable_zip_embedded_version",
        "check_desktop_update_manifest_version",
    ):
        monkeypatch.setattr(repo_guardrails, name, lambda: [])
    monkeypatch.setattr(
        repo_guardrails, "check_pending_shipped_code_version_gate", lambda: ["pending"]
    )
    monkeypatch.setattr(
        repo_guardrails, "check_container_shipped_code_version_gate", lambda: ["window"]
    )
    monkeypatch.setattr(repo_guardrails, "_PENDING_CONTAINER_VERSION_GATE", False)

    assert repo_guardrails.run_release_group() == [
        repo_guardrails.GuardFailure(
            "release", "check_container_shipped_code_version_gate", "window"
        )
    ]


def test_commit_gate_runs_the_pending_check_for_unwatched_shipped_paths(
    monkeypatch,
) -> None:
    """`styles/`, `Dockerfile`, and runtime `data/` sit outside the watched roots.

    The watched-root filter keeps the full guardrail run cheap, but it does not
    cover every shipped path, so a commit touching only those skipped the
    container version gate entirely -- the 2026-10-01 CSS deletion was one.
    """
    calls: list[tuple[tuple[str, ...], bool]] = []

    def fake_guardrails(
        groups: tuple[str, ...] = (), *, pending_container_version_gate: bool = False
    ) -> int:
        calls.append((groups, pending_container_version_gate))
        return 1

    monkeypatch.setattr(precommit_gate, "collect_changed_files", lambda: ["styles/admin.css"])
    monkeypatch.setattr(precommit_gate, "_run_precommit_command", lambda command: 0)
    monkeypatch.setattr(precommit_gate, "should_run_repo_guardrails", lambda files: False)
    monkeypatch.setattr(precommit_gate, "has_staged_changes", lambda: True)
    monkeypatch.setattr(precommit_gate, "run_repo_guardrails", fake_guardrails)

    assert precommit_gate.run_changed() == 1
    assert calls == [(("release",), True)]
