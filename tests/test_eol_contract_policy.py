"""The working-tree EOL gate: git's own classification is the reference.

`git status` and `git diff` normalize through the clean filter, so a tracked text
file rewritten with CRLF reports clean while its bytes contradict
`.gitattributes`. These tests build real git repositories because the behaviour
under test *is* git's classification -- a stubbed subprocess would only test the
stub.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from scripts import precommit_gate
from tools.repo_health import eol_contract_policy, repo_guardrails

LF_ATTRIBUTES = "* text=auto eol=lf\n"


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=EOL Contract Test",
            "-c",
            "user.email=eol-contract@example.invalid",
            "-c",
            "commit.gpgsign=false",
            *args,
        ],
        capture_output=True,
        check=True,
    )


def _repository(tmp_path: Path, attributes: str = LF_ATTRIBUTES) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    if attributes:
        (repo / ".gitattributes").write_bytes(attributes.encode("utf-8"))
    return repo


def _commit_all(repo: Path) -> None:
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "seed")


def test_lf_worktree_satisfies_the_contract(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    (repo / "notes.md").write_bytes(b"one\ntwo\n")
    _commit_all(repo)

    assert eol_contract_policy.check_root_eol_contract(repo) == []
    assert eol_contract_policy.check_worktree_eol_contract(repo) == []


def test_crlf_worktree_is_reported_even_though_git_diff_is_clean(tmp_path: Path) -> None:
    """The drift this gate exists for: content-identical to git, outside the contract."""
    repo = _repository(tmp_path)
    path = repo / "notes.md"
    path.write_bytes(b"one\ntwo\n")
    _commit_all(repo)
    path.write_bytes(b"one\r\ntwo\r\n")

    # `git diff` sees no change: the clean filter normalizes CRLF back to LF.
    assert _git(repo, "diff", "--quiet").returncode == 0

    messages = eol_contract_policy.check_worktree_eol_contract(repo)

    assert len(messages) == 1
    assert "notes.md" in messages[0]
    assert "CRLF line endings" in messages[0]
    assert "eol=lf" in messages[0]


def test_mixed_line_endings_are_reported(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    path = repo / "notes.md"
    path.write_bytes(b"one\ntwo\n")
    _commit_all(repo)
    path.write_bytes(b"one\r\ntwo\n")

    messages = eol_contract_policy.check_worktree_eol_contract(repo)

    assert len(messages) == 1
    assert "mixed LF and CRLF line endings" in messages[0]


def test_binary_and_empty_files_are_outside_the_contract(tmp_path: Path) -> None:
    """Neither is line-ending converted by git, so neither has an EOL to enforce."""
    repo = _repository(tmp_path)
    (repo / "logo.png").write_bytes(b"\x89PNG\r\n\x1a\n\x00\r\n")
    (repo / "empty.txt").write_bytes(b"")
    _commit_all(repo)

    assert eol_contract_policy.check_worktree_eol_contract(repo) == []


def test_untracked_files_are_ignored(tmp_path: Path) -> None:
    repo = _repository(tmp_path)
    (repo / "tracked.md").write_bytes(b"one\n")
    _commit_all(repo)
    (repo / "scratch.md").write_bytes(b"one\r\ntwo\r\n")

    assert eol_contract_policy.check_worktree_eol_contract(repo) == []


def test_per_file_eol_attribute_is_the_reference_not_a_hardcoded_lf(tmp_path: Path) -> None:
    """A `eol=crlf` file with an LF worktree is equally out of contract."""
    repo = _repository(tmp_path, attributes="*.md text=auto eol=crlf\n")
    (repo / "notes.md").write_bytes(b"one\ntwo\n")
    _commit_all(repo)

    messages = eol_contract_policy.check_worktree_eol_contract(repo)

    assert len(messages) == 1
    assert "eol=crlf" in messages[0]
    assert "LF line endings" in messages[0]


def test_missing_root_rule_is_reported_rather_than_checked_vacuously(tmp_path: Path) -> None:
    repo = _repository(tmp_path, attributes="")
    (repo / "notes.md").write_bytes(b"one\n")
    _commit_all(repo)

    messages = eol_contract_policy.check_root_eol_contract(repo)

    assert len(messages) == 1
    assert "eol=lf" in messages[0]
    assert ".gitattributes" in messages[0]


def test_report_is_capped_so_a_global_writer_failure_stays_readable(
    tmp_path: Path, monkeypatch
) -> None:
    records = [(f"file{index}.py", "crlf", "text=auto eol=lf") for index in range(25)]
    monkeypatch.setattr(eol_contract_policy, "_tracked_eol_records", lambda _root: records)

    messages = eol_contract_policy.check_worktree_eol_contract(tmp_path)

    assert len(messages) == eol_contract_policy.MAX_REPORTED_FILES + 1
    assert "file0.py" in messages[0]
    assert "... and 5 more" in messages[-1]


def test_real_repository_satisfies_its_own_eol_contract() -> None:
    """Tripwire for this checkout: the gate itself must start from a clean tree."""
    assert eol_contract_policy.check_root_eol_contract() == []
    assert eol_contract_policy.check_worktree_eol_contract() == []


def test_eol_group_is_registered_and_wired() -> None:
    assert "eol" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["eol"] is repo_guardrails.run_eol_group


def test_eol_group_reports_both_checks(monkeypatch) -> None:
    monkeypatch.setattr(
        repo_guardrails, "check_root_eol_contract", lambda _root: ["root rule missing"]
    )
    monkeypatch.setattr(
        repo_guardrails, "check_worktree_eol_contract", lambda _root: ["notes.md is CRLF"]
    )

    assert repo_guardrails.run_eol_group() == [
        repo_guardrails.GuardFailure("eol", "check_root_eol_contract", "root rule missing"),
        repo_guardrails.GuardFailure("eol", "check_worktree_eol_contract", "notes.md is CRLF"),
    ]


def test_gitattributes_changes_take_the_full_repo_guardrail_set() -> None:
    """A contract edit must reach the `eol` group that reads it."""
    assert precommit_gate.should_run_repo_guardrails([".gitattributes"]) is True
