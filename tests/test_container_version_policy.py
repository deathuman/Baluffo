from __future__ import annotations

import subprocess
from pathlib import Path

import yaml  # type: ignore[import-untyped]  # tools/ is outside mypy's file set; this test parses workflow YAML

from tools.repo_health.container_version_policy import (
    NON_SHIPPED_PATTERNS,
    ROOT,
    WindowCommit,
    _declared_release_tag_versions,
    _is_shipped_path,
    check_container_shipped_code_version_gate,
    evaluate_window,
)


def _run(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )


def _init_git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _run(repo, "init", "-q", "-b", "main")
    _run(repo, "config", "user.email", "test@example.com")
    _run(repo, "config", "user.name", "Test Runner")
    return repo


def _write_version_files(repo: Path, version: str) -> None:
    (repo / "src").mkdir(parents=True, exist_ok=True)
    (repo / "deathuman-baluffo").mkdir(parents=True, exist_ok=True)
    (repo / "src" / "app_version.py").write_text(f'APP_VERSION = "{version}"\n', encoding="utf-8")
    (repo / "deathuman-baluffo" / "umbrel-app.yml").write_text(
        f'version: "{version}"\n', encoding="utf-8"
    )


def _commit_all(repo: Path, message: str) -> None:
    _run(repo, "add", "-A")
    completed = _run(repo, "commit", "-q", "-m", message)
    assert completed.returncode == 0, completed.stderr


def _commit_bump(repo: Path) -> None:
    _write_version_files(repo, "0.2.140")
    _commit_all(repo, "release(v0.2.140): initial")
    _write_version_files(repo, "0.2.141")
    _commit_all(repo, "release(v0.2.141): bump container version")


def test_is_shipped_path_classifies_docs_vs_code() -> None:
    assert _is_shipped_path("src/jobs/adapters/plugins/static/phapp.py")
    assert _is_shipped_path("deathuman-baluffo/docker-compose.yml")
    assert _is_shipped_path("data/defaults/source-registry-active.seed.json")
    assert _is_shipped_path("Dockerfile")
    assert not _is_shipped_path("docs/RELEASE.md")
    assert not _is_shipped_path("docs/archive/old-plan.md")
    assert not _is_shipped_path("tests/test_foo.py")
    assert not _is_shipped_path("tools/repo_health/repo_guardrails.py")
    assert not _is_shipped_path(".github/workflows/lint.yml")
    assert not _is_shipped_path("README.md")
    assert not _is_shipped_path("release-notes.md")


def test_declared_release_tag_versions_parses_intent_forms() -> None:
    assert _declared_release_tag_versions("feat: x\n\nRelease-tag: v0.2.142") == ["0.2.142"]
    assert _declared_release_tag_versions("feat: x\n\nrelease-tag: 0.2.142") == ["0.2.142"]
    assert _declared_release_tag_versions("release(v0.2.142): ship it") == ["0.2.142"]
    assert _declared_release_tag_versions("release(0.2.142): ship it") == ["0.2.142"]
    assert _declared_release_tag_versions("chore(release): prepare 0.2.142") == ["0.2.142"]
    assert _declared_release_tag_versions("feat: plain change without intent") == []


def _commit(sha: str, subject: str, message: str, files: tuple[str, ...]) -> WindowCommit:
    return WindowCommit(sha=sha, subject=subject, message=message, files=files)


def test_evaluate_window_ignores_no_shipped_commits() -> None:
    commits = [
        _commit("a1b2c3d4", "docs: update guide", "docs: update guide", ("docs/RELEASE.md",))
    ]
    assert evaluate_window(commits, "0.2.141") == []


def test_evaluate_window_ignores_mixed_docs_only_window() -> None:
    commits = [
        _commit("a1b2c3d4", "docs: update", "docs: update", ("docs/RELEASE.md",)),
        _commit("b2c3d4e5", "chore: guardrail", "chore: guardrail", ("tools/repo_health/x.py",)),
    ]
    assert evaluate_window(commits, "0.2.141") == []


def test_evaluate_window_fails_shipped_without_bump_or_intent() -> None:
    commits = [_commit("a1b2c3d4", "feat: adapter", "feat: adapter", ("src/jobs/x.py",))]
    failures = evaluate_window(commits, "0.2.141")
    assert len(failures) == 1
    assert "0.2.141" in failures[0]
    assert "feat: adapter" in failures[0]
    assert "Release-tag" in failures[0]


def test_evaluate_window_passes_with_release_tag_line() -> None:
    commits = [
        _commit(
            "a1b2c3d4",
            "feat: adapter",
            "feat: adapter\n\nRelease-tag: v0.2.142",
            ("src/jobs/x.py",),
        )
    ]
    assert evaluate_window(commits, "0.2.141") == []


def test_evaluate_window_passes_with_release_subject() -> None:
    commits = [
        _commit(
            "a1b2c3d4",
            "release(v0.2.142): adapter",
            "release(v0.2.142): adapter",
            ("src/jobs/x.py",),
        )
    ]
    assert evaluate_window(commits, "0.2.141") == []


def test_evaluate_window_rejects_intent_not_newer_than_current() -> None:
    commits = [
        _commit(
            "a1b2c3d4",
            "feat: adapter",
            "feat: adapter\n\nRelease-tag: v0.2.141",
            ("src/jobs/x.py",),
        )
    ]
    assert len(evaluate_window(commits, "0.2.141")) == 1


def test_gate_fails_shipped_code_after_bump_without_bump_or_intent(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    (repo / "src" / "feature.py").write_text("x = 1\n", encoding="utf-8")
    _commit_all(repo, "feat: new container feature")

    failures = check_container_shipped_code_version_gate(repo)
    assert len(failures) == 1
    assert "0.2.141" in failures[0]
    assert "new container feature" in failures[0]


def test_gate_passes_when_bump_commit_is_the_window_head(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    assert check_container_shipped_code_version_gate(repo) == []


def test_gate_passes_with_release_tag_intent(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    (repo / "src" / "feature.py").write_text("x = 1\n", encoding="utf-8")
    _commit_all(repo, "feat: new container feature\n\nRelease-tag: v0.2.142")

    assert check_container_shipped_code_version_gate(repo) == []


def test_gate_passes_with_release_subject_intent(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    (repo / "src" / "feature.py").write_text("x = 1\n", encoding="utf-8")
    _commit_all(repo, "release(v0.2.142): ship the feature")

    assert check_container_shipped_code_version_gate(repo) == []


def test_gate_ignores_docs_only_window(tmp_path: Path) -> None:
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    docs_dir = repo / "docs"
    docs_dir.mkdir()
    (docs_dir / "RELEASE.md").write_text("# notes\n", encoding="utf-8")
    _commit_all(repo, "docs: update release guide")

    assert check_container_shipped_code_version_gate(repo) == []


def _workflow_paths_ignore_blocks() -> list[tuple[str, ...]]:
    """Return the ``paths-ignore`` lists from the container workflow, per trigger."""
    workflow = ROOT / ".github" / "workflows" / "build-container.yml"
    data = yaml.safe_load(workflow.read_text(encoding="utf-8"))
    # PyYAML parses the YAML 1.1 ``on:`` key as boolean True, not the string "on".
    triggers = data.get("on") or data.get(True)
    return [tuple(triggers[event]["paths-ignore"]) for event in ("push", "pull_request")]


def test_container_workflow_paths_ignore_stays_aligned_with_guardrail() -> None:
    """The workflow republish trigger and the guardrail shipped-path list must not drift."""
    blocks = _workflow_paths_ignore_blocks()
    assert blocks, "expected push + pull_request paths-ignore blocks in build-container.yml"
    expected = tuple(NON_SHIPPED_PATTERNS)
    for block in blocks:
        assert len(block) == len(expected), "duplicate/missing pattern in paths-ignore"
        assert set(block) == set(expected)
        # Preserve order so reviewers can diff the two lists at a glance.
        assert block == expected
