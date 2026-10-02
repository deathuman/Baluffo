from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
import yaml

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
    assert not _is_shipped_path("opencode.json")
    assert not _is_shipped_path("README.md")
    assert not _is_shipped_path("release-notes.md")


def test_memory_notes_are_not_shipped_paths() -> None:
    """Continuity notes must not force a container republish.

    ``memory/`` was a shipped path, so committing a memory note re-triggered
    ``Build Container`` and retagged the current version with newer code while
    ``umbrel-app.yml`` still declared the old version -- the 0.2.140 reuse trap.
    """
    assert not _is_shipped_path("memory/platform-improvement-report-2026-08-26.md")
    assert not _is_shipped_path("memory/notes/anything.md")


def test_dockerignore_excludes_memory_notes() -> None:
    """The image must not carry AI continuity notes, which are not runtime inputs."""
    patterns = {
        line.strip()
        for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    }
    assert "memory" in patterns
    assert "memory/**" in patterns


# Directories that are dev/agent tooling and never runtime inputs. Each must be
# BOTH excluded from the image (.dockerignore) and treated as non-shipped, or the
# two drift apart in a way nobody notices until a push is blocked for no reason.
#
# `.agents/` is here because it was missed: skills were shipped into the image by
# `COPY . .` and, until 2026-10-02, an edit to a SKILL.md was classified as
# shipped container code. `memory/` already had this rule and a test; skills are
# the same class and had neither.
DEV_TOOLING_DIRS = ("docs", "tests", "memory", ".agents", ".github")


def _dockerignore_patterns() -> set[str]:
    return {
        line.strip()
        for line in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    }


def _dockerignore_excludes(patterns: set[str], directory: str) -> bool:
    """A bare directory name and a `dir/**` glob both exclude the whole tree in Docker,
    so either form satisfies the intent. `.github` is listed bare; the rest carry both."""
    return directory in patterns or f"{directory}/**" in patterns


@pytest.mark.parametrize("directory", DEV_TOOLING_DIRS)
def test_dev_tooling_dir_is_excluded_from_the_image(directory: str) -> None:
    assert _dockerignore_excludes(_dockerignore_patterns(), directory), (
        f".dockerignore must exclude {directory}/"
    )


@pytest.mark.parametrize("directory", DEV_TOOLING_DIRS)
def test_dev_tooling_dir_is_not_shipped_container_code(directory: str) -> None:
    """An edit here must never republish a released container tag."""
    assert f"{directory}/**" in NON_SHIPPED_PATTERNS


def test_dockerignore_and_nonshipped_agree_on_dev_tooling() -> None:
    """The invariant as one assertion, so a new tooling dir cannot half-register.

    Not a blanket cross-check: the two lists are deliberately not 1:1.
    `package-lock.json` is excluded from neither (the Dockerfile copies it and it
    changes the build), and `tools/**` is non-shipped for edits while most of it
    still enters the image. The rule that holds is narrower -- dev/agent tooling
    must be absent from both.
    """
    patterns = _dockerignore_patterns()
    non_shipped = set(NON_SHIPPED_PATTERNS)
    for directory in DEV_TOOLING_DIRS:
        assert _dockerignore_excludes(patterns, directory), f"{directory} must leave the image"
        assert f"{directory}/**" in non_shipped


# The Node manifest and its lockfile are the same class and were registered
# separately by accident: `package-lock.json` was added, `package.json` was not.
# A Dependabot dev-dependency bump rewrites both, so it was still counted as
# shipped code and still re-triggered Build Container. See
# `test_evaluate_window_allows_dev_dependency_bump_once_released` for the
# consequence, and `test_package_json_declares_no_runtime_dependencies` for the
# invariant that makes excluding the manifest sound at all.
def test_manifest_and_lockfile_classify_together() -> None:
    assert not _is_shipped_path("package.json")
    assert not _is_shipped_path("package-lock.json")


def test_package_json_declares_no_runtime_dependencies() -> None:
    """Keep the blanket `package.json` exclusion honest.

    Dev deps install into the image's build stage only, which is the whole
    reason both files are non-shipped. That reasoning only holds while the
    manifest declares no runtime `dependencies`. Adding one makes the manifest
    shipped code again, so fail here where a re-think is cheap instead of
    mis-classifying silently. `.dockerignore` deliberately keeps both files in
    the image, so they change its digest; the exemption is that a toolchain bump
    should not be allowed to republish a released tag.
    """
    manifest = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    assert not manifest.get("dependencies"), (
        "package.json is non-shipped because it declares no runtime dependencies; "
        "adding some makes it shipped code and it must leave NON_SHIPPED_PATTERNS "
        "and the build-container paths-ignore lists"
    )


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


def test_evaluate_window_rejects_intent_older_than_current() -> None:
    commits = [
        _commit(
            "a1b2c3d4",
            "feat: adapter",
            "feat: adapter\n\nRelease-tag: v0.2.140",
            ("src/jobs/x.py",),
        )
    ]
    assert len(evaluate_window(commits, "0.2.141")) == 1


def test_evaluate_window_accepts_intent_equal_to_current() -> None:
    """A post-bump fix retagged into the same release declares "= current"."""
    commits = [
        _commit(
            "a1b2c3d4",
            "fix: adapter",
            "fix: adapter\n\nRelease-tag: v0.2.141",
            ("src/jobs/x.py",),
        )
    ]
    assert evaluate_window(commits, "0.2.141") == []


def test_evaluate_window_rejects_intent_equal_to_current_once_released() -> None:
    """Once the current version is published, "= current" is no longer permission.

    This is the bug from 2026-10-01: `Release-tag: v<current>` counted as intent
    forever, so a shipped commit could keep declaring the already-published
    version and republish its container tag.
    """
    commits = [
        _commit(
            "b1c2d3e4",
            "refactor(css)",
            "refactor(css)\n\nRelease-tag: v0.2.141",
            ("styles/admin.css",),
        )
    ]
    failures = evaluate_window(commits, "0.2.141", current_released=True)
    assert failures, "a released version must not be republishable via '= current' intent"
    assert "v0.2.141" in failures[0]


def test_evaluate_window_rejects_newer_intent_once_released() -> None:
    """Even a strictly-newer declaration cannot authorise overwriting a live tag.

    Declaring `v0.2.142` is correct bookkeeping for where the change should ship,
    but it does not move the version, so the push would still overwrite the
    published `0.2.141` image. The version has to actually move.
    """
    commits = [
        _commit(
            "c1d2e3f4",
            "refactor(css)",
            "refactor(css)\n\nRelease-tag: v0.2.142",
            ("styles/admin.css",),
        )
    ]
    failures = evaluate_window(commits, "0.2.141", current_released=True)
    assert failures, "a newer Release-tag must not substitute for a version bump"
    assert "Bump the version" in failures[0]


def test_evaluate_window_allows_dev_dependency_bump_once_released() -> None:
    """A Dependabot dev-dep bump must not need a version bump or release intent.

    This is the PR #12 failure. The bump was `eslint` and `knip` in
    `devDependencies`, touching exactly these two files, against the released
    `v0.3.002`. `package-lock.json` was registered as non-shipped but
    `package.json` was not, so the commit counted as shipped code and the
    pre-commit gate failed a linter bump. That in turn would have forced
    spending a release version on toolchain-only changes -- the exact cost the
    lockfile exemption was added to avoid.
    """
    commits = [
        _commit(
            "49b32e15",
            "chore(deps-dev): bump the js-dev-tooling group",
            "chore(deps-dev): bump the js-dev-tooling group",
            ("package.json", "package-lock.json"),
        )
    ]
    assert evaluate_window(commits, "0.3.002", current_released=True) == []


def test_evaluate_window_allows_pre_release_churn_by_default() -> None:
    """Before a release, republishing the bumped version is inert and stays allowed.

    Nobody can hold an unreleased version string, and Umbrel's update check is
    string inequality, so this churn reaches nobody. Blocking it would cost a
    release-window fix for nothing.
    """
    commits = [
        _commit(
            "d1e2f3a4", "fix: bridge", "fix: bridge\n\nRelease-tag: v0.2.141", ("src/bridge/x.py",)
        )
    ]
    assert evaluate_window(commits, "0.2.141") == []


def test_gate_ignores_shipped_commits_already_inside_the_release_tag(tmp_path: Path) -> None:
    """Shipped code that is an ancestor of the release tag is inside the image already."""
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    (repo / "src" / "feature.py").write_text("x = 1\n", encoding="utf-8")
    _commit_all(repo, "feat: shipped before the release")
    _run(repo, "tag", "v0.2.141")

    # No commit after the tag, so nothing can overwrite it.
    assert check_container_shipped_code_version_gate(repo) == []


def test_gate_blocks_shipped_code_after_a_release_even_with_newer_intent(tmp_path: Path) -> None:
    """The end-to-end shape of the 2026-10-01 incident, reproduced."""
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    (repo / "src" / "feature.py").write_text("x = 1\n", encoding="utf-8")
    _commit_all(repo, "feat: part of the release")
    _run(repo, "tag", "v0.2.141")
    (repo / "styles").mkdir(exist_ok=True)
    (repo / "styles" / "admin.css").write_text(".x { color: red }\n", encoding="utf-8")
    _commit_all(repo, "refactor(css): delete dead rules\n\nRelease-tag: v0.2.142")

    failures = check_container_shipped_code_version_gate(repo)
    assert failures, "shipped code after a released version must be blocked"
    assert "v0.2.141" in failures[0]
    assert "Bump the version" in failures[0]


def test_gate_allows_non_shipped_work_after_a_release(tmp_path: Path) -> None:
    """Docs/tools/tests-only commits never touch the image, released or not."""
    repo = _init_git_repo(tmp_path)
    _commit_bump(repo)
    _run(repo, "tag", "v0.2.141")
    (repo / "docs").mkdir(exist_ok=True)
    (repo / "docs" / "plan.md").write_text("notes\n", encoding="utf-8")
    _commit_all(repo, "docs(plans): notes only")

    assert check_container_shipped_code_version_gate(repo) == []


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
