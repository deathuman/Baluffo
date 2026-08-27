import subprocess
import sys
from pathlib import Path

import pytest

from src.app_version import get_app_version
from tools.repo_health.release_docs_policy import (
    test_changelog_keeps_unreleased_above_versioned_rollup,
    test_release_notes_artifact_matches_current_version,
)


def test_release_notes_extractor_uses_top_changelog_section(
    repo_root: Path, tmp_path: Path
) -> None:
    script_path = repo_root / "scripts" / "extract_release_notes.py"
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    output_path = tmp_path / "release-notes.md"
    app_version = get_app_version()

    completed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            str(script_path),
            "--version",
            app_version,
            "--changelog",
            str(changelog_path),
            "--output",
            str(output_path),
        ],
        cwd=repo_root,
        capture_output=True,
        check=True,
        text=True,
    )

    extracted = output_path.read_text(encoding="utf-8")
    assert str(output_path) in completed.stdout
    assert extracted.startswith(f"## [{app_version}] - ")
    assert "## [Unreleased]" not in extracted


def test_release_notes_guardrail_accepts_current_version(repo_root: Path) -> None:
    """The tracked release-notes.md must be regenerated for the current app version."""
    release_notes_path = repo_root / "release-notes.md"
    assert release_notes_path.is_file(), "test precondition: release-notes.md is tracked"
    test_release_notes_artifact_matches_current_version(repo_root)  # noqa: S101 - raises on stale


def test_changelog_unreleased_guardrail_accepts_current_tree(repo_root: Path) -> None:
    """The tracked changelog keeps an `[Unreleased]` section above the versioned rollup."""
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    assert changelog_path.is_file(), "test precondition: docs/CHANGELOG.md is tracked"
    test_changelog_keeps_unreleased_above_versioned_rollup(repo_root)  # noqa: S101 - raises on drift


def test_changelog_unreleased_guardrail_rejects_missing_unreleased(repo_root: Path) -> None:
    """A changelog with no `[Unreleased]` heading must fail the docs guardrail."""
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    original = changelog_path.read_bytes()
    app_version = get_app_version()
    try:
        payload = (
            f"# Changelog\n\n---\n\n## [{app_version}] - 2026-08-18\n\n### Added\n\n- nothing\n"
        )
        changelog_path.write_bytes(payload.encode("utf-8"))
        with pytest.raises(AssertionError, match="missing the `## \\[Unreleased\\]` section"):
            test_changelog_keeps_unreleased_above_versioned_rollup(repo_root)
    finally:
        changelog_path.write_bytes(original)


def test_changelog_unreleased_guardrail_rejects_unreleased_below_rollup(repo_root: Path) -> None:
    """An `[Unreleased]` heading below the versioned rollup must fail the docs guardrail."""
    changelog_path = repo_root / "docs" / "CHANGELOG.md"
    original = changelog_path.read_bytes()
    app_version = get_app_version()
    try:
        payload = f"# Changelog\n\n---\n\n## [{app_version}] - 2026-08-18\n\n### Added\n\n- nothing\n\n---\n\n## [Unreleased]\n\n"
        changelog_path.write_bytes(payload.encode("utf-8"))
        with pytest.raises(AssertionError, match="must sit above the versioned rollup"):
            test_changelog_keeps_unreleased_above_versioned_rollup(repo_root)
    finally:
        changelog_path.write_bytes(original)


def test_release_notes_guardrail_rejects_stale_version(repo_root: Path, tmp_path: Path) -> None:
    """A version bump without regenerating release-notes.md must fail the docs guardrail."""
    release_notes_path = repo_root / "release-notes.md"
    original = release_notes_path.read_bytes()
    try:
        release_notes_path.write_bytes(b"## [0.0.0] - 1970-01-01\n\n> stale placeholder\n")
        with pytest.raises(AssertionError, match="release-notes.md is stale"):
            test_release_notes_artifact_matches_current_version(repo_root)
    finally:
        release_notes_path.write_bytes(original)


def test_tracked_docs_restore_is_byte_identical(repo_root: Path) -> None:
    """Rewriting and restoring tracked docs must preserve on-disk bytes exactly."""
    for rel_path in ("docs/CHANGELOG.md", "release-notes.md"):
        path = repo_root / rel_path
        before = path.read_bytes()
        try:
            path.write_bytes(b"## [0.0.0] - 1970-01-01\n\n> stale placeholder\n")
        finally:
            path.write_bytes(before)
        assert path.read_bytes() == before
