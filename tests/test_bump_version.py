"""Bump-helper behaviour: the three release artifacts, plus drift detection.

The private app-store metadata is gone, so a bump touches only `src/app_version.py`,
`docs/CHANGELOG.md`, and `dist/baluffo-ship/app/current.txt`. What still has to
hold is that a half-applied earlier bump is reported instead of carried forward.
"""

import sys
from pathlib import Path

import pytest

# --- Release artifacts ----------------------------------------------------


def _load_bump_version(repo_root: Path):
    import importlib

    sys.path.insert(0, str(repo_root / "scripts"))
    try:
        return importlib.import_module("bump_version")
    finally:
        sys.path.pop(0)


def _write_bump_fixture(
    root: Path,
    *,
    app_version: str = "0.3.019",
    changelog_version: str = "0.3.019",
    current_version: str = "0.3.019",
) -> None:
    """A minimal release tree carrying the three version-bearing artifacts."""
    (root / "src").mkdir(parents=True)
    (root / "src" / "app_version.py").write_text(
        f'APP_VERSION = "{app_version}"\n', encoding="utf-8"
    )
    (root / "docs").mkdir()
    (root / "docs" / "CHANGELOG.md").write_text(
        "# Changelog\n\n## [Unreleased]\n\n### Fixed\n\n- pending work\n\n"
        f"## [{changelog_version}] - 2026-10-14\n\n### Fixed\n\n- shipped work\n",
        encoding="utf-8",
    )
    (root / "dist" / "baluffo-ship" / "app").mkdir(parents=True)
    (root / "dist" / "baluffo-ship" / "app" / "current.txt").write_text(
        f"{current_version}\n", encoding="utf-8"
    )


def test_bump_version_moves_every_artifact(
    repo_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The happy path, and no drift on a consistent tree."""
    bump_version = _load_bump_version(repo_root)
    _write_bump_fixture(tmp_path)
    monkeypatch.setattr(bump_version, "ROOT", tmp_path)

    exit_code = bump_version.main(["0.9.900", "--date", "2026-01-02"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "drift" not in output
    assert "skipping" not in output
    assert 'APP_VERSION = "0.9.900"' in (tmp_path / "src" / "app_version.py").read_text(
        encoding="utf-8"
    )
    assert "## [0.9.900] - 2026-01-02" in (tmp_path / "docs" / "CHANGELOG.md").read_text(
        encoding="utf-8"
    )
    assert (tmp_path / "dist" / "baluffo-ship" / "app" / "current.txt").read_text(
        encoding="utf-8"
    ) == "0.9.900\n"


def test_bump_version_stops_on_half_applied_previous_bump(
    repo_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A version that reached only some artifacts is reported, not carried forward."""
    bump_version = _load_bump_version(repo_root)
    _write_bump_fixture(tmp_path, changelog_version="0.3.018", current_version="0.3.018")
    monkeypatch.setattr(bump_version, "ROOT", tmp_path)

    exit_code = bump_version.main(["0.9.900", "--date", "2026-01-02"])
    captured = capsys.readouterr()

    assert exit_code == 2
    assert "version drift detected" in captured.err
    assert "src/app_version.py declares 0.3.019" in captured.err
    assert "docs/CHANGELOG.md: 0.3.018" in captured.err
    assert "dist/baluffo-ship/app/current.txt: 0.3.018" in captured.err
    # Nothing was written: the point is to stop, not to paper over the gap.
    assert 'APP_VERSION = "0.3.019"' in (tmp_path / "src" / "app_version.py").read_text(
        encoding="utf-8"
    )


def test_bump_version_allow_drift_rewrites_every_present_artifact(
    repo_root: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--allow-drift` is the deliberate escape hatch, and it resolves the drift."""
    bump_version = _load_bump_version(repo_root)
    _write_bump_fixture(tmp_path, changelog_version="0.3.018")
    monkeypatch.setattr(bump_version, "ROOT", tmp_path)

    exit_code = bump_version.main(["0.9.900", "--date", "2026-01-02", "--allow-drift"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "version drift detected" in captured.err
    assert 'APP_VERSION = "0.9.900"' in (tmp_path / "src" / "app_version.py").read_text(
        encoding="utf-8"
    )
    assert "## [0.9.900] - 2026-01-02" in (tmp_path / "docs" / "CHANGELOG.md").read_text(
        encoding="utf-8"
    )
    assert (tmp_path / "dist" / "baluffo-ship" / "app" / "current.txt").read_text(
        encoding="utf-8"
    ) == "0.9.900\n"
