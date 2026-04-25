import subprocess
import sys
from pathlib import Path

from src.app_version import get_app_version


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
