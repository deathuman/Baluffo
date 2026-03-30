from pathlib import Path


def test_release_guide_is_canonical_single_source(repo_root: Path) -> None:
    """Ensure `docs/RELEASE.md` remains the canonical release guide."""
    release_path = repo_root / "docs" / "RELEASE.md"
    assert release_path.is_file(), "docs/RELEASE.md must exist as the canonical release guide."
    text = release_path.read_text(encoding="utf-8").strip()
    assert len(text.splitlines()) > 0, "docs/RELEASE.md should not be empty."


def test_release_docs_stay_on_the_public_0_0_x_line(repo_root: Path) -> None:
    docs_dir = repo_root / "docs"
    release_text = (docs_dir / "RELEASE.md").read_text(encoding="utf-8")
    changelog_text = (docs_dir / "CHANGELOG.md").read_text(encoding="utf-8")

    assert "src/app_version.py" in release_text
    assert "v<app_version>" in release_text
    assert "0.0.15" in changelog_text
    assert "## [1.3.0]" not in changelog_text
    assert "[1.3.0] — 2026-03-22" not in changelog_text


def test_local_setup_examples_use_placeholder_version(repo_root: Path) -> None:
    local_setup_path = repo_root / "docs" / "LOCAL_SETUP.md"
    text = local_setup_path.read_text(encoding="utf-8")

    assert "--bundle-version <version>" in text
    assert "dist/baluffo-portable-<version>.zip" in text
    assert "1.2.3" not in text
