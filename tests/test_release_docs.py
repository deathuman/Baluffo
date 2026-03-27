from pathlib import Path


def test_release_guide_is_canonical_single_source(repo_root: Path) -> None:
    """Ensure `docs/RELEASE.md` remains the canonical release guide."""
    release_path = repo_root / "docs" / "RELEASE.md"
    assert release_path.is_file(), "docs/RELEASE.md must exist as the canonical release guide."
    text = release_path.read_text(encoding="utf-8").strip()
    assert len(text.splitlines()) > 0, "docs/RELEASE.md should not be empty."
