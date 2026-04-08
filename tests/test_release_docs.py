import json
from pathlib import Path


def _section(text: str, heading: str) -> str:
    start = text.index(heading)
    next_heading = text.find("\n## ", start + len(heading))
    return text[start:] if next_heading == -1 else text[start:next_heading]


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


def test_ai_bootstrap_sequence_is_single_path(repo_root: Path) -> None:
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    guide_text = (repo_root / "docs" / "AI_ASSISTANT_GUIDE.md").read_text(encoding="utf-8")

    sequence = _section(index_text, "## Recommended AI read sequence")
    read_order = _section(guide_text, "## Read order")

    assert "AI_ASSISTANT_GUIDE.md" in sequence
    assert "architecture-ai-map.md" in sequence
    assert "testing.md" in sequence
    assert "README.md" not in sequence
    assert "../AGENTS.md" not in sequence

    assert "README.md" not in read_order
    assert "architecture-ai-map.md" in read_order
    assert "AGENTS.md" in read_order


def test_ai_docs_use_exact_frontend_syntax_example(repo_root: Path) -> None:
    ai_text = (repo_root / "docs" / "AI_ASSISTANT_GUIDE.md").read_text(encoding="utf-8")
    map_text = (repo_root / "docs" / "architecture-ai-map.md").read_text(encoding="utf-8")

    for text in (ai_text, map_text):
        assert "node --check frontend/*/app.js" not in text
        assert "node --check frontend/jobs/app.js" in text


def test_readme_is_product_overview_not_ai_entrypoint(repo_root: Path) -> None:
    text = (repo_root / "README.md").read_text(encoding="utf-8")

    assert "primary entry point for AI coders" not in text
    assert "scan-first guide for AI-assisted coding" not in text
    assert "AI coding workflow and edit routing" in text


def test_package_json_build_aliases_use_leaf_builders(repo_root: Path) -> None:
    package_path = repo_root / "package.json"
    package = json.loads(package_path.read_text(encoding="utf-8"))
    scripts = package["scripts"]

    assert scripts["build:ship-bundle"] == (
        "npm run check:python-version && python scripts/build_ship_bundle.py"
    )
    assert scripts["build:portable-exe"] == (
        "npm run check:python-version && python scripts/build_portable_exe.py"
    )
    assert "verify:portable" not in scripts


def test_release_and_setup_docs_use_canonical_packaged_smoke_commands(repo_root: Path) -> None:
    release_text = (repo_root / "docs" / "RELEASE.md").read_text(encoding="utf-8")
    local_setup_text = (repo_root / "docs" / "LOCAL_SETUP.md").read_text(encoding="utf-8")

    assert "python scripts/packaged_desktop_smoke.py --rebuild" not in release_text
    assert "npm run probe:desktop:startup:cold" in release_text
    assert "npm run probe:desktop:startup:warm" in release_text
    assert "python scripts/packaged_desktop_smoke.py --rebuild" not in local_setup_text


def test_testing_doc_owns_verification_matrix(repo_root: Path) -> None:
    testing_text = (repo_root / "docs" / "testing.md").read_text(encoding="utf-8")

    for command in (
        "npm run build:ship-bundle",
        "npm run build:portable-exe",
        "python scripts/build_ship_bundle.py --bundle-version <version>",
        "python scripts/build_portable_exe.py --bundle-version <version>",
        "npm run test:frontend:packaged",
        "npm run probe:desktop:startup:cold",
    ):
        assert command in testing_text

    assert "This document owns the verification matrix for Baluffo." in testing_text
    assert "For the AI bootstrap and task-routing summary" in testing_text
