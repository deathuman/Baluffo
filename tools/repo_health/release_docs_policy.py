import json
import subprocess
import sys
from pathlib import Path

from src.app_version import get_app_version


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


def test_release_docs_cover_the_current_public_release_line(repo_root: Path) -> None:
    docs_dir = repo_root / "docs"
    changelog_text = (docs_dir / "CHANGELOG.md").read_text(encoding="utf-8")
    release_text = (docs_dir / "RELEASE.md").read_text(encoding="utf-8")
    app_version = get_app_version()
    top_release = _section(changelog_text, f"## [{app_version}]")
    previous_release = _section(changelog_text, "## [0.1.1]")
    legacy_notes = _section(changelog_text, "## Legacy notes")

    assert "src/app_version.py" in release_text
    assert "v<app_version>" in release_text
    assert "single release-note source of truth" in release_text
    assert "npm run test:frontend:packaged:jobs-pipeline" in release_text
    assert "npm run test:frontend:packaged:sync-rehearsal" in release_text
    assert "npm run test:py:extended" in release_text
    assert "python scripts/extract_release_notes.py" in release_text
    assert f"## [{app_version}]" in changelog_text
    assert top_release.startswith(f"## [{app_version}] - ")
    assert any(
        heading in top_release for heading in ("### Added", "### Changed", "### Fixed", "### Notes")
    )
    assert "\n- " in top_release
    assert "isolated policy and telemetry path" in top_release
    assert "Playwright bridge local data" in top_release
    assert "authoritative ordering for browser launch" in top_release
    assert "Desktop in-app update flow in the Jobs desktop UI" in previous_release
    assert (
        "Location normalization was consolidated into the canonical parsers path"
        in previous_release
    )
    assert (
        "Closing the packaged desktop window now tears down the desktop session cleanly"
        in previous_release
    )
    assert "Desktop portable EXE with PyInstaller" not in legacy_notes
    assert "Ship bundle (zip-first) release channel" not in legacy_notes
    assert "## [Unreleased]" in changelog_text
    assert "Current development" not in changelog_text
    assert "## [1.3.0]" not in changelog_text
    assert "[1.3.0] — 2026-03-22" not in changelog_text


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


def test_local_setup_points_to_canonical_commands_and_docs(repo_root: Path) -> None:
    local_setup_path = repo_root / "docs" / "LOCAL_SETUP.md"
    text = local_setup_path.read_text(encoding="utf-8")

    assert "npm run dev:bridge" in text
    assert "npm run test:py" in text
    assert "admin-bridge-api.md" in text
    assert "RELEASE.md" in text
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
    assert "Historical archive detail is intentionally trimmed" in index_text

    assert "README.md" not in read_order
    assert "DOCS_WORKFLOW.md" not in read_order
    assert "architecture-ai-map.md" in read_order
    assert "AGENTS.md" in read_order
    assert "Do not load archive docs by default." in guide_text
    assert "docs-first, not docs-only" in guide_text
    assert "authoritative only for the surface they declare" in guide_text
    assert "Serena memory and repo docs ever diverge" in guide_text
    assert "docs/wiki/" not in guide_text
    assert "wiki > code" not in guide_text


def test_docs_workflow_is_indexed_and_linked_for_contributors(repo_root: Path) -> None:
    docs_dir = repo_root / "docs"
    workflow_text = (docs_dir / "DOCS_WORKFLOW.md").read_text(encoding="utf-8")
    index_text = (docs_dir / "INDEX.md").read_text(encoding="utf-8")
    contributing_text = (repo_root / "CONTRIBUTING.md").read_text(encoding="utf-8")
    readme_text = (repo_root / "README.md").read_text(encoding="utf-8")

    assert "docs-first, not docs-only" in workflow_text
    assert "checked-in repo targets" in workflow_text
    assert "generated or usually-absent artifact paths such as `_out/`" in workflow_text
    assert "docs/CHANGELOG.md" not in workflow_text
    assert "[`DOCS_WORKFLOW.md`](DOCS_WORKFLOW.md)" in index_text
    assert "Documentation maintenance" in index_text
    assert "docs/DOCS_WORKFLOW.md" in contributing_text
    assert "docs/DOCS_WORKFLOW.md" in readme_text


def test_runtime_and_tool_configs_keep_separate_ownership(repo_root: Path) -> None:
    runtime_config = json.loads((repo_root / "baluffo.config.json").read_text(encoding="utf-8"))
    tool_config = json.loads((repo_root / "opencode.json").read_text(encoding="utf-8"))
    data_contract_text = (repo_root / "docs" / "DATA_CONTRACT.md").read_text(encoding="utf-8")

    runtime_sections = {"bridge", "storage", "security", "sync", "desktop"}
    tool_sections = {"$schema", "plugin", "mcp"}
    assert runtime_sections.issubset(runtime_config)
    assert not tool_sections.intersection(runtime_config)
    assert tool_sections.issubset(tool_config)
    assert not runtime_sections.intersection(tool_config)

    storage = runtime_config["storage"]
    assert storage["data_dir"] == "data"
    assert storage["source_discovery_config_path"] == "data/source-discovery-config.json"
    assert storage["source_discovery_log_path"] == "data/source-discovery.log"
    assert storage["social_sources_config_path"] == "data/social-sources-config.json"
    assert "`opencode.json` remains separate" in data_contract_text
    assert "do not move MCP/editor keys into" in data_contract_text


def test_serena_tooling_is_first_class_for_codex_and_opencode(repo_root: Path) -> None:
    docs_dir = repo_root / "docs"
    serena_text = (repo_root / "tools" / "mcp" / "SERENA.md").read_text(encoding="utf-8")
    mcp_index_text = (repo_root / "tools" / "mcp" / "INDEX.md").read_text(encoding="utf-8")
    playwright_text = (repo_root / "tools" / "mcp" / "PLAYWRIGHT.md").read_text(encoding="utf-8")
    index_text = (docs_dir / "INDEX.md").read_text(encoding="utf-8")
    guide_text = (docs_dir / "AI_ASSISTANT_GUIDE.md").read_text(encoding="utf-8")
    contributing_text = (repo_root / "CONTRIBUTING.md").read_text(encoding="utf-8")
    readme_text = (repo_root / "README.md").read_text(encoding="utf-8")
    tools_text = (repo_root / "tools" / "README.md").read_text(encoding="utf-8")
    opencode = json.loads((repo_root / "opencode.json").read_text(encoding="utf-8"))

    assert "SERENA.md" in mcp_index_text
    assert "PLAYWRIGHT.md" in mcp_index_text
    assert "Conventions for Future MCP Docs" in mcp_index_text
    assert "For the MCP tooling landing page, use [INDEX.md](INDEX.md)." in playwright_text
    assert "[PLAYWRIGHT.md](PLAYWRIGHT.md)" in serena_text
    assert "Codex CLI" in serena_text
    assert "OpenCode" in serena_text
    assert "repo docs stay canonical" in serena_text
    assert ".serena/" in serena_text
    assert (
        "codex mcp add serena -- serena start-mcp-server --context=codex --project-from-cwd"
        in serena_text
    )
    assert "serena project create --language python --language typescript" in serena_text
    assert "There is no separate JavaScript Serena language key; use `typescript`" in serena_text
    assert "Node.js and npm" in serena_text
    assert "OpenCode-specific context" in serena_text
    assert "tools/mcp/SERENA.md" in contributing_text
    assert "tools/mcp/SERENA.md" in readme_text
    assert "tools/mcp/README.md" not in contributing_text
    assert "tools/mcp/README.md" not in readme_text
    assert "tools/mcp/README.md" not in tools_text
    assert "[`../tools/mcp/SERENA.md`](../tools/mcp/SERENA.md)" in index_text
    assert "[`../tools/mcp/INDEX.md`](../tools/mcp/INDEX.md)" in index_text
    assert "../tools/mcp/README.md" not in index_text
    assert "Serena memory and repo docs ever diverge" in guide_text
    assert "required Serena setup" in tools_text
    assert "OpenCode" in tools_text
    assert opencode["mcp"]["serena"]["command"] == [
        "serena",
        "start-mcp-server",
        "--context",
        "ide",
        "--project-from-cwd",
    ]


def test_docs_avoid_stale_archive_and_generated_artifact_links(repo_root: Path) -> None:
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    changelog_text = (repo_root / "docs" / "CHANGELOG.md").read_text(encoding="utf-8")

    assert "[`_out/`](../_out/)" not in index_text
    assert "`_out/`" in index_text
    assert "docs/archive/" not in changelog_text
    assert "(archive/)" not in changelog_text
    assert "scraping-pipeline-run-notes.md" in changelog_text


def test_ai_docs_classify_compatibility_surfaces(repo_root: Path) -> None:
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    guide_text = (repo_root / "docs" / "AI_ASSISTANT_GUIDE.md").read_text(encoding="utf-8")
    map_text = (repo_root / "docs" / "architecture-ai-map.md").read_text(encoding="utf-8")
    inventory_text = (repo_root / "docs" / "adapter-plugin-inventory.md").read_text(
        encoding="utf-8"
    )

    assert "## Compatibility Surfaces" in index_text
    assert "This index intentionally does not duplicate that table." in index_text
    assert "stable thin entrypoint for bridge startup and compatibility wrappers" not in index_text
    assert "src/admin_bridge.py (stable thin entrypoint / wiring-only composition root)" in map_text
    assert "stable thin CLI entrypoint" in map_text
    assert "stable thin CLI facade" in map_text
    assert "`src/jobs/common/__init__.py` is a package marker only" in guide_text
    assert "_runtime.facade()` is retired" in guide_text
    assert "package marker only" in map_text
    assert "transitional local-data boundary" in map_text
    assert (
        "export the parser through `src/jobs/parsers.py` and `src/jobs_fetcher.py`"
        not in inventory_text
    )
    assert (
        "only touch `src/jobs_fetcher.py` if a legacy CLI compatibility re-export must stay available"
        in inventory_text
    )


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
    assert scripts["test:frontend:packaged"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py"
    )
    assert scripts["test:frontend:packaged:sync-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --sync-rehearsal --rebuild --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:browser-job-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --browser-job-rehearsal --rebuild --runtime-timeout 60"
    )
    assert scripts["release:preflight"] == (
        "npm run lint:precommit && npm run test:py:extended && npm run test:frontend:unit && npm run test:frontend:packaged && npm run test:frontend:packaged:sync-rehearsal && npm run test:frontend:packaged:update-rehearsal && npm run test:frontend:packaged:orphan-reclaim-rehearsal && npm run test:frontend:packaged:browser-job-rehearsal && npm run test:frontend:packaged:jobs-pipeline"
    )
    assert (
        "_out/latest/build/portable/Baluffo.exe"
        not in scripts["test:frontend:packaged:jobs-pipeline"]
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
        "npm run test:py",
        "npm run test:py:extended",
        "npm run release:preflight",
        "npm run lint:repo-guardrails",
        "npm run security:python",
        "npm run build:ship-bundle",
        "npm run build:portable-exe",
        "python scripts/build_ship_bundle.py --bundle-version <version>",
        "python scripts/build_portable_exe.py --bundle-version <version>",
        "npm run test:frontend:packaged",
        "npm run test:frontend:packaged:sync-rehearsal",
        "npm run test:frontend:packaged:orphan-reclaim-rehearsal",
        "npm run test:frontend:packaged:browser-job-rehearsal",
        "npm run probe:desktop:startup:cold",
    ):
        assert command in testing_text

    assert "This document owns the verification matrix for Baluffo." in testing_text
    assert "startup-probe-architecture.md" in testing_text
    assert "Real shard files must own real tests." in testing_text
    assert "delete or merge any older test that already protects the same invariant" in testing_text
    assert "For the AI bootstrap and task-routing summary" in testing_text
    assert "admin_bridge_entrypoint_root" in testing_text
    assert "admin_bridge_ops_root" not in testing_text
    assert "tests/source_discovery/" in testing_text


def test_release_guide_uses_canonical_release_preflight(repo_root: Path) -> None:
    release_text = (repo_root / "docs" / "RELEASE.md").read_text(encoding="utf-8")

    assert "npm run release:preflight" in release_text
    assert "exact commit you plan to push or tag" in release_text
    assert "npm run lint:precommit" in release_text
    assert "npm run test:frontend:packaged:sync-rehearsal" in release_text
    assert "npm run test:frontend:packaged:orphan-reclaim-rehearsal" in release_text
    assert "npm run test:frontend:packaged:browser-job-rehearsal" in release_text
    assert "npm run test:frontend:packaged:jobs-pipeline" in release_text
    assert "startup-probe-architecture.md" in release_text


def test_active_docs_avoid_stale_runtime_and_test_guidance(repo_root: Path) -> None:
    local_setup_text = (repo_root / "docs" / "LOCAL_SETUP.md").read_text(encoding="utf-8")
    admin_api_text = (repo_root / "docs" / "admin-bridge-api.md").read_text(encoding="utf-8")
    game_sheet_text = (repo_root / "docs" / "game-studios-sheet.md").read_text(encoding="utf-8")
    release_text = (repo_root / "docs" / "RELEASE.md").read_text(encoding="utf-8")
    testing_text = (repo_root / "docs" / "testing.md").read_text(encoding="utf-8")
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    assert "configured local admin PIN" not in local_setup_text
    assert "1234" not in local_setup_text
    assert "Admin overview (requires PIN)" not in admin_api_text
    assert "Wipe account (requires PIN)" not in admin_api_text

    for text in (local_setup_text, admin_api_text):
        assert "requires PIN" not in text

    for text in (local_setup_text, game_sheet_text, testing_text, index_text):
        assert "tests/test_source_discovery.py" not in text

    assert "scripts/ship/update_manager.py" not in release_text
    assert "src/ship/update_manager.py" in release_text
    assert "## Minimum commands" in local_setup_text
    assert "npm run test:py" in local_setup_text
    assert "Full suite / release lane" in testing_text


def test_index_routes_current_process_docs_only(repo_root: Path) -> None:
    index_text = (repo_root / "docs" / "INDEX.md").read_text(encoding="utf-8")
    archive_files = sorted(
        path.relative_to(repo_root).as_posix()
        for path in (repo_root / "docs" / "archive").rglob("*")
        if path.is_file()
    )

    assert "historical-debt-roadmap.md" not in index_text
    assert "quality-improvement-roadmap.md" not in index_text
    assert "quality-follow-up.md" not in index_text
    assert "startup-probe-architecture.md" in index_text
    assert "## Archive" in index_text
    assert "archive/README.md" in index_text
    assert "## Refactor Charters" not in index_text
    assert "Refactor record" not in index_text
    assert archive_files == [
        "docs/archive/README.md",
        "docs/archive/admin-health-dashboard-console-closeout.md",
        "docs/archive/dedup-current-run-blocker-review-closeout.md",
        "docs/archive/dedup-google-sheets-role-bucket-audit-closeout.md",
        "docs/archive/dedup-lifecycle-readiness-closeout.md",
        "docs/archive/dedup-provider-static-disagreement-reconciliation-closeout.md",
        "docs/archive/external-memory-mcp-policy-closeout.md",
        "docs/archive/jobs-fetcher-aggressive-simplification-closeout.md",
        "docs/archive/read-only-lifecycle-ux-closeout.md",
        "docs/archive/source-discovery-adapter-follow-ups-closeout.md",
        "docs/archive/source-sync-production-readiness-closeout.md",
        "docs/archive/task-progress-operational-console-closeout.md",
    ]


def test_contributing_points_startup_perf_changes_to_canonical_architecture_doc(
    repo_root: Path,
) -> None:
    contributing_text = (repo_root / "CONTRIBUTING.md").read_text(encoding="utf-8")

    assert "docs/startup-probe-architecture.md" in contributing_text
