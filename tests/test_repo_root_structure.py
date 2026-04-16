from pathlib import Path

REQUIRED_ROOT_FILES = (
    "index.html",
    "jobs.html",
    "saved.html",
    "admin.html",
    "styles.css",
    "theme.js",
    "frontend-runtime-config.js",
)

MOVED_ROOT_SUPPORT_FILES = (
    "admin-config.js",
    "app-local-data-client.js",
    "local-data-client.js",
    "desktop-local-data-client.js",
    "jobs-state.js",
    "jobs-parsing-utils.js",
    "saved-zip-utils.js",
)

REQUIRED_FRONTEND_OWNERS = (
    "frontend/shared/config/admin-config.js",
    "frontend/shared/local-data/app-client.js",
    "frontend/shared/local-data/browser-client.js",
    "frontend/shared/local-data/desktop-client.js",
    "frontend/jobs/state.js",
    "frontend/jobs/parsing-utils.js",
    "frontend/saved/zip-utils.js",
)


def test_repo_root_keeps_required_page_assets_only(repo_root: Path) -> None:
    for rel_path in REQUIRED_ROOT_FILES:
        assert (repo_root / rel_path).exists(), f"required root asset missing: {rel_path}"

    for rel_path in MOVED_ROOT_SUPPORT_FILES:
        assert not (repo_root / rel_path).exists(), (
            f"support module drifted back to repo root: {rel_path}"
        )


def test_relocated_support_modules_exist_under_frontend(repo_root: Path) -> None:
    for rel_path in REQUIRED_FRONTEND_OWNERS:
        assert (repo_root / rel_path).exists(), f"relocated support module missing: {rel_path}"


def test_repo_controlled_temp_roots_use_dot_tmp(repo_root: Path) -> None:
    owner_expectations = {
        "scripts/run_py_tests.cmd": ".tmp\\pytest",
        "scripts/run_py_tests_extended.cmd": ".tmp\\pytest",
        "scripts/run_py_tests_timing.cmd": ".tmp\\pytest",
        "tests/helpers/temp_paths.py": '.tmp" / "pytest',
        "src/packaged_desktop_smoke.py": '.tmp" / "packaged-desktop-smoke',
        "probes/packaged_desktop_double_launch_probe.py": '.tmp" / "probes" / "double-launch',
        "tests/frontend/packaged-desktop-smoke.mjs": ".tmp/packaged-desktop-smoke/",
        "tests/frontend/packaged-desktop-smoke.jobs-pipeline.mjs": ".tmp/packaged-desktop-smoke/",
        "playwright.config.js": ".tmp/playwright/test-results",
        ".github/workflows/build-portable-exe.yml": ".tmp/packaged-desktop-smoke",
    }
    forbidden_tokens = (".codex-tmp/", ".codex-tmp\\", "test-results/")

    for rel_path, expected_token in owner_expectations.items():
        text = (repo_root / rel_path).read_text(encoding="utf-8")
        assert expected_token in text, f"{rel_path} should reference the .tmp-owned path"
        for forbidden in forbidden_tokens:
            assert forbidden not in text, (
                f"{rel_path} should not reference stale temp root `{forbidden}`"
            )
