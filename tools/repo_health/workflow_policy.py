import json
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_release_workflow_uses_canonical_test_entrypoints() -> None:
    root = ROOT
    workflow_path = root / ".github" / "workflows" / "build-portable-exe.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")

    for expected_command in (
        "npm run test:frontend:unit",
        "npm run test:py:extended",
        "npm run test:frontend:packaged",
        "npm run test:frontend:packaged:sync-rehearsal",
        "npm run test:frontend:packaged:update-rehearsal",
        "npm run test:frontend:packaged:orphan-reclaim-rehearsal",
        "npm run test:frontend:packaged:browser-job-rehearsal",
        "npm run test:frontend:packaged:jobs-pipeline",
        "python scripts/build_ship_bundle.py",
        "python scripts/extract_release_notes.py",
        "python scripts/build_desktop_update_release.py",
    ):
        assert expected_command in workflow_text, (
            f"{workflow_path.name} should invoke `{expected_command}`."
        )

    for forbidden_command in (
        "node tests/frontend/unit/all.test.mjs",
        "Sync test manifest",
        "npm run sync:test-manifest",
        "scripts/sync_frontend_unit_manifest.mjs",
        'py -3.13 -m unittest discover -s tests -p "test_*.py" -v',
        "scripts\\run_py_tests.cmd",
        "py -3.13 scripts/packaged_desktop_smoke.py",
        "python scripts/packaged_desktop_smoke.py",
        "py -3.13 scripts/build_ship_bundle.py",
        "Set-Content -Path release-notes.md",
    ):
        assert forbidden_command not in workflow_text, (
            f"{workflow_path.name} should route release-gate test lanes through their canonical npm scripts instead of duplicating raw commands."
        )


def test_lint_workflow_uses_canonical_precommit_entrypoints() -> None:
    root = ROOT
    workflow_path = root / ".github" / "workflows" / "lint.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")
    package_path = root / "package.json"
    package_text = package_path.read_text(encoding="utf-8")

    assert "lint:precommit:ci" in workflow_text, (
        f"{workflow_path.name} should run the CI pre-commit entrypoint."
    )
    assert "pre-commit run --all-files" not in workflow_text, (
        f"{workflow_path.name} should not embed the raw full-repo pre-commit command."
    )
    assert "--exclude-root data" in package_text, (
        f"{package_path.name} should route the CI pre-commit entrypoint through the data exclusion."
    )
    assert (
        "python -m pip install -r requirements-lock.txt pre-commit mypy pip-audit==2.10.0"
        in workflow_text
    ), f"{workflow_path.name} should install pinned Python tooling before running lint."
    assert "npm run security:python" in workflow_text, (
        f"{workflow_path.name} should run the Python dependency security audit."
    )
    assert "ruff==0.15.9" in (root / "requirements-lock.txt").read_text(encoding="utf-8")


def test_github_workflows_use_project_node_runtime_and_playwright_bridge_owner(
    repo_root: Path,
) -> None:
    workflows = sorted((repo_root / ".github" / "workflows").glob("*.yml"))
    setup_node_workflows = [
        path for path in workflows if "actions/setup-node" in path.read_text(encoding="utf-8")
    ]
    assert setup_node_workflows, "At least one workflow should configure Node."

    for workflow_path in setup_node_workflows:
        workflow_text = workflow_path.read_text(encoding="utf-8")
        assert (
            'node-version: "25.8.0"' in workflow_text or "node-version: '25.8.0'" in workflow_text
        ), f"{workflow_path.relative_to(repo_root)} should pin project Node to 25.8.0."
        assert "FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true" in workflow_text, (
            f"{workflow_path.relative_to(repo_root)} should keep GitHub JavaScript actions on Node 24."
        )

    test_workflow_text = (repo_root / ".github" / "workflows" / "test.yml").read_text(
        encoding="utf-8"
    )
    assert "sleep 15" not in test_workflow_text
    assert "npm run dev:bridge &" not in test_workflow_text
    assert "Wait for bridge readiness" not in test_workflow_text
    assert "npm run test:smoke" in test_workflow_text
    playwright_config = (repo_root / "playwright.config.js").read_text(encoding="utf-8")
    assert 'globalSetup: "./tests/frontend/global-setup.js"' in playwright_config
    assert 'globalTeardown: "./tests/frontend/global-teardown.js"' in playwright_config

    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    assert package["engines"]["node"] == "25.8.0"


def test_lint_workflow_enforces_ruff_import_sorting() -> None:
    root = ROOT
    ruff_config = tomllib.loads((root / "ruff.toml").read_text(encoding="utf-8"))
    pre_commit_text = (root / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    package = json.loads((root / "package.json").read_text(encoding="utf-8"))

    assert "I" in ruff_config["lint"]["select"]
    assert "id: ruff-check" in pre_commit_text
    assert "rev: v0.15.9" in pre_commit_text
    assert package["scripts"]["lint:precommit:ci"] == (
        "python scripts/precommit_gate.py --mode all --exclude-root data"
    )


def test_lint_workflow_enforces_source_complexity_baseline() -> None:
    root = ROOT
    baseline = json.loads(
        (root / "scripts" / "complexity_baseline.json").read_text(encoding="utf-8")
    )
    precommit_gate = (root / "scripts" / "precommit_gate.py").read_text(encoding="utf-8")

    assert baseline["ruff_version"] == "0.15.9"
    assert baseline["rule"] == "C901"
    assert baseline["threshold"] == 10
    assert baseline["scope"] == ["src"]
    assert isinstance(baseline["entries"], dict)
    assert "run_repo_guardrails()" in precommit_gate
    assert "run_complexity_baseline()" in precommit_gate


def test_package_json_exposes_repo_guardrails_entrypoint(repo_root: Path) -> None:
    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    refactor_gate = (repo_root / "scripts" / "refactor_changed_gate.py").read_text(encoding="utf-8")

    assert package["scripts"]["lint:repo-guardrails"] == (
        "python tools/repo_health/repo_guardrails.py"
    )
    assert "tests/test_suite_contract.py" not in refactor_gate
    assert "tools/repo_health/repo_guardrails.py" in refactor_gate


def test_package_json_uses_direct_frontend_unit_discovery(repo_root: Path) -> None:
    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    package_text = (repo_root / "package.json").read_text(encoding="utf-8")
    workflow_text = (repo_root / ".github" / "workflows" / "build-portable-exe.yml").read_text(
        encoding="utf-8"
    )
    frontend_workflow_text = (repo_root / ".github" / "workflows" / "test.yml").read_text(
        encoding="utf-8"
    )

    assert package["scripts"]["test:frontend:unit"] == (
        'node --test --test-reporter=dot "tests/frontend/unit/*.test.mjs"'
    )
    assert package["scripts"]["test:unit"] == "npm run test:frontend:unit"
    for stale_token in (
        "check:test-manifest",
        "sync:test-manifest",
        "scripts/sync_frontend_unit_manifest.mjs",
        "tests/frontend/unit/all.test.mjs",
        "tests/frontend/unit/manifest-contract.test.mjs",
    ):
        assert stale_token not in package_text
        assert stale_token not in workflow_text
    assert "Sync test manifest" not in workflow_text
    assert 'node-version: "25.8.0"' in frontend_workflow_text


def test_package_json_exposes_python_security_audit_entrypoint(repo_root: Path) -> None:
    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    allowlist = repo_root / "tools" / "security" / "pip-audit-allowlist.json"

    assert package["scripts"]["security:python"] == "python scripts/security_audit.py"
    assert allowlist.is_file()


def test_pre_push_hook_runs_python_and_smoke_gates() -> None:
    root = ROOT
    hook_path = root / ".githooks" / "pre-push"
    hook_text = hook_path.read_text(encoding="utf-8")

    for expected_command in (
        "npm run lint:precommit:ci",
        "npm run test:refactor:changed",
        "npm run test:py:extended",
        "npm run test:smoke",
    ):
        assert expected_command in hook_text, (
            f"{hook_path.name} should invoke `{expected_command}` before pushing to main."
        )


def test_pre_commit_hook_runs_lint_gate() -> None:
    root = ROOT
    hook_path = root / ".githooks" / "pre-commit"
    hook_text = hook_path.read_text(encoding="utf-8")

    assert "npm run lint:precommit:changed" in hook_text, (
        f"{hook_path.name} should invoke the changed-file lint gate before every commit."
    )


def test_package_json_dev_pipeline_uses_module_entrypoint(repo_root: Path) -> None:
    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    assert package["scripts"]["dev:pipeline"] == (
        "npm run check:python-version && python -m src.jobs.pipeline --force-refresh-all"
    )


def test_package_json_exposes_refactor_changed_entrypoint(repo_root: Path) -> None:
    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    assert package["scripts"]["test:refactor:changed"] == "python scripts/refactor_changed_gate.py"


def test_package_json_packaged_smoke_scripts_use_direct_dist_by_default(repo_root: Path) -> None:
    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    scripts = package["scripts"]
    assert scripts["test:frontend:packaged"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py"
    )
    assert scripts["test:frontend:packaged:sync-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --sync-rehearsal --rebuild --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:update-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --desktop-update-rehearsal --rebuild --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:orphan-reclaim-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --orphan-reclaim-rehearsal --rebuild --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:browser-job-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --browser-job-rehearsal --rebuild --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:jobs-pipeline"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --node-smoke-script tests/frontend/packaged-desktop-smoke.jobs-pipeline.mjs --playwright-timeout 300"
    )
    assert scripts["test:frontend:packaged:orchestrated"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --exe-path _out/latest/build/portable/Baluffo.exe"
    )


def test_package_json_perf_scripts_reuse_existing_perf_entrypoints(repo_root: Path) -> None:
    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    scripts = package["scripts"]
    assert scripts["perf:py:timing"] == "npm run test:py:timing"
    assert scripts["perf:discovery:benchmark"] == (
        "npm run check:python-version && python src/discovery_sanity_benchmark.py"
    )
    assert scripts["perf:startup:cold"] == "npm run probe:desktop:startup:cold"
    assert scripts["perf:startup:pair"] == "npm run probe:desktop:startup:pair"
    assert scripts["perf:startup:warm"] == "npm run probe:desktop:startup:warm"


def test_dev_pipeline_targeted_npm_entrypoint_starts_without_relative_import_failure(
    repo_root: Path, tmp_path: Path
) -> None:
    npm_command = shutil.which("npm.cmd") or shutil.which("npm")
    assert npm_command, "npm must be available for the pipeline entrypoint smoke test."
    completed = subprocess.run(  # noqa: S603
        [
            npm_command,
            "run",
            "dev:pipeline",
            "--",
            "--only-sources",
            "missing-dummy-source",
            "--output-dir",
            str(tmp_path),
            "--max-workers",
            "1",
            "--no-preserve-previous-on-empty",
            "--quiet",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    combined = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
    report_path = tmp_path / "jobs-fetch-report.json"
    assert completed.returncode in (0, 2), combined
    assert report_path.exists(), combined
    assert "attempted relative import with no known parent package" not in combined


def test_location_unknown_country_manifest_script_runs_from_repo_root(
    repo_root: Path, tmp_path: Path
) -> None:
    input_csv = tmp_path / "jobs-unified.csv"
    input_csv.write_text(
        "title,company,city,country,source,jobLink\n"
        "Environment Artist,Studio,Hong Kong,Unknown,google_sheets,https://example.com/job\n",
        encoding="utf-8",
    )
    output_json = tmp_path / "manifest.json"
    completed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "scripts/location_unknown_country_manifest.py",
            "build",
            "--input-csv",
            str(input_csv),
            "--output-json",
            str(output_json),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    combined = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
    assert completed.returncode == 0, combined
    assert output_json.exists(), combined
