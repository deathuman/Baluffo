import json
import re
import shutil
import subprocess
import sys
import tempfile
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
        "npm run test:frontend:packaged:admin-startup",
        "npm run test:frontend:packaged:admin-active-run",
        "npm run test:frontend:packaged:sync-rehearsal",
        "npm run test:frontend:packaged:update-rehearsal",
        "npm run test:frontend:packaged:orphan-reclaim-rehearsal",
        "npm run test:frontend:packaged:browser-job-rehearsal",
        "npm run test:frontend:packaged:desktop-lifecycle-rehearsal",
        "npm run test:frontend:packaged:active-task-close-rehearsal",
        "npm run test:frontend:packaged:task-abort-schedule-rehearsal",
        "npm run test:frontend:packaged:first-run",
        "npm run test:frontend:packaged:jobs-pipeline",
        "npm run probe:desktop:startup:jobs:cold",
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
        "python -m pip install -r requirements-lock.txt pre-commit pip-audit==2.10.0"
        in workflow_text
    ), f"{workflow_path.name} should install pinned Python tooling before running lint."
    for forbidden_bare_install in (
        "pre-commit mypy",
        "requirements-lock.txt mypy",
    ):
        assert forbidden_bare_install not in workflow_text, (
            f"{workflow_path.name} must not install bare mypy: it is pinned in "
            "requirements-lock.txt and a bare install shadows the pin with latest "
            "(2026-09 drift; same shape as the ruff 0.16.7 environment drift)."
        )
    assert "npm run security:python" in workflow_text, (
        f"{workflow_path.name} should run the Python dependency security audit."
    )
    assert "npm run security:js" in workflow_text, (
        f"{workflow_path.name} should run the JavaScript dependency security audit "
        "so npm advisories surface on PRs and main pushes, not only on release "
        "pushes when Dependabot rescans the default-branch manifest."
    )
    assert "ruff==0.15.14" in (root / "requirements-lock.txt").read_text(encoding="utf-8")


def test_package_json_exposes_js_security_audit_entrypoint(repo_root: Path) -> None:
    package = json.loads((repo_root / "package.json").read_text(encoding="utf-8"))
    script = package["scripts"]["security:js"]
    allowlist = repo_root / "tools" / "security" / "npm-audit-allowlist.json"
    dependabot = repo_root / ".github" / "dependabot.yml"
    lint_workflow = (repo_root / ".github" / "workflows" / "lint.yml").read_text(encoding="utf-8")

    assert script == "python scripts/js_security_audit.py"
    assert allowlist.is_file(), (
        "The npm-audit allowlist must exist so accepted advisories stay visible "
        "and expiry-enforced, mirroring the pip-audit allowlist contract."
    )
    assert dependabot.is_file(), (
        ".github/dependabot.yml must register the npm ecosystem so Dependabot "
        "previews alerts on PRs and opens version-update PRs."
    )
    assert "package-ecosystem" in dependabot.read_text(encoding="utf-8")
    assert "Run JavaScript dependency security audit" in lint_workflow, (
        "lint.yml must run the JavaScript dependency security audit beside the Python lane."
    )


def test_github_workflows_use_project_node_runtime_and_playwright_bridge_owner(
    repo_root: Path,
) -> None:
    workflows = sorted((repo_root / ".github" / "workflows").glob("*.yml"))
    workflow_text_by_path = {path: path.read_text(encoding="utf-8") for path in workflows}
    setup_node_workflows = [
        path
        for path, workflow_text in workflow_text_by_path.items()
        if "actions/setup-node" in workflow_text
    ]
    assert setup_node_workflows, "At least one workflow should configure Node."

    for workflow_path in setup_node_workflows:
        workflow_text = workflow_text_by_path[workflow_path]
        assert (
            'node-version: "25.8.0"' in workflow_text or "node-version: '25.8.0'" in workflow_text
        ), f"{workflow_path.relative_to(repo_root)} should pin project Node to 25.8.0."
        assert "FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true" in workflow_text, (
            f"{workflow_path.relative_to(repo_root)} should keep GitHub JavaScript actions on Node 24."
        )

    release_workflow_text = workflow_text_by_path[
        repo_root / ".github" / "workflows" / "build-portable-exe.yml"
    ]
    assert "runs-on: windows-2022" in release_workflow_text
    assert "runs-on: windows-latest" not in release_workflow_text

    forbidden_node20_actions = (
        "actions/upload-artifact@v4",
        "actions/setup-python@v5",
        "actions/setup-node@v5",
        "actions/checkout@v4",
        "softprops/action-gh-release@v2",
    )
    for workflow_path, workflow_text in workflow_text_by_path.items():
        for action_ref in forbidden_node20_actions:
            assert action_ref not in workflow_text, (
                f"{workflow_path.relative_to(repo_root)} should not use Node 20-era action {action_ref}."
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


def _playwright_install_markers(repo_root: Path) -> tuple[str, str, str]:
    """Return the Python revision, Node revision, and Node install command markers."""
    requirements = (repo_root / "requirements-lock.txt").read_text(encoding="utf-8")
    package_lock = (repo_root / "package-lock.json").read_text(encoding="utf-8")
    python_pin = ""
    for line in requirements.splitlines():
        if line.startswith("playwright=="):
            python_pin = line.split("==", 1)[1].strip()
            break
    assert python_pin, "requirements-lock.txt should pin the Python Playwright version."
    assert '"@playwright/test": "1.63.0"' in package_lock, (
        "package-lock.json should pin the Node Playwright test runner the unit suite imports."
    )
    return python_pin, "1.63.0", "npx playwright install"


def test_release_workflow_installs_both_playwright_runtimes(repo_root: Path) -> None:
    """The Windows release workflow must satisfy both Playwright consumers.

    The portable builder imports Python Playwright, while two tracked frontend unit
    tests launch Node Playwright (``tests/frontend/unit/admin-authoritative-hydration-smoke.test.mjs``
    and ``tests/frontend/unit/admin-schedule-partial-hydration-smoke.test.mjs``).
    Each runtime resolves its own browser revision, so installing only one leaves the
    other launching from an empty cache. That previously produced a 58-minute hang in
    the release-gate step instead of a fast failure.
    """
    workflow_path = repo_root / ".github" / "workflows" / "build-portable-exe.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")
    _, _, node_install = _playwright_install_markers(repo_root)

    assert "python -m playwright install chromium" in workflow_text, (
        f"{workflow_path.name} should install the Python Playwright browser used by the portable build."
    )
    assert node_install in workflow_text, (
        f"{workflow_path.name} should also install the Node Playwright browser; the frontend unit "
        "lane imports @playwright/test and cannot launch Chromium without it."
    )


def _playwright_cache_selection_script() -> str:
    """Extract the two cache-selection expressions from the release workflow.

    Reading the step out of the workflow keeps the test honest: it executes the
    shipped expression instead of a copy that can drift away from the YAML.
    """
    workflow_path = Path(__file__).resolve().parents[2] / ".github/workflows/build-portable-exe.yml"
    text = workflow_path.read_text(encoding="utf-8")
    shell_expr = re.search(r"^\s*\$shellDirs\s*=.*$", text, re.MULTILINE)
    node_expr = re.search(r"^\s*\$nodeDirs\s*=.*$", text, re.MULTILINE)
    assert shell_expr and node_expr, (
        "build-portable-exe.yml should select the Python and Node Playwright caches "
        "into $shellDirs and $nodeDirs."
    )
    return f"{shell_expr.group(0).strip()}\n{node_expr.group(0).strip()}"


def test_release_workflow_playwright_cache_guard_selects_both_consumers(repo_root: Path) -> None:
    """The Playwright cache guard must actually find the Node browser when present.

    The guard shipped as ``Get-ChildItem -Filter "chromium-*" -Exclude
    "chromium_headless_shell-*"``. On pwsh 7, ``-Exclude`` combined with ``-Filter``
    and a non-wildcard ``-Path`` silently drops every child, so the expression
    returned 0 with a real ``chromium-1243`` on disk and the release workflow threw
    "Node Playwright Chromium cache is missing" on every run. The substring-only
    assertions next to it could not see that: they check that the install commands
    are mentioned, never that the verification selects anything.

    This asserts the selection form itself, so the broken combination cannot return.
    """
    script = _playwright_cache_selection_script()
    assert "-Exclude" not in script, (
        "Get-ChildItem -Exclude combined with -Filter and a literal -Path drops every "
        "child on pwsh 7, which made the Playwright cache guard throw unconditionally. "
        "Select with Where-Object -like/-notlike instead."
    )
    assert "chromium_headless_shell-" in script, (
        "The cache guard must recognise the Python Playwright headless shell directory."
    )
    assert "chromium-" in script, (
        "The cache guard must recognise the Node Playwright chromium directory."
    )


def test_release_workflow_playwright_cache_guard_passes_and_fails_correctly(
    repo_root: Path,
) -> None:
    """Exercise the shipped guard both ways: it must pass on a full cache and fail without Node.

    A gate that has only ever been observed passing proves nothing, so this drives
    the real expression from the workflow against synthetic cache layouts.

    Windows-only: the defect is PowerShell's ``-Exclude`` handling, which is
    evaluated by a real PowerShell. Returns early elsewhere rather than importing
    pytest, because the CI Lint lane runs the guardrail group without pytest
    installed and a module-level import would break the whole group there.
    """
    if sys.platform != "win32":
        return

    script = _playwright_cache_selection_script()
    pwsh = shutil.which("pwsh") or shutil.which("powershell")
    if not pwsh:
        return

    def _select(cache_root: Path) -> tuple[int, int]:
        completed = subprocess.run(
            [
                pwsh,
                "-NoProfile",
                "-Command",
                f"$installed = @(Get-ChildItem -Path '{cache_root}' -Directory "
                f"-ErrorAction SilentlyContinue)\n{script}\n"
                'Write-Output "$($shellDirs.Count) $($nodeDirs.Count)"',
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        shell_count, node_count = completed.stdout.split()
        return int(shell_count), int(node_count)

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        full = root / "full"
        (full / "chromium-1243").mkdir(parents=True)
        (full / "chromium_headless_shell-1243").mkdir()
        (full / "ffmpeg-1011").mkdir()
        assert _select(full) == (1, 1), "Guard must find both consumers on a complete cache."

        python_only = root / "python-only"
        (python_only / "chromium_headless_shell-1243").mkdir(parents=True)
        assert _select(python_only) == (1, 0), (
            "Guard must fail when the Node Playwright browser is missing, or the frontend "
            "unit lane hangs for an hour instead of failing fast."
        )


def test_workflows_running_frontend_unit_install_node_playwright(repo_root: Path) -> None:
    """Any workflow that runs the frontend unit lane must install the Node browser."""
    workflows = sorted((repo_root / ".github" / "workflows").glob("*.yml"))
    _, _, node_install = _playwright_install_markers(repo_root)
    unit_lanes = ("npm run test:frontend:unit", "npm run test:unit", "npm run test:smoke")

    checked = 0
    for workflow_path in workflows:
        workflow_text = workflow_path.read_text(encoding="utf-8")
        if not any(lane in workflow_text for lane in unit_lanes):
            continue
        checked += 1
        assert (
            "npx playwright install" in workflow_text
            or "playwright install --with-deps" in workflow_text
        ), (
            f"{workflow_path.relative_to(repo_root)} runs the frontend unit lane but never installs "
            f"the Node Playwright browser (`{node_install}`); browser-backed unit tests will hang "
            "instead of failing."
        )
    assert checked, "Expected at least one workflow to run the frontend unit lane."


def test_release_workflow_passes_ship_zip_to_update_manifest(repo_root: Path) -> None:
    """The release manifest must carry the ship recovery artifact, not just the portable ZIP.

    ``docs/RELEASE.md`` requires the portable ZIP, ship recovery ZIP, and manifest to
    publish together, and ``tests/test_build_desktop_update_release.py`` proves the
    builder supports ``--ship-zip``. Omitting the flag ships a manifest with no
    ``ship_recovery_artifact`` while the release still advertises one.
    """
    workflow_path = repo_root / ".github" / "workflows" / "build-portable-exe.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")

    assert "build_desktop_update_release.py" in workflow_text, (
        f"{workflow_path.name} should build the desktop update manifest."
    )
    assert "--ship-zip" in workflow_text, (
        f"{workflow_path.name} builds a ship bundle but does not pass `--ship-zip`, so the published "
        "desktop update manifest would omit `ship_recovery_artifact`."
    )
    assert "--portable-zip" not in workflow_text, (
        f"{workflow_path.name} should rely on the builder's versioned default portable ZIP rather "
        "than hardcoding a path."
    )


def test_release_workflow_bounds_release_gate_runtime(repo_root: Path) -> None:
    """A hung release gate must fail in minutes, not consume an hour of runner time.

    The gate step runs a browser-backed unit lane plus a long packaged-smoke sequence.
    Without a bound, a leaked listener left the runner idle for 58 minutes before manual
    cancellation produced any artifact.
    """
    workflow_path = repo_root / ".github" / "workflows" / "build-portable-exe.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")

    assert "timeout-minutes:" in workflow_text, (
        f"{workflow_path.name} should bound the release-gate step so a hang fails quickly "
        "with uploaded artifacts instead of running until manual cancellation."
    )
    assert "if-no-files-found: ignore" in workflow_text, (
        f"{workflow_path.name} should tolerate a missing smoke report; a bounded timeout can "
        "cancel a step before it writes one."
    )


def test_release_workflow_uploads_smoke_artifacts_when_cancelled(repo_root: Path) -> None:
    """The artifact upload must be reachable when a hung gate is cancelled.

    GitHub's ``failure()`` is **false** on cancellation, so a condition of the form
    ``failure() && (... || cancelled())`` can never fire on the cancelled case it was
    written for. Cancelled run 35624613484 shows the consequence: the gate ran 57
    minutes, concluded ``cancelled``, and the upload was ``skipped`` -- leaving no
    artifact to diagnose. The condition must therefore be reachable without
    ``failure()`` being true, and must still avoid uploading on a green run.
    """
    workflow_path = repo_root / ".github" / "workflows" / "build-portable-exe.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")

    upload_block = _guard_step_block(workflow_text, "Upload smoke test artifacts on failure")
    assert upload_block, (
        f"{workflow_path.name} should keep a smoke-artifact upload step for failed or "
        "cancelled release gates."
    )

    condition = _guard_step_condition(upload_block)
    assert condition, f"{workflow_path.name} upload step should declare an `if:` condition."

    # `failure() &&` as the outer operator makes every trailing `cancelled()` unreachable.
    assert not re.search(r"failure\(\)\s*&&", condition), (
        f"{workflow_path.name} upload condition must not use `failure() &&` as its outer "
        "operator: `failure()` is false on cancellation, so the cancelled path is dead "
        "code. Use `always()` with an outcome check."
    )
    assert "always()" in condition, (
        f"{workflow_path.name} upload condition should use `always()` so it is evaluated "
        "on cancelled runs as well as failed ones."
    )
    assert "success" in condition, (
        f"{workflow_path.name} upload condition should exclude green runs (for example "
        "`steps.release_gates.outcome != 'success'`) so every successful release does not "
        "attach smoke artifacts."
    )


def _guard_step_block(workflow_text: str, step_name: str) -> str:
    """Return the YAML text of one named step, up to the next step or job boundary."""
    lines = workflow_text.splitlines()
    start = None
    for index, line in enumerate(lines):
        if line.strip() == f"- name: {step_name}":
            start = index
            break
    if start is None:
        return ""
    block: list[str] = []
    for line in lines[start + 1 :]:
        if re.match(r"^\s*- name: ", line):
            break
        block.append(line)
    return "\n".join(block)


def _guard_step_condition(step_block: str) -> str:
    """Return the `if:` expression of a step block, with the `${{ }}` wrapper stripped."""
    for line in step_block.splitlines():
        stripped = line.strip()
        if stripped.startswith("if:"):
            expression = stripped[len("if:") :].strip()
            match = re.fullmatch(r"\$\{\{\s*(.*?)\s*\}\}", expression, re.DOTALL)
            return match.group(1) if match else expression
    return ""


def test_precommit_gate_excludes_tracked_runtime_data(repo_root: Path) -> None:
    """The local changed-mode gate must ignore tracked runtime artifacts.

    The app rewrites these files during normal local use, and they are canonical
    (`git ls-files data/`), so they are never deleted. CI scopes the same gate with
    `--exclude-root data`; without an equivalent local list, `end-of-file-fixer`
    rewrites them and the changed-mode gate fails on files the developer never touched,
    blocking any commit made while the app had been running.
    """
    gate_text = (repo_root / "scripts" / "precommit_gate.py").read_text(encoding="utf-8")
    for runtime_file in sorted(_runtime_written_tracked_data_files(repo_root)):
        assert f'"{runtime_file}"' in gate_text, (
            f"scripts/precommit_gate.py should exclude the tracked runtime artifact "
            f"{runtime_file} from changed-mode collection."
        )
    assert "EXCLUDED_ROOT_PREFIXES" in gate_text, (
        "scripts/precommit_gate.py should keep its excluded-root mechanism alongside the "
        "explicit runtime artifact list."
    )


def _runtime_written_tracked_data_files(repo_root: Path) -> set[str]:
    """Return the tracked ``data/`` files the changed-mode gate must ignore.

    Only tracked files are returned, since untracked runtime artifacts cannot appear in a
    `git`-driven file list. The set itself is declared rather than inferred: `src/` writes
    these through path variables, and a string-matching heuristic over `src/` produced 7
    false positives (contracts, default seeds, social config) while missing 9 real entries,
    which is worse than an explicit list with a tracked-file guard behind it.
    """
    tracked = set(_git_lines(repo_root, "ls-files", "data/"))
    return tracked & _RUNTIME_OWNED_DATA_FILES


# Files the running app rewrites that are also tracked in git, so their working-tree churn
# can reach a changed-files list. Verified against src/ writers:
#   - src/bridge/job_availability_service.py:65, src/jobs/pipeline_bootstrap.py:42-43
#   - src/bridge/routes/get_admin_ops_tab_counts.py:243 (jobs-source-state heartbeats)
#   - src/source_registry_io_paths.py:34,50,78 (gzip-backed tombstones)
#   - src/runtime_seed.py:89-104 (payload defaults for tasks/cache/report/candidates)
#
# Runtime-owned but untracked, EXCLUDED_FILES only: `data/jobs-fetch-report.json` and
# `data/desktop-startup-metrics.jsonl` are written every time the app runs and would
# otherwise churn the working tree on every session. The metrics file was deliberately
# untracked on 2026-09-26 because every desktop start rewrites it and `git status` was
# producing misleading noise. The two ledgers below followed on 2026-09-28 for the same
# reason plus size: `jobs-lifecycle-state.json` is 35.2 MB of remembered jobs whose
# committed snapshot was frozen at 2026-04-10, so every clone and CI checkout
# materialised a five-month-old memory, and `git` re-stored it on each of its 29
# committed versions (21.6 MB of the 167 MB pack). The container image never carried it:
# `.dockerignore` excludes `data/*` except contracts/ and defaults/, and the ship bundle
# seeds these paths via `seed_runtime_data` instead of copying them, so the cost was
# checkout weight, not shipped payload. It is
# runtime state per `docs/DATA_CONTRACT.md`, `src/runtime_seed.py` seeds its sibling
# `jobs-source-state.json` from code, and readers default to an empty ledger when the
# file is absent (`read_job_lifecycle_state` -> `read_json_object(state_path, {})`), so a
# fresh install starts with no memory and rebuilds it as it runs.
_RUNTIME_OWNED_DATA_FILES = frozenset(
    {
        # Tombstones stay tracked on purpose. Unlike the runtime artifacts below,
        # they have no seed: `src/runtime_seed.py` copies
        # `data/defaults/source-registry-{active,pending}.seed.json` and the URL
        # collision file, but nothing seeds tombstones. They record which source
        # URLs were already examined and rejected, and ~11 modules under `src/`
        # read them, so a fresh install with no tombstone file would start
        # re-proposing rejected boards. 33 KB is cheap enough to keep shipping.
        "data/source-registry-tombstones.json.gz",
    }
)

# Runtime-owned artifacts that must stay out of git for the reasons recorded above. They
# are still rewritten during normal use, so they must remain in
# `scripts/precommit_gate.py` EXCLUDED_FILES even though no tracked-file guard covers them.
#
# Split rule: a runtime artifact is untrackable when `src/runtime_seed.py` can create its
# starting shape from code (`payloads` covers the tasks, cache, candidates, report and
# ledger entries below) and its readers tolerate absence. A runtime artifact that carries
# accumulated curation with no seed behind it stays tracked.
_RUNTIME_OWNED_UNTRACKED_DATA_FILES = frozenset(
    {
        "data/jobs-lifecycle-state.json",
        "data/jobs-source-state.json",
        "data/jobs-fetch-report.json",
        "data/desktop-startup-metrics.jsonl",
        "data/jobs-fetch-tasks.json",
        "data/jobs-success-cache.json",
        "data/source-discovery-candidates.json",
        "data/source-discovery-report.json",
    }
)


def test_runtime_owned_data_files_are_tracked(repo_root: Path) -> None:
    """Every declared runtime-owned artifact should still be a tracked file.

    Keeps the exclusion list honest: if one of these stops being tracked (or is renamed),
    the changed-mode gate would be silently excluding a path that no longer exists while
    the real runtime artifact went missing from the list.
    """
    tracked = set(_git_lines(repo_root, "ls-files", "data/"))
    for path in sorted(_RUNTIME_OWNED_DATA_FILES):
        assert path in tracked, (
            f"{path} is declared runtime-owned but is not tracked under data/; update "
            "scripts/precommit_gate.py EXCLUDED_FILES and this list together."
        )


def test_runtime_owned_untracked_data_files_stay_untracked(repo_root: Path) -> None:
    """Deliberately untracked runtime artifacts must stay untracked and stay excluded.

    These are rewritten by normal use and are covered by the `data/*.json`/`data/*.jsonl`
    ignore rules, so a stray `git add -f` would silently re-track them — which is how a
    35 MB lifecycle snapshot frozen months earlier ended up in every clone and CI
    checkout. They stay in `EXCLUDED_FILES` because the app still rewrites
    them locally, and the changed-mode gate must keep ignoring that churn.
    """
    tracked = set(_git_lines(repo_root, "ls-files", "data/"))
    gate_text = (repo_root / "scripts" / "precommit_gate.py").read_text(encoding="utf-8")
    for path in sorted(_RUNTIME_OWNED_UNTRACKED_DATA_FILES):
        assert path not in tracked, (
            f"{path} is tracked again after being deliberately untracked; run "
            f"`git rm --cached {path}` to restore the intended runtime-owned state."
        )
        assert f'"{path}"' in gate_text, (
            f"scripts/precommit_gate.py should keep {path} in EXCLUDED_FILES: the app "
            "rewrites it at runtime, and the changed-mode gate would otherwise fail on "
            "files the developer never touched."
        )


def _git_lines(repo_root: Path, *args: str) -> list[str]:
    """Return non-empty stdout lines from a git command, or [] when it fails."""
    completed = subprocess.run(  # noqa: S603
        ["git", "-C", str(repo_root), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return []
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def test_hooks_path_is_wired_and_bypass_is_prohibited(repo_root: Path) -> None:
    """The repo must route git hooks through `.githooks` and forbid `--no-verify`.

    `.githooks/pre-commit` runs the changed-mode gate and `.githooks/pre-push` runs the
    full CI-parity gate before a push to `main`. Both are advisory at the client: a
    `--no-verify` bypass leaves no detectable trace, because the fixer hooks are
    idempotent and a bypassed tree is byte-identical to a clean one. The durable control
    is CI re-running the same gate on every push to `main`, so this test asserts that
    wiring exists and that the prohibition is documented where an agent will read it.
    """
    agents = (repo_root / "AGENTS.md").read_text(encoding="utf-8")
    assert "--no-verify" in agents, "AGENTS.md should state the --no-verify prohibition explicitly."

    hook_dir = repo_root / ".githooks"
    for hook_name, gate_command in (
        ("pre-commit", "lint:precommit:changed"),
        ("pre-push", "lint:precommit:ci"),
    ):
        hook_path = hook_dir / hook_name
        assert hook_path.is_file(), f".githooks/{hook_name} should exist and be tracked."
        hook_text = hook_path.read_text(encoding="utf-8")
        assert gate_command in hook_text, (
            f".githooks/{hook_name} should run `{gate_command}` so the local gate matches CI."
        )

    # CI is the enforcement point a bypass cannot skip.
    lint_workflow = (repo_root / ".github" / "workflows" / "lint.yml").read_text(encoding="utf-8")
    assert "lint:precommit:ci" in lint_workflow, (
        "lint.yml should re-run the CI-parity gate on push, which is what actually "
        "enforces it when a local hook is bypassed."
    )


def test_precommit_gate_reports_the_failing_stage() -> None:
    """A nonzero gate exit should name the stage that produced it.

    A failing CI lint run once exited 1 after every guardrail group printed "passed",
    leaving no way to tell which stage failed from the log alone.
    """
    gate_text = (ROOT / "scripts" / "precommit_gate.py").read_text(encoding="utf-8")

    assert "_report_failing_stage" in gate_text, (
        "scripts/precommit_gate.py should report which stage failed before returning nonzero."
    )
    for stage in ("pre-commit", "repo-guardrails", "complexity-baseline"):
        assert f'"{stage}"' in gate_text, (
            f"scripts/precommit_gate.py should attribute failures to the {stage} stage."
        )


def test_lint_workflow_enforces_ruff_import_sorting() -> None:
    root = ROOT
    ruff_config = tomllib.loads((root / "ruff.toml").read_text(encoding="utf-8"))
    pre_commit_text = (root / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    package = json.loads((root / "package.json").read_text(encoding="utf-8"))

    assert "I" in ruff_config["lint"]["select"]
    assert "id: ruff-check" in pre_commit_text
    assert "rev: v0.15.14" in pre_commit_text
    assert package["scripts"]["lint:precommit:ci"] == (
        "python scripts/precommit_gate.py --mode all --exclude-root data"
    )


def test_lint_workflow_enforces_source_complexity_baseline() -> None:
    root = ROOT
    baseline = json.loads(
        (root / "scripts" / "complexity_baseline.json").read_text(encoding="utf-8")
    )
    precommit_gate = (root / "scripts" / "precommit_gate.py").read_text(encoding="utf-8")

    assert baseline["ruff_version"] == "0.15.14"
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


def test_pre_push_hook_uses_timed_lint_default_and_explicit_full_ci_mode() -> None:
    root = ROOT
    hook_path = root / ".githooks" / "pre-push"
    hook_text = hook_path.read_text(encoding="utf-8")
    package = json.loads((root / "package.json").read_text(encoding="utf-8"))

    assert "npm run lint:precommit:ci" in hook_text, (
        f"{hook_path.name} should invoke the lint gate before pushing to main."
    )
    assert "PRE_PUSH_FULL_CI" in hook_text, (
        f"{hook_path.name} should expose an explicit full local CI mode."
    )
    assert "PRE_PUSH_WARM_HOOKS" in hook_text, f"{hook_path.name} should expose a hook warmup mode."
    assert "phase=pre-push-start" not in hook_text, (
        f"{hook_path.name} should keep timing output dynamic rather than hardcoded as static text."
    )
    for legacy_command in (
        "npm run test:refactor:changed",
        "npm run test:py:extended",
        "npm run test:smoke",
    ):
        assert legacy_command not in hook_text, (
            f"{hook_path.name} should not run `{legacy_command}` on the default push path."
        )

    assert package["scripts"]["prepush:warm"] == (
        "python scripts/run_pre_push_hook.py --warm-hooks"
    )
    assert package["scripts"]["prepush:full"] == ("python scripts/run_pre_push_hook.py --full-ci")


def _bash_executable() -> str | None:
    """Return a bash that honours argv/stdin, or None.

    On Windows, ``shutil.which("bash")`` can resolve to the WSL launcher
    (``C:\\Windows\\System32\\bash.exe``), which accepts ``-c`` but silently drops
    positional arguments and stdin -- so a behavioural check written against it
    passes vacuously or fails for the wrong reason. Prefer Git Bash.
    """
    for candidate in (
        r"C:\Program Files\Git\bin\bash.exe",
        r"C:\Program Files\Git\usr\bin\bash.exe",
    ):
        if Path(candidate).is_file():
            return candidate
    found = shutil.which("bash")
    if not found:
        return None
    normalized = Path(found).as_posix().lower()
    if "system32/bash" in normalized or "system32\\bash" in normalized:
        # The WSL launcher drops argv/stdin; fall back to no behavioural check.
        return None
    return found


def test_pre_push_hook_gates_release_tags(repo_root: Path) -> None:
    """A release-tag push must run the same gate as a push to main.

    The hook's ref-detection loop only matched ``refs/heads/main``, so
    ``git push origin vX.Y.Z`` matched no case and ran no gate at all -- measured
    at 104 ms on the real v0.3.0 tag push. A tag publishes the GitHub release,
    the signed desktop update manifest, and the GHCR image, so it is the
    highest-stakes push in the repo.

    This executes the shipped ``case`` block with synthetic ref lines rather than
    asserting on its text, so a refactor that keeps the strings but drops the
    behaviour still fails.
    """
    hook_path = repo_root / ".githooks" / "pre-push"
    hook_text = hook_path.read_text(encoding="utf-8")
    case_block = re.search(r"case \"\$remote_ref\" in.*?esac\n", hook_text, re.DOTALL)
    assert case_block, f"{hook_path.name} should still classify pushed refs in a case block."
    assert "refs/tags/v*" in case_block.group(0), (
        f"{hook_path.name} must gate release tags (`refs/tags/v*`), not only `refs/heads/main`; "
        "an ungated tag push publishes the release, the signed manifest, and the image."
    )

    shell = _bash_executable()
    if not shell:
        return

    script = (
        "need_gate=0\nbranch_ref=''\n"
        'for remote_ref in "$@"; do\n'
        + case_block.group(0)
        + "done\n"
        + 'echo "$need_gate|$branch_ref"\n'
    )

    def _detect(refs: list[str]) -> str:
        # Refs are passed as positional arguments rather than stdin: the WSL
        # launcher accepts `-c` but drops stdin, which would make every
        # assertion below pass for the wrong reason.
        completed = subprocess.run(
            [shell, "-c", script, "--", *refs],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        return completed.stdout.strip()

    assert _detect(["refs/tags/v0.3.0"]) == "1|refs/tags/v0.3.0", "a release tag must be gated"
    assert _detect(["refs/heads/main"]) == "1|refs/heads/main", "main must stay gated"
    assert _detect(["refs/heads/feature/x"]) == "0|", "a feature branch must not be gated"
    assert _detect(["refs/tags/rollback-2026-09-25"]) == "0|", (
        "a rollback tag is not a release and must not be gated"
    )
    assert _detect(["refs/tags/v1.2.3", "refs/heads/main"]).startswith("1|"), (
        "a multi-ref push containing a release must be gated"
    )


def test_shell_hooks_have_no_backticks_in_case_branch_comments(repo_root: Path) -> None:
    """Reject backticks in comments inside a shell ``case`` branch.

    bash performs command substitution inside a ``case`` branch's comments, so a
    comment that quotes a command runs it. Verified on Git Bash 5.3: a comment
    reading ``git push origin vX.Y.Z`` inside a case branch issued a real push
    attempt against the live remote and left ``need_gate`` empty, silently
    disabling the gate it was documenting. The same backticks outside a case
    branch are inert (verified), so this only scans case blocks.
    """
    offenders: list[str] = []
    for hook_path in sorted((repo_root / ".githooks").glob("*")):
        if not hook_path.is_file():
            continue
        text = hook_path.read_text(encoding="utf-8")
        for block in re.finditer(r"case\s+.*?\n.*?\besac\n", text, re.DOTALL):
            start_line = text[: block.start()].count("\n") + 1
            for offset, line in enumerate(block.group(0).splitlines()):
                stripped = line.lstrip()
                if stripped.startswith("#") and "`" in stripped:
                    offenders.append(f"{hook_path.name}:{start_line + offset}")
    assert not offenders, (
        "Backticks in a comment inside a shell `case` branch are executed as command "
        "substitution by bash, which can silently disable a gate or run the quoted "
        f"command: {offenders}"
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
        "npm run check:python-version && python src/packaged_desktop_smoke.py --sync-rehearsal --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:update-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --desktop-update-rehearsal --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:orphan-reclaim-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --orphan-reclaim-rehearsal --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:browser-job-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --browser-job-rehearsal --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:desktop-lifecycle-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --desktop-lifecycle-rehearsal --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:active-task-close-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --active-task-close-rehearsal --runtime-timeout 60"
    )
    assert scripts["test:frontend:packaged:task-abort-schedule-rehearsal"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --open-path admin.html --node-smoke-script tests/frontend/packaged-desktop-smoke.task-abort-schedule.mjs --runtime-timeout 60 --playwright-timeout 240"
    )
    assert scripts["test:frontend:packaged:first-run"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --open-path desktop-probe.html --node-smoke-script tests/frontend/packaged-desktop-smoke.first-run-jobs.mjs --runtime-timeout 60 --playwright-timeout 240"
    )
    assert scripts["test:frontend:packaged:jobs-pipeline"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --node-smoke-script tests/frontend/packaged-desktop-smoke.jobs-pipeline.mjs --playwright-timeout 300"
    )
    assert scripts["test:frontend:packaged:admin-active-run"] == (
        "npm run check:python-version && python src/packaged_desktop_smoke.py --open-path admin.html --node-smoke-script tests/frontend/packaged-desktop-smoke.admin-active-run.mjs --runtime-timeout 60 --playwright-timeout 180"
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
    input_json = tmp_path / "jobs-unified.json"
    input_json.write_text(
        json.dumps(
            [
                {
                    "title": "Environment Artist",
                    "company": "Studio",
                    "city": "Hong Kong",
                    "country": "Unknown",
                    "source": "google_sheets",
                    "jobLink": "https://example.com/job",
                }
            ]
        ),
        encoding="utf-8",
    )
    output_json = tmp_path / "manifest.json"
    completed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "scripts/location_unknown_country_manifest.py",
            "build",
            "--input-json",
            str(input_json),
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
