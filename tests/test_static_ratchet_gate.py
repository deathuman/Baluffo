"""Wiring for the pre-push live-tree static ratchet step.

The step exists because the int-coercion ratchet failed on main for three pushes
while every local gate was green: the hook ran pre-commit, pip-audit and knip, and
no test at all. These tests keep the step wired -- the listed modules must exist,
the motivating module must stay listed, list mode must report the same selection,
and both the npm script and the hook must call it. They deliberately do not run
the pytest selection; that is the gate's own job and would double the suite's cost.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from scripts.static_ratchet_gate import RATCHET_MODULES, missing_modules

ROOT = Path(__file__).resolve().parents[1]


def test_every_listed_ratchet_module_exists() -> None:
    assert missing_modules() == []


def test_the_int_coercion_ratchet_stays_listed() -> None:
    # The failure this gate was written for: three int(<bare name>) handlers let
    # float("inf") escape, and only CI's Tests job ran the scan that named them.
    assert "tests/test_shared_utils_coercion_ratchet.py" in RATCHET_MODULES


def test_a_missing_module_is_reported_rather_than_skipped(tmp_path: Path) -> None:
    assert missing_modules(tmp_path) == list(RATCHET_MODULES)


def test_list_mode_reports_the_same_modules() -> None:
    completed = subprocess.run(
        [sys.executable, "scripts/static_ratchet_gate.py", "--list"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.split() == list(RATCHET_MODULES)


def test_package_script_and_pre_push_hook_invoke_the_gate() -> None:
    package = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))
    assert "static_ratchet_gate.py" in package["scripts"]["test:py:ratchets"]
    hook = (ROOT / ".githooks" / "pre-push").read_text(encoding="utf-8")
    assert "npm run test:py:ratchets" in hook


def _full_ci_guard_body() -> list[str]:
    """Lines inside the hook's `full_ci_enabled -eq 0` guard, exclusive of both bounds."""
    lines = (ROOT / ".githooks" / "pre-push").read_text(encoding="utf-8").splitlines()
    guard = '  if [ "$full_ci_enabled" -eq 0 ]; then'
    assert guard in lines, "the standalone ratchet step must be guarded by the full-CI check"
    body: list[str] = []
    for line in lines[lines.index(guard) + 1 :]:
        if line == "  fi":
            return body
        body.append(line)
    raise AssertionError("the full-CI guard around the ratchet step is never closed")


def test_full_ci_mode_skips_the_standalone_ratchet_step() -> None:
    # `npm run test:py` selects the same modules, so full local CI mode would pay for
    # them twice. The guard has to cover the invocation itself, not merely precede it.
    assert any("npm run test:py:ratchets" in line for line in _full_ci_guard_body())
    hook = (ROOT / ".githooks" / "pre-push").read_text(encoding="utf-8")
    assert hook.count("npm run test:py:ratchets") == 1


def test_full_ci_mode_still_runs_the_suite_that_contains_those_modules() -> None:
    # Skipping the standalone copy is only safe because the full run still gates the
    # push on the same assertions.
    hook = (ROOT / ".githooks" / "pre-push").read_text(encoding="utf-8")
    assert 'run_timed_command_background "test:py" "$test_py_tmp" npm run test:py' in hook
