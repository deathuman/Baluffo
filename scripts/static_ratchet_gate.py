"""Run the live-tree static ratchets that CI's Tests job runs and the push gate skipped.

Every module listed here asserts over the checked-out tree: AST scans of ``src/``,
``scripts/`` and ``tools/``, docs-versus-registry audits, and the version/release
policy read from git. A defect introduced anywhere fails them without any test
needing to import the changed module, which makes them the cheapest early warning
the suite has.

They were also the one pytest surface the pre-push hook never ran: ``lint:precommit:ci``
runs pre-commit, pip-audit and repo guardrails, and knip covers dead JS, but none of
them executes a test. So the int-coercion ratchet named three handlers that let
``float("inf")`` escape only after the push, on CI, and main's Tests job stayed red for
three pushes while every local gate was green.

Why a list and not the whole suite: the developer lane is four minutes. This selection
is 98 tests in roughly 16 seconds of pytest (about 22 s in the hook), which keeps the
push under two minutes.

Deliberately absent, with the reason each is already covered or unsafe here:

* ``tests/test_repo_guardrails.py`` and ``tests/test_duplicate_body_guardrail.py`` --
  ``lint:precommit:ci`` already runs ``repo_guardrails.py``, and these wrappers re-run
  the same groups.
* ``tests/test_rule_list_guardrail.py`` -- the docs audit it drives is the
  repo_guardrails ``docs`` group, already run at commit, and it costs 11 seconds.
* ``tests/test_workflow_syntax_policy.py`` -- it may install an actionlint binary, so
  it is not hermetic enough for a push gate.

Keep additions to modules that read the working tree. A module that drives its guard
against a temp fixture is a functional test of that guard, belongs in the suite, and
does not catch a change to the tree.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

RATCHET_MODULES: tuple[str, ...] = (
    "tests/test_shared_utils_coercion_ratchet.py",
    "tests/test_container_version_policy.py",
    "tests/test_task_run_owner_guardrail.py",
    "tests/test_timing_assertion_policy.py",
    "tests/test_changelog_rule_lists_guardrail.py",
    "tests/test_repo_guardrails_markdown_links.py",
    "tests/test_source_suppression_budget_allowed_files.py",
    "tests/test_data_uri_page_guard.py",
)

DEFAULT_TIMEOUT_SECONDS = 180.0


def missing_modules(root: Path = ROOT) -> list[str]:
    """Return the listed modules that are not present in the checkout."""
    return [module for module in RATCHET_MODULES if not (root / module).is_file()]


def build_command() -> list[str]:
    return [
        sys.executable,
        "-m",
        "pytest",
        *RATCHET_MODULES,
        "-q",
        "--color=no",
        "--basetemp=.tmp/pytest/ratchets",
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the live-tree static ratchets that must pass before a push."
    )
    parser.add_argument("--list", action="store_true", help="Print the module list and exit.")
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Wall-clock ceiling for the run, in seconds (default: %(default)s).",
    )
    args = parser.parse_args(argv)

    missing = missing_modules()
    if missing:
        print("Static ratchet gate is missing modules:", file=sys.stderr)
        for module in missing:
            print(f"  - {module}", file=sys.stderr)
        print("Restore the module or update RATCHET_MODULES in this script.", file=sys.stderr)
        return 2

    if args.list:
        print("\n".join(RATCHET_MODULES))
        return 0

    print(f"Running {len(RATCHET_MODULES)} live-tree static ratchet modules...", flush=True)
    try:
        completed = subprocess.run(
            build_command(),
            cwd=ROOT,
            timeout=args.timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        print(
            f"Static ratchets exceeded their {args.timeout:.0f}s ceiling; treat that as a failure.",
            file=sys.stderr,
        )
        return 1
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
