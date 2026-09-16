"""Coercion helpers must not raise on JSON-legal float values.

``int()`` raises two unrelated exception types:

    int(float("nan"))  -> ValueError("cannot convert float NaN to integer")
    int(float("inf"))  -> OverflowError("cannot convert float infinity to integer")

``issubclass(OverflowError, ValueError)`` is False, so the common idiom
``try: int(value) / except (TypeError, ValueError)`` catches NaN and lets
Infinity escape. ``json.dumps`` emits bare ``NaN``/``Infinity`` tokens by
default and ``json.loads`` parses them back, so both values are reachable from
any round-tripped artifact or external payload.

These tests pin the contract for the canonical helpers and the desktop copies,
and ratchet the rest of the repository so the pattern cannot spread.
"""

from __future__ import annotations

import ast
import importlib
from decimal import Decimal
from pathlib import Path

import pytest

from src.shared.utils import coerce_bool, coerce_int, int_or_default

ROOT = Path(__file__).resolve().parents[1]

# Values a coercion helper must survive. NaN/Infinity are JSON-legal tokens.
NON_CONVERTIBLE = [
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="inf"),
    pytest.param(float("-inf"), id="-inf"),
    pytest.param(None, id="none"),
    pytest.param("", id="empty-str"),
    pytest.param("abc", id="non-numeric-str"),
    pytest.param([], id="list"),
    pytest.param({}, id="dict"),
    pytest.param(object(), id="object"),
]

CONVERTIBLE = [
    pytest.param(True, 1, id="true"),
    pytest.param(False, 0, id="false"),
    pytest.param(7, 7, id="int"),
    pytest.param(-7, -7, id="negative-int"),
    pytest.param(3.7, 3, id="float"),
    pytest.param(-3.7, -3, id="negative-float"),
    pytest.param("5", 5, id="numeric-str"),
    pytest.param(" 5 ", 5, id="padded-numeric-str"),
    pytest.param("-5", -5, id="negative-numeric-str"),
    pytest.param(Decimal("3.7"), 3, id="decimal"),
]


class TestIntOrDefault:
    @pytest.mark.parametrize("value", NON_CONVERTIBLE)
    def test_returns_default_without_raising(self, value: object) -> None:
        assert int_or_default(value, 42) == 42

    @pytest.mark.parametrize(("value", "expected"), CONVERTIBLE)
    def test_converts_convertible_values(self, value: object, expected: int) -> None:
        assert int_or_default(value, 0) == expected


class TestCoerceInt:
    @pytest.mark.parametrize("value", NON_CONVERTIBLE)
    def test_falls_back_to_clamped_default_without_raising(self, value: object) -> None:
        # minimum=0 so the default survives clamping unchanged.
        assert coerce_int(value, 42, minimum=0, maximum=100) == 42

    @pytest.mark.parametrize(("value", "expected"), CONVERTIBLE)
    def test_converts_and_clamps(self, value: object, expected: int) -> None:
        assert coerce_int(value, 0, minimum=-100, maximum=100) == expected

    def test_clamps_to_bounds(self) -> None:
        assert coerce_int(999, 0, minimum=0, maximum=10) == 10
        assert coerce_int(-999, 0, minimum=0, maximum=10) == 0


class TestCoerceBool:
    @pytest.mark.parametrize("value", NON_CONVERTIBLE)
    def test_returns_default_without_raising(self, value: object) -> None:
        assert coerce_bool(value, True) is True
        assert coerce_bool(value, False) is False

    @pytest.mark.parametrize("text", ["1", "true", "TRUE", "yes", "on", " True "])
    def test_truthy_tokens(self, text: str) -> None:
        assert coerce_bool(text, False) is True

    @pytest.mark.parametrize("text", ["0", "false", "FALSE", "no", "off"])
    def test_falsy_tokens(self, text: str) -> None:
        assert coerce_bool(text, True) is False


# --- behavioural checks on the desktop copies ------------------------------

_DESKTOP_MODULES = (
    "src.ship.desktop_app.browser",
    "src.ship.desktop_app.launcher_diagnostics",
    "src.ship.desktop_app.launcher_flow",
    "src.ship.desktop_app.session",
    "src.ship.desktop_app.startup_ready",
    "src.ship.desktop_app.startup_watchdog",
    "src.ship.desktop_app._linux",
    "src.ship.desktop_app._windows",
    "src.ship.desktop_update_state",
)

_RAISING_PROBES = (float("nan"), float("inf"), float("-inf"))


@pytest.mark.parametrize("module_name", _DESKTOP_MODULES)
def test_desktop_as_int_never_raises(module_name: str) -> None:
    module = importlib.import_module(module_name)
    helper = getattr(module, "_as_int", None)
    if helper is None:
        pytest.skip(f"{module_name} has no _as_int helper")

    for value in _RAISING_PROBES:
        assert helper(value, 42) == 42, f"{module_name}._as_int({value!r}) must return the default"


def test_desktop_as_int_copies_agree() -> None:
    """Every desktop _as_int must return the same result for the same input."""
    helpers = []
    for module_name in _DESKTOP_MODULES:
        module = importlib.import_module(module_name)
        helper = getattr(module, "_as_int", None)
        if helper is not None:
            helpers.append((module_name, helper))
    assert helpers, "expected at least one desktop _as_int helper"

    probes = [
        True,
        False,
        7,
        -7,
        3.7,
        -3.7,
        "5",
        "-5",
        "abc",
        "",
        None,
        (),
        *_RAISING_PROBES,
    ]

    disagreements = []
    for value in probes:
        outcomes = []
        for module_name, helper in helpers:
            try:
                outcomes.append((module_name, repr(helper(value, 42))))
            except Exception as exc:  # noqa: BLE001
                outcomes.append((module_name, f"raised {type(exc).__name__}"))
        if len({outcome for _, outcome in outcomes}) > 1:
            detail = ", ".join(f"{name.rsplit('.', 1)[-1]}={outcome}" for name, outcome in outcomes)
            disagreements.append(f"_as_int({value!r}): {detail}")

    assert not disagreements, (
        "desktop _as_int copies disagree on the same input:\n  " + "\n  ".join(disagreements)
    )


# --- repo-wide ratchet -----------------------------------------------------

# Names that read as "coerce a value to int": private variants of the canonical
# src/shared/utils.py helpers.
_INT_HELPER_NAMES = frozenset(
    {
        "_as_int",
        "_int_value",
        "_safe_int",
        "_coerce_int",
        "safe_int",
        "coerce_int",
        "int_or_default",
        "coerce_non_negative_int",
        "_int",
    }
)

# Helpers that call int() without catching OverflowError, so float("inf")
# escapes as an uncaught exception. Recorded at the time the canonical helpers
# were fixed; the list may shrink but must never grow.
#
# Fixing one of these means deleting its entry here.
_KNOWN_OVERFLOW_UNSAFE = frozenset(
    {
        "scripts/chrome_trace_summary.py:_safe_int",
        "scripts/jobs_yield_gate.py:_int_value",
        "scripts/perf_compare.py:_int_value",
        "scripts/source_policy_soak_report.py:_int_value",
        "src/bridge/admin_bootstrap.py:_int",
        "src/bridge/fetch_report_summary.py:_safe_int",
        "src/bridge/ops_health.py:_safe_int",
        "src/bridge/ops_live_payload.py:coerce_non_negative_int",
        "src/bridge/registry_sync_summary.py:_safe_int",
        "src/bridge/report_normalizer.py:safe_int",
        "src/bridge/task_failure_attempts.py:_safe_int",
        "src/container_entrypoint.py:_coerce_int",
        "src/jobs/adapters/plugins/social/register.py:_int_value",
        "src/jobs/adapters/social.py:_coerce_int",
        "src/jobs/common/taxonomy.py:_coerce_int",
        "src/source_discovery/active_audit_runtime.py:_safe_int",
        "src/source_discovery/audit_report_summary.py:safe_int",
        "src/source_discovery/gamedevmap_active_dry_run.py:_safe_int",
        "src/source_registry_state.py:_coerce_int",
        "src/storage/source_runtime.py:_coerce_int",
        "src/storage_metrics.py:_int_value",
        "tools/measurements/pipeline/dedup_pressure_report.py:_int",
    }
)

_OVERFLOW_HANDLER_NAMES = frozenset(
    {"OverflowError", "ArithmeticError", "Exception", "BaseException"}
)


def _helper_defs() -> list[tuple[str, ast.FunctionDef]]:
    found: list[tuple[str, ast.FunctionDef]] = []
    for base in ("src", "scripts", "tools"):
        for path in sorted((ROOT / base).rglob("*.py")):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name in _INT_HELPER_NAMES:
                    found.append((path.relative_to(ROOT).as_posix(), node))
    return found


def _calls_int(node: ast.FunctionDef) -> bool:
    return any(
        isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == "int"
        for call in ast.walk(node)
    )


def _catches_overflow(node: ast.FunctionDef) -> bool:
    """True when a handler catches OverflowError (or something wider)."""
    for handler in ast.walk(node):
        if not isinstance(handler, ast.ExceptHandler):
            continue
        if handler.type is None:  # bare except
            return True
        types = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
        for exc_type in types:
            if isinstance(exc_type, ast.Name) and exc_type.id in _OVERFLOW_HANDLER_NAMES:
                return True
    return False


def test_no_new_overflow_unsafe_int_helper() -> None:
    """Ratchet: int() without an OverflowError handler must not spread."""
    current = {
        f"{path}:{node.name}"
        for path, node in _helper_defs()
        if _calls_int(node) and not _catches_overflow(node)
    }
    new = sorted(current - _KNOWN_OVERFLOW_UNSAFE)
    assert not new, (
        "new int-coercion helper(s) call int() without catching OverflowError, so "
        "float('inf') escapes as an uncaught exception. Catch "
        "(TypeError, ValueError, OverflowError) or reuse src.shared.utils:\n  " + "\n  ".join(new)
    )


def test_ratchet_baseline_has_no_stale_entries() -> None:
    """Every baselined helper must still exist and still be unsafe."""
    current = {
        f"{path}:{node.name}"
        for path, node in _helper_defs()
        if _calls_int(node) and not _catches_overflow(node)
    }
    stale = sorted(_KNOWN_OVERFLOW_UNSAFE - current)
    assert not stale, (
        "these helpers are baselined as overflow-unsafe but no longer are; "
        "delete their entries from _KNOWN_OVERFLOW_UNSAFE:\n  " + "\n  ".join(stale)
    )
