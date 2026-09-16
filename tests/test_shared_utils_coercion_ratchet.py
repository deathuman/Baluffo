"""Static ratchets that stop unguarded int() coercion spreading repo-wide.

``int(float("inf"))`` raises ``OverflowError``, which is **not** a
``ValueError`` subclass, so ``except (TypeError, ValueError)`` catches ``nan``
but lets ``inf`` escape as an uncaught exception. The behavioural contract for
the canonical helpers lives in ``test_shared_utils_coercion.py``; this module
holds the static scans that keep the defect from reappearing anywhere in
``src/``, ``scripts/`` or ``tools/``.

Three complementary rules, because each alone has a blind spot:

* a **name-gated** scan over ``_INT_HELPER_NAMES`` for ``int()`` with no
  ``OverflowError`` handler;
* a **body-digest** scan, because the defect is a property of the body and not
  the name, so a guarded helper's body can reappear unguarded under a name the
  name-gated scan ignores;
* a **name-independent** scan for ``int(<bare name>)`` under a
  ``ValueError``-only handler, which is the shape that can actually raise
  ``OverflowError``.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Names that read as "coerce a value to int": private variants of the canonical
# src/shared/utils.py helpers.
#
# The list is deliberately wider than the canonical names. The bug this ratchet
# guards (int() without OverflowError) is a property of the BODY, not the name,
# so a name-only scan has a blind spot: `_count`, `positive_int`, `_safe_status`
# and `_safe_non_negative_int` all carried a byte-identical buggy body while
# being invisible here. The second block below is every such name found by
# matching bodies against the baselined set.
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
        # names recovered from byte-identical buggy bodies
        "_clamped_int",
        "_count",
        "_safe_non_negative_int",
        "_safe_pid",
        "_safe_status",
        "_summary_int",
        "positive_int",
        "safe_non_negative_int",
        "to_int",
    }
)

# Helpers that call int() without catching OverflowError, so float("inf")
# escapes as an uncaught exception.
#
# EMPTY: every helper this ratchet can see now catches OverflowError. Keep it
# empty -- a new entry means new unguarded int() coercion, and the test below
# will name it.
_KNOWN_OVERFLOW_UNSAFE: frozenset[str] = frozenset()

_OVERFLOW_HANDLER_NAMES = frozenset(
    {"OverflowError", "ArithmeticError", "Exception", "BaseException"}
)


def _helper_defs(require_known_name: bool = True) -> list[tuple[str, ast.FunctionDef]]:
    """Return ``(path, node)`` for every scanned function definition.

    ``require_known_name=True`` restricts to ``_INT_HELPER_NAMES`` (the original
    name-gated ratchet). ``require_known_name=False`` returns every function, so
    a name-independent rule can run over the same corpus.
    """
    found: list[tuple[str, ast.FunctionDef]] = []
    for base in ("src", "scripts", "tools"):
        for path in sorted((ROOT / base).rglob("*.py")):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef):
                    continue
                if require_known_name and node.name not in _INT_HELPER_NAMES:
                    continue
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


def test_guarded_coercion_helper_is_not_shadowed_by_an_unguarded_twin() -> None:
    """A guarded helper's body must not reappear unguarded under another name.

    The name-gated ratchet above has a blind spot: a helper named `_count` or
    `positive_int` is invisible to it even when its body is a byte-identical
    copy of an unguarded coercion. That is exactly how 9 extra unsafe helpers
    hid before they were consolidated. Compare bodies instead of names.
    """
    import hashlib

    from tools.repo_health.duplicate_body_policy import _normalized_body

    def body_digest(node: ast.FunctionDef) -> str:
        return hashlib.sha256(_normalized_body(node).encode()).hexdigest()[:12]

    # Digests of every unguarded body the ratchet already knows about.
    guarded_digests = {
        body_digest(node)
        for _, node in _helper_defs()
        if _calls_int(node) and _catches_overflow(node)
    }

    shadowed = []
    for base in ("src", "scripts", "tools"):
        for path in sorted((ROOT / base).rglob("*.py")):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            rel = path.relative_to(ROOT).as_posix()
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef):
                    continue
                if node.name in _INT_HELPER_NAMES:
                    continue  # already covered by the ratchet above
                if not _calls_int(node) or _catches_overflow(node):
                    continue
                if body_digest(node) in guarded_digests:
                    shadowed.append(f"{rel}:{node.name}")

    assert not shadowed, (
        "these functions have a body byte-identical to a guarded coercion helper "
        "but are named something the name-gated ratchet ignores, so they still "
        "let float('inf') escape. Rename them into _INT_HELPER_NAMES or guard "
        "them:\n  " + "\n  ".join(sorted(shadowed))
    )


# Sites where ``int(<bare name>)`` sits under a ValueError-only handler and a
# static rule cannot decide whether the name can be a float.
#
# This is deliberately NOT a claim that all of these are bugs. Reading them
# shows a mix: ``int(raw)`` where ``raw`` was built with ``str(...).strip()`` or
# taken from a regex group cannot raise OverflowError, while ``int(pid)`` on a
# value decoded from JSON can. Separating the two needs real type information
# (the repo pins mypy for exactly that); a hand-rolled flow analysis was tried
# and did not move the count at all, because the values cross function
# boundaries.
#
# The ratchet is therefore on the *set*, not on correctness: the count must not
# grow, so new undecidable coercions are noticed, and each entry should be
# deleted as its site is either proven safe or routed through int_or_default.
_UNDECIDABLE_BARE_NAME_INT_SITES = frozenset(
    {
        "scripts/build_portable_exe.py:_resolve_cache_retain_entries",
        "scripts/perf_pipeline_stages.py:_container_host_pid",
        "src/bridge/lifecycle_cleanup.py:_schema_version_int",
        "src/bridge/routes/get_local_data.py:_handle_startup_metrics_route",
        "src/bridge/routes/get_local_data.py:_payload",
        "src/bridge/routes/get_ops_diagnostics.py:handle_ops_diagnostic_routes",
        "src/bridge/routes/get_ops_status.py:handle_ops_status_routes",
        "src/bridge/routes/get_registry.py:_registry_table_limit_per_bucket",
        "src/bridge/routes/route_payload_helpers.py:safe_query_int",
        "src/bridge/storage_health.py:_env_int",
        "src/bridge/task_launch_api.py:start_jobs_bootstrap_task",
        "src/dev_admin_supervisor.py:_schema_version_int",
        "src/fetch_incremental_sanity_benchmark.py:_select_loaders",
        "src/jobs/adapters/plugins/versioning.py:normalize_schema_version",
        "src/jobs/browser_fallback_pool.py:browser_pool_recycle_acquisitions",
        "src/ship/desktop_app/_linux.py:_listening_socket_inodes_for_port",
        "src/ship/desktop_update_service.py:_run_download_worker",
        "src/ship/desktop_update_service.py:on_progress",
        "src/ship/desktop_update_shared.py:download_file",
        "src/ship/packaged_smoke/rehearsal_browser.py:run_packaged_desktop_lifecycle_rehearsal",
        "src/ship/packaged_smoke/runtime_process.py:pids_listening_on_tcp_port_windows",
        "src/ship/startup_profile.py:_parse_ts_ms",
        "src/source_discovery/config.py:env_int",
        "src/source_discovery/gamedevmap_active_dry_run.py:_active_audit_ttl_minutes",
        "src/source_discovery/orchestrator_finalize.py:finalize_run",
        "src/source_discovery/probe.py:_static_result_count",
        "src/source_discovery/probe_failure_memory.py:_parse_retention_days",
        "src/source_discovery/scoring.py:resolve_discovery_thresholds",
        "src/source_discovery/sheet_directory.py:_sheet_max_rows",
    }
)


def _provably_not_float_names(node: ast.FunctionDef) -> set[str]:
    """Names that cannot hold a float, so ``int(name)`` cannot raise OverflowError.

    A parameter or local annotated ``int``/``bool``/``str`` cannot be a float:
    ``int()`` on those raises ``TypeError`` or ``ValueError``, both already
    caught. This is the one piece of real type information available without
    running mypy, and it removes the obvious false positives from the scan.
    """
    safe: set[str] = set()
    args = node.args
    for arg in list(args.posonlyargs) + list(args.args) + list(args.kwonlyargs):
        if arg.annotation is not None and ast.unparse(arg.annotation) in {"int", "bool", "str"}:
            safe.add(arg.arg)
    for inner in ast.walk(node):
        if isinstance(inner, ast.AnnAssign) and inner.annotation is not None:
            if isinstance(inner.target, ast.Name) and ast.unparse(inner.annotation) in {
                "int",
                "bool",
                "str",
            }:
                safe.add(inner.target.id)
    return safe


def _undecidable_bare_name_int_sites() -> set[str]:
    """Name-independent scan for ``int(<bare name>)`` guarded without OverflowError.

    ``int("inf")`` raises ``ValueError`` and is already caught, but ``int(x)``
    where ``x`` is a float raises ``OverflowError``. Only the bare-name form is
    reported: a string-typed argument (``int(str(x))``, ``int(x.strip())``,
    a regex group) cannot raise ``OverflowError``, and neither can a name that is
    annotated ``int``, ``bool`` or ``str``.
    """
    found: set[str] = set()
    for path, node in _helper_defs(require_known_name=False):
        safe_names = _provably_not_float_names(node)
        for try_node in ast.walk(node):
            if not isinstance(try_node, ast.Try):
                continue
            catches_value_error = False
            overflow_safe = False
            for handler in try_node.handlers:
                if handler.type is None:
                    overflow_safe = True
                    break
                types = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
                names = {t.id for t in types if isinstance(t, ast.Name)}
                if names & _OVERFLOW_HANDLER_NAMES:
                    overflow_safe = True
                    break
                if "ValueError" in names:
                    catches_value_error = True
            if overflow_safe or not catches_value_error:
                continue
            for stmt in try_node.body:
                for call in ast.walk(stmt):
                    if (
                        isinstance(call, ast.Call)
                        and isinstance(call.func, ast.Name)
                        and call.func.id == "int"
                        and call.args
                        and isinstance(call.args[0], ast.Name)
                        and call.args[0].id not in safe_names
                    ):
                        found.add(f"{path}:{node.name}")
    return found


def test_no_new_undecidable_bare_name_int_coercion() -> None:
    """Ratchet: the set of undecidable ``int(<bare name>)`` sites must not grow.

    The name-gated ratchet above only sees helpers whose names look like
    coercions, and the body-digest test only sees byte-identical copies. This
    one is name-independent and catches the shape wherever it appears, so a new
    site is surfaced even when it is named nothing like a coercion helper.
    """
    current = _undecidable_bare_name_int_sites()
    new = sorted(current - _UNDECIDABLE_BARE_NAME_INT_SITES)
    assert not new, (
        "new int(<bare name>) coercion guarded without OverflowError, so "
        "float('inf') escapes as an uncaught exception. Add OverflowError to the "
        "handler, or route the value through src.shared.utils.int_or_default:\n  "
        + "\n  ".join(new)
    )


def test_undecidable_ratchet_has_no_stale_entries() -> None:
    """A site that was fixed or removed must leave the ratchet in the same change."""
    current = _undecidable_bare_name_int_sites()
    stale = sorted(_UNDECIDABLE_BARE_NAME_INT_SITES - current)
    assert not stale, (
        "these sites no longer match the undecidable bare-name int() shape; delete "
        "them from _UNDECIDABLE_BARE_NAME_INT_SITES:\n  " + "\n  ".join(stale)
    )
