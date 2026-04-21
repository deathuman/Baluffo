import ast
import re
from pathlib import Path


def _module_tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


def _top_level_function_names(tree: ast.Module) -> list[str]:
    return [node.name for node in tree.body if isinstance(node, ast.FunctionDef)]


def _imported_modules(tree: ast.Module) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
    return modules


def _top_level_imported_modules(tree: ast.Module) -> set[str]:
    modules: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                modules.add(alias.name)
    return modules


def _find_function(tree: ast.Module, name: str) -> ast.FunctionDef:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"Expected top-level function {name!r}")


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Attribute):
        left = _call_name(node.value)
        return f"{left}.{node.attr}" if left else node.attr
    if isinstance(node, ast.Name):
        return node.id
    return ""


def _function_call_names(function: ast.FunctionDef) -> set[str]:
    return {
        _call_name(node.func)
        for node in ast.walk(function)
        if isinstance(node, ast.Call) and _call_name(node.func)
    }


def _function_imports_module(function: ast.FunctionDef, module_name: str) -> bool:
    return any(
        isinstance(node, ast.ImportFrom) and node.module == module_name
        for node in ast.walk(function)
    )


def _has_name_main_guard(tree: ast.Module) -> bool:
    for node in tree.body:
        if not isinstance(node, ast.If):
            continue
        test = node.test
        if not isinstance(test, ast.Compare):
            continue
        if (
            isinstance(test.left, ast.Name)
            and test.left.id == "__name__"
            and len(test.ops) == 1
            and isinstance(test.ops[0], ast.Eq)
            and len(test.comparators) == 1
            and isinstance(test.comparators[0], ast.Constant)
            and test.comparators[0].value == "__main__"
        ):
            return True
    return False


def test_discovered_python_test_files_define_real_tests() -> None:
    root = Path(__file__).resolve().parent
    test_files = sorted(root.rglob("test_*.py"))
    pattern = re.compile(r"^(class\s+\w+|def\s+test_)", re.MULTILINE)

    for path in test_files:
        text = path.read_text(encoding="utf-8")
        assert pattern.search(text), (
            f"{path.name} matches test discovery but does not define a real test case."
        )


def test_frontend_test_patterns_reserve_generated_manifest_as_only_aggregator() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    tests_root = repo_root / "tests"
    frontend_unit_root = tests_root / "frontend" / "unit"
    allowed_manifest = frontend_unit_root / "all.test.mjs"
    aggregator_paths = sorted(tests_root.rglob("all.test.mjs"))
    assert aggregator_paths == [allowed_manifest], (
        "tests/frontend/unit/all.test.mjs must remain the only all.test.mjs-style test aggregator."
    )

    real_frontend_test_pattern = re.compile(r"\btest\s*\(")
    for path in sorted(frontend_unit_root.glob("*.test.mjs")):
        if path == allowed_manifest:
            continue
        text = path.read_text(encoding="utf-8")
        assert real_frontend_test_pattern.search(text), (
            f"{path.relative_to(repo_root)} matches the frontend discovered test pattern but does not define a real test."
        )


def test_bridge_mutable_module_state_stays_in_approved_runtime_modules() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    bridge_root = repo_root / "src" / "bridge"
    allowed = {
        (bridge_root / "sync_state.py").resolve(),
        (bridge_root / "server" / "runtime_state.py").resolve(),
    }
    allowed_constant_maps = {
        (bridge_root / "config.py").resolve(): {"LOG_LEVEL_ORDER"},
    }

    offenders: list[str] = []
    for path in sorted(bridge_root.rglob("*.py")):
        if path.resolve() in allowed:
            continue
        tree = _module_tree(path)
        for node in tree.body:
            target_name = ""
            value = None
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                target_name = node.targets[0].id
                value = node.value
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                target_name = node.target.id
                value = node.value
            if not target_name or target_name == "__all__" or value is None:
                continue
            if target_name in allowed_constant_maps.get(path.resolve(), set()):
                continue
            suspicious = isinstance(value, (ast.Dict, ast.Set))
            suspicious = suspicious or (isinstance(value, ast.List) and target_name.isupper())
            suspicious = suspicious or (
                isinstance(value, ast.Call)
                and _call_name(value.func)
                in {"threading.RLock", "threading.Lock", "threading.Event"}
            )
            if suspicious:
                offenders.append(str(path.relative_to(repo_root)))
                break

    assert not offenders, (
        "Mutable bridge module state should stay in src/bridge/sync_state.py or "
        "src/bridge/server/runtime_state.py only:\n- " + "\n- ".join(offenders)
    )


def test_admin_bridge_keeps_registry_autosync_and_sync_normalization_out_of_entrypoint(
    repo_root: Path,
) -> None:
    admin_bridge = (repo_root / "src" / "admin_bridge.py").read_text(encoding="utf-8")
    admin_registry_api = (repo_root / "src" / "bridge" / "admin_registry_api.py").read_text(
        encoding="utf-8"
    )
    admin_task_runtime = (repo_root / "src" / "bridge" / "admin_task_runtime.py").read_text(
        encoding="utf-8"
    )
    assert "from src.bridge import admin_registry_api as admin_registry_api_mod" in admin_bridge
    assert "from src.bridge import registry_sync_flow as _registry_sync_flow" in admin_bridge
    assert "_registry_sync_flow.persist_state_and_auto_sync(" not in admin_bridge
    assert "_registry_sync_flow.persist_state_and_auto_sync(" in admin_registry_api
    assert "_registry_sync_flow.maybe_trigger_auto_sync_push(" not in admin_bridge
    assert "_registry_sync_flow.maybe_trigger_auto_sync_push(" in admin_task_runtime
    assert "def _normalize_sync_settings" not in admin_bridge
    assert "def _mask_sync_token" not in admin_bridge


def test_source_discovery_entrypoint_stays_thin_cli_wrapper(repo_root: Path) -> None:
    target = repo_root / "src" / "source_discovery.py"
    tree = _module_tree(target)
    main_fn = _find_function(tree, "main")

    assert _top_level_function_names(tree) == ["_ensure_repo_on_path", "main"]
    assert _top_level_imported_modules(tree) == {"__future__", "sys", "pathlib"}
    assert _function_imports_module(main_fn, "src.source_discovery.orchestrator")
    assert "_main" in _function_call_names(main_fn)
    assert "int" in _function_call_names(main_fn)
    assert _has_name_main_guard(tree)


def test_source_discovery_orchestrator_stays_split_by_phase(repo_root: Path) -> None:
    target = repo_root / "src" / "source_discovery" / "orchestrator.py"
    text = target.read_text(encoding="utf-8")

    assert "from . import orchestrator_generation as orchestrator_generation_mod" in text
    assert "from . import orchestrator_probe as orchestrator_probe_mod" in text
    assert "from . import orchestrator_finalize as orchestrator_finalize_mod" in text
    assert "from .orchestrator_runtime import DiscoveryRunDeps, DiscoveryRunState" in text
    assert "orchestrator_generation_mod.root = sys.modules[__name__]" in text
    assert "orchestrator_probe_mod.root = sys.modules[__name__]" in text
    assert "orchestrator_finalize_mod.root = sys.modules[__name__]" in text
    assert "async def _run_probe_batch(" not in text
    assert "def write_progress_report(" not in text


def test_jobs_fetcher_facade_uses_leaf_common_modules_not_root_symbol_barrel(
    repo_root: Path,
) -> None:
    target = repo_root / "src" / "jobs_fetcher.py"
    text = target.read_text(encoding="utf-8")

    assert "from src.jobs import common as _common" not in text
    assert "from src.jobs.common import config as _common_config" in text
    assert "from src.jobs.common import diagnostics as _common_diagnostics" in text
    assert "from src.jobs.common import fetch as _common_fetch" in text


def test_jobs_fetcher_facade_stays_lazy_and_small(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs_fetcher.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    function_names = _top_level_function_names(tree)
    assert "__getattr__" in function_names
    assert "__dir__" in function_names
    assert "_ensure_repo_on_path" in function_names
    assert "_COMPAT_MODULE_EXPORTS" in text
    assert "parse_google_sheets_csv = _parsers.parse_google_sheets_csv" not in text
    assert "run_static_studio_pages_source = _static.run_static_studio_pages_source" not in text
    assert "raise SystemExit(main())" in text

    alias_lines = [
        line
        for line in text.splitlines()
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*=\s*_[A-Za-z0-9_]+\.[A-Za-z0-9_]+$", line)
    ]
    assert len(alias_lines) <= 3, "jobs_fetcher facade drifted back to bulk alias re-exports"


def test_static_adapter_root_stays_thin_orchestration_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "adapters" / "static.py"
    text = target.read_text(encoding="utf-8")

    assert "from . import static_listing as static_listing_mod" in text
    assert "from . import static_runtime as static_runtime_mod" in text
    assert "from . import static_sources as static_sources_mod" in text
    assert "static_detail_mod.root = sys.modules[__name__]" in text
    assert "static_listing_mod.root = sys.modules[__name__]" in text
    assert "def run_static_studio_pages_source(" in text
    assert "def static_source_shard(" not in text
    assert "def static_source_name_for_registry_row(" not in text
    assert "fetch_pages_batched" not in text
    assert len(text.splitlines()) <= 160, "static adapter root drifted back toward monolith size"


def test_sync_task_worker_logic_is_shared_between_admin_bridge_and_sync_service(
    repo_root: Path,
) -> None:
    admin_bridge = (repo_root / "src" / "admin_bridge.py").read_text(encoding="utf-8")
    admin_task_runtime = (repo_root / "src" / "bridge" / "admin_task_runtime.py").read_text(
        encoding="utf-8"
    )
    sync_service = (repo_root / "src" / "bridge" / "sync_service.py").read_text(encoding="utf-8")
    assert "from src.bridge import admin_task_runtime as admin_task_runtime_mod" in admin_bridge
    assert "from src.bridge import sync_task_flow as _sync_task_flow" in sync_service
    assert "admin_task_runtime_mod.run_sync_task_worker(" in admin_bridge
    assert "root_mod._sync_task_flow.run_sync_task_worker(" in admin_task_runtime
    assert "_sync_task_flow.run_sync_task_worker(" in sync_service


def test_bridge_api_uses_sync_service_for_sync_status_wiring(repo_root: Path) -> None:
    admin_bridge = (repo_root / "src" / "admin_bridge.py").read_text(encoding="utf-8")
    bridge_bootstrap = (repo_root / "src" / "bridge" / "bootstrap.py").read_text(encoding="utf-8")
    bridge_api = (repo_root / "src" / "bridge" / "api.py").read_text(encoding="utf-8")
    sync_service = (repo_root / "src" / "bridge" / "sync_service.py").read_text(encoding="utf-8")
    assert "return bridge_bootstrap.build_bridge_api(" in admin_bridge
    assert (
        "sync_config_status=lambda: source_sync_module.config_status(refresh_sync_config())"
        not in bridge_bootstrap
    )
    assert "set_sync_status=_set_sync_status" not in bridge_bootstrap
    assert 'if self._field_is_default("sync_config_status"):' in bridge_api
    assert 'if self._field_is_default("set_sync_status"):' in bridge_api
    assert "def sync_config_status(self) -> dict[str, Any]:" in sync_service
    assert "def set_sync_status(" in sync_service


def test_bridge_api_exposes_route_facing_entrypoints(repo_root: Path) -> None:
    admin_bridge = (repo_root / "src" / "admin_bridge.py").read_text(encoding="utf-8")
    bridge_bootstrap = (repo_root / "src" / "bridge" / "bootstrap.py").read_text(encoding="utf-8")
    bridge_api = (repo_root / "src" / "bridge" / "api.py").read_text(encoding="utf-8")
    assert "return bridge_bootstrap.build_bridge_api(" in admin_bridge
    assert "append_startup_metric=append_startup_metric" in bridge_bootstrap
    assert "persist_state_and_auto_sync=persist_state_and_auto_sync" in bridge_bootstrap
    assert "add_manual_source=add_manual_source" in bridge_bootstrap
    assert "trigger_source_check=trigger_source_check" in bridge_bootstrap
    assert "append_startup_metric: Callable[[str, dict[str, Any] | None], None]" in bridge_api
    assert "add_manual_source: Callable[[str], dict[str, Any]]" in bridge_api
    assert "trigger_source_check: Callable[..., dict[str, Any]]" in bridge_api


def test_admin_bridge_delegates_source_check_orchestration_to_bridge_module(
    repo_root: Path,
) -> None:
    target = repo_root / "src" / "admin_bridge.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    imported_modules = _imported_modules(tree)
    trigger_fn = _find_function(tree, "trigger_source_check")
    normalize_fn = _find_function(tree, "normalize_manual_static_studio_fields")

    assert "src.bridge" in imported_modules
    assert "from src.bridge import admin_registry_api as admin_registry_api_mod" in text
    assert "admin_registry_api_mod.root = sys.modules[__name__]" in text
    assert "admin_registry_api_mod.trigger_source_check" in _function_call_names(trigger_fn)
    assert "admin_registry_api_mod.normalize_manual_static_studio_fields" in _function_call_names(
        normalize_fn
    )


def test_desktop_app_package_stays_lazy_compat_facade(repo_root: Path) -> None:
    target = repo_root / "src" / "ship" / "desktop_app" / "__init__.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    function_names = _top_level_function_names(tree)
    assert "__getattr__" in function_names
    assert "__dir__" in function_names
    assert "import *" not in text
    assert "_COMPAT_MODULES = (" in text


def test_sharded_python_test_families_do_not_use_star_helper_imports(repo_root: Path) -> None:
    for relative_path in (
        "tests/admin/test_admin_bridge_report_history.py",
        "tests/admin/test_admin_bridge_live_payloads.py",
        "tests/source_discovery/test_candidate_generation.py",
        "tests/source_discovery/test_config_and_helpers.py",
        "tests/source_discovery/test_directory_sources.py",
        "tests/source_discovery/test_run_discovery_flow.py",
        "tests/jobs_static/test_browser_and_regression_queues.py",
        "tests/jobs_static/test_detail_fallback.py",
        "tests/jobs_static/test_rendered_cards_and_plugins.py",
        "tests/jobs_static/test_static_source_execution.py",
    ):
        text = (repo_root / relative_path).read_text(encoding="utf-8")
        assert "from ._helpers import *" not in text


def test_admin_bridge_root_stays_thin_entrypoint_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "admin_bridge.py"
    text = target.read_text(encoding="utf-8")

    assert "from src.bridge import admin_entrypoint_runtime as admin_entrypoint_runtime_mod" in text
    assert (
        "from src.bridge import admin_entrypoint_services as admin_entrypoint_services_mod" in text
    )
    assert "from src.bridge import admin_registry_api as admin_registry_api_mod" in text
    assert "from src.bridge import admin_task_runtime as admin_task_runtime_mod" in text
    assert "admin_entrypoint_runtime_mod.root = sys.modules[__name__]" in text
    assert "admin_entrypoint_services_mod.root = sys.modules[__name__]" in text
    assert "admin_registry_api_mod.root = sys.modules[__name__]" in text
    assert "admin_task_runtime_mod.root = sys.modules[__name__]" in text
    assert "def build_bridge_api(" in text
    assert "return bridge_bootstrap.build_bridge_api(" in text
    assert "smoke_runtime: dict[str, Any]" not in text
    assert "find_existing_static_source_by_studio_domain(" not in text
    assert len(text.splitlines()) <= 900, "admin bridge root drifted back toward monolith size"


def test_admin_runtime_megatest_stays_split(repo_root: Path) -> None:
    legacy = repo_root / "tests" / "admin" / "test_admin_bridge_ops_runtime.py"
    assert not legacy.exists()
    for relative_path in (
        "tests/admin/test_admin_bridge_runtime_config.py",
        "tests/admin/test_admin_bridge_ops_health.py",
        "tests/admin/test_admin_bridge_report_history.py",
        "tests/admin/test_admin_bridge_task_launch.py",
        "tests/admin/test_admin_bridge_live_payloads.py",
    ):
        assert (repo_root / relative_path).exists()


def test_admin_bridge_delegates_task_launch_orchestration_to_bridge_module(repo_root: Path) -> None:
    admin_bridge_tree = _module_tree(repo_root / "src" / "admin_bridge.py")
    task_launch_api = (repo_root / "src" / "bridge" / "task_launch_api.py").read_text(
        encoding="utf-8"
    )
    run_script_fn = _find_function(admin_bridge_tree, "run_background_script")
    fetcher_args_fn = _find_function(admin_bridge_tree, "build_fetcher_args_from_payload")

    assert "src.bridge" in _imported_modules(admin_bridge_tree)
    assert "_get_task_launch_api" in _function_call_names(run_script_fn)
    assert "_get_task_launch_api" in _function_call_names(fetcher_args_fn)
    assert "build_fetcher_args_from_payload" in _function_call_names(fetcher_args_fn)
    assert "run_background_script" in _function_call_names(run_script_fn)
    admin_bridge = (repo_root / "src" / "admin_bridge.py").read_text(encoding="utf-8")
    assert "--max-workers" not in admin_bridge
    assert "class TaskLaunchApi:" in task_launch_api
    assert "def run_background_script(" in task_launch_api
    assert "def build_fetcher_args_from_payload(" in task_launch_api


def test_admin_bridge_delegates_ops_orchestration_to_bridge_module(repo_root: Path) -> None:
    admin_bridge_tree = _module_tree(repo_root / "src" / "admin_bridge.py")
    ops_api = (repo_root / "src" / "bridge" / "ops_api.py").read_text(encoding="utf-8")
    assert "src.bridge" in _imported_modules(admin_bridge_tree)
    failed_names_fn = _find_function(admin_bridge_tree, "_failed_source_names_from_latest_report")
    sync_history_fn = _find_function(admin_bridge_tree, "sync_history_from_reports")
    ops_health_fn = _find_function(admin_bridge_tree, "compute_ops_health")
    fetcher_metrics_fn = _find_function(admin_bridge_tree, "compute_fetcher_metrics")

    assert "_get_ops_api" in _function_call_names(failed_names_fn)
    assert "failed_source_names_from_latest_report" in _function_call_names(failed_names_fn)
    assert "_get_ops_api" in _function_call_names(sync_history_fn)
    assert "sync_history_from_reports" in _function_call_names(sync_history_fn)
    assert "_get_ops_api" in _function_call_names(ops_health_fn)
    assert "compute_ops_health" in _function_call_names(ops_health_fn)
    assert "_get_ops_api" in _function_call_names(fetcher_metrics_fn)
    assert "compute_fetcher_metrics" in _function_call_names(fetcher_metrics_fn)
    assert "class OpsApi:" in ops_api
    assert "def failed_source_names_from_latest_report(" in ops_api
    assert "def sync_history_from_reports(" in ops_api
    assert "def compute_ops_health(" in ops_api
    assert "def compute_fetcher_metrics(" in ops_api


def test_bridge_api_defaults_registry_identity_helpers_to_source_registry(repo_root: Path) -> None:
    bridge_api = (repo_root / "src" / "bridge" / "api.py").read_text(encoding="utf-8")
    assert (
        "from src.source_registry import normalize_source_url as normalize_source_url_impl"
        in bridge_api
    )
    assert "from src.source_registry import source_identity as source_identity_impl" in bridge_api
    assert (
        "from src.source_registry import source_url_fingerprint as source_url_fingerprint_impl"
        in bridge_api
    )
    assert "from src.source_registry import unique_sources as unique_sources_impl" in bridge_api
    assert (
        "unique_sources: Callable[[list[dict[str, Any]]], list[dict[str, Any]]] = unique_sources_impl"
        in bridge_api
    )
    assert "source_identity: Callable[[dict[str, Any]], str] = source_identity_impl" in bridge_api


def test_bridge_api_prefers_registry_service_identity_helpers_when_present(repo_root: Path) -> None:
    bridge_api = (repo_root / "src" / "bridge" / "api.py").read_text(encoding="utf-8")
    registry_service = (repo_root / "src" / "bridge" / "registry_service.py").read_text(
        encoding="utf-8"
    )
    assert 'if self._field_is_default("unique_sources"):' in bridge_api
    assert 'if self._field_is_default("source_identity"):' in bridge_api
    assert 'if self._field_is_default("source_url_fingerprint"):' in bridge_api
    assert 'if self._field_is_default("normalize_source_url"):' in bridge_api
    assert (
        "def unique_sources(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:"
        in registry_service
    )
    assert "def source_identity(row: dict[str, Any]) -> str:" in registry_service
