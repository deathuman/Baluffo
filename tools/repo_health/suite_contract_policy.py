import ast
import importlib
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


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
    root = ROOT / "tests"
    test_files = sorted(root.rglob("test_*.py"))
    pattern = re.compile(r"^(class\s+\w+|def\s+test_)", re.MULTILINE)

    for path in test_files:
        text = path.read_text(encoding="utf-8")
        assert pattern.search(text), (
            f"{path.name} matches test discovery but does not define a real test case."
        )


def test_frontend_test_patterns_disallow_generated_manifest_aggregators() -> None:
    repo_root = ROOT
    tests_root = repo_root / "tests"
    frontend_unit_root = tests_root / "frontend" / "unit"
    aggregator_paths = sorted(tests_root.rglob("all.test.mjs"))
    assert aggregator_paths == [], (
        "Generated all.test.mjs aggregators are retired; frontend unit tests run through direct Node discovery."
    )

    sync_script_import = "scripts/sync_frontend_unit_manifest.mjs"
    real_frontend_test_pattern = re.compile(r"\btest\s*\(")
    for path in sorted(frontend_unit_root.glob("*.test.mjs")):
        text = path.read_text(encoding="utf-8")
        assert real_frontend_test_pattern.search(text), (
            f"{path.relative_to(repo_root)} matches the frontend discovered test pattern but does not define a real test."
        )
        assert sync_script_import not in text, (
            f"{path.relative_to(repo_root)} should not import retired frontend unit manifest tooling."
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


def test_ops_task_live_root_stays_split_by_task_type(repo_root: Path) -> None:
    target = repo_root / "src" / "bridge" / "ops_task_live.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert "from src.bridge import ops_task_fetch_live as ops_task_fetch_live_mod" in text
    assert "from src.bridge import ops_task_discovery_live as ops_task_discovery_live_mod" in text
    assert "from src.bridge import ops_task_projection as ops_task_projection_mod" in text
    assert "build_fetch_live_payload=ops_task_fetch_live_mod.build_fetch_live_payload" in text
    assert (
        "build_discovery_live_payload=ops_task_discovery_live_mod.build_discovery_live_payload"
        in text
    )
    assert "def build_fetch_live_payload(" not in text
    assert "def build_discovery_live_payload(" not in text
    assert "def build_sync_live_payload(" not in text
    assert "def resolve_projected_live_context(" not in text
    assert {
        "coerce_non_negative_int",
        "fetch_progress_counts",
        "count_present",
        "live_task_signal_is_recent",
        "live_task_artifact_recently_updated",
        "live_task_heartbeat_at",
        "build_pipeline_task_progress",
        "build_current_task_state_payload",
        "get_task_live_payload",
    } <= function_names
    assert len(text.splitlines()) <= 140, "ops_task_live.py drifted back toward monolith size"


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


def test_source_discovery_gamesmap_root_stays_thin_compat_surface(repo_root: Path) -> None:
    from src.source_discovery import gamesmap as gamesmap_module

    target = repo_root / "src" / "source_discovery" / "gamesmap.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    assert "from . import gamesmap_candidates as gamesmap_candidates_mod" in text
    assert "from . import gamesmap_parsing as gamesmap_parsing_mod" in text
    assert "gamesmap_candidates_mod.root = sys.modules[__name__]" in text
    assert _top_level_function_names(tree) == []
    assert callable(gamesmap_module.discover_gamesmap_candidates)
    assert callable(gamesmap_module.gamesmap_matches_category)
    assert callable(gamesmap_module.parse_gamesmap_detail_page)
    assert callable(gamesmap_module.parse_gamesmap_index_entries)
    assert callable(gamesmap_module._parse_gamesmap_index_entries_with_diagnostics)
    assert len(text.splitlines()) <= 40, (
        "source_discovery/gamesmap.py drifted back toward monolith size"
    )


def test_source_discovery_reporting_root_stays_thin_compat_surface(repo_root: Path) -> None:
    from src.source_discovery import reporting as discovery_reporting

    target = repo_root / "src" / "source_discovery" / "reporting.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    assert "from .reporting_backlog import" in text
    assert "from .reporting_candidates import" in text
    assert "from .reporting_progress import (" in text
    assert _top_level_function_names(tree) == []
    assert callable(discovery_reporting.build_stage_summary)
    assert callable(discovery_reporting.emit_log)
    assert callable(discovery_reporting.merge_candidate_streams)
    assert callable(discovery_reporting.build_m5_strategic_backlog)
    assert len(text.splitlines()) <= 30, (
        "source_discovery/reporting.py drifted back toward implementation ownership"
    )


def test_source_discovery_web_search_root_stays_thin_compat_surface(repo_root: Path) -> None:
    from src.source_discovery import web_search as discovery_web_search

    target = repo_root / "src" / "source_discovery" / "web_search.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    assert "from .web_search_candidates import (" in text
    assert "from .web_search_extract import (" in text
    assert "from .web_search_fetch import (" in text
    assert _top_level_function_names(tree) == []
    assert callable(discovery_web_search.fetch_text)
    assert callable(discovery_web_search.async_fetch_text_httpx)
    assert callable(discovery_web_search.infer_provider_candidates_from_html)
    assert not hasattr(discovery_web_search, "discover_web_search_candidates")
    assert len(text.splitlines()) <= 40, (
        "source_discovery/web_search.py drifted back toward implementation ownership"
    )


def test_jobs_fetcher_compat_exports_use_leaf_common_modules_not_root_symbol_barrel(
    repo_root: Path,
) -> None:
    target = repo_root / "src" / "jobs" / "fetcher_compat_exports.py"
    text = target.read_text(encoding="utf-8")

    assert "from src.jobs import common as _common" not in text
    assert "from src.jobs.common import config as common_config_mod" in text
    assert "from src.jobs.common import diagnostics as common_diagnostics_mod" in text
    assert "from src.jobs.common import fetch as common_fetch_mod" in text


def test_jobs_package_private_helper_boundaries_stay_in_repo_guardrails(
    repo_root: Path,
) -> None:
    failures: list[str] = []
    checks = (
        (
            "src/jobs/pipeline.py",
            (
                "from . import pipeline_run_setup as pipeline_run_setup_mod",
                "from . import pipeline_execution_flow as pipeline_execution_flow_mod",
            ),
            ("from src.jobs import common as common",),
            False,
        ),
        (
            "src/jobs/state.py",
            (),
            ("def normalize_source_state_payload(", "def apply_job_lifecycle_state("),
            True,
        ),
        (
            "src/jobs/pipeline_stage_source_execution.py",
            (),
            ("def emit_progress_line(", "def mark_task_started(", "def execute_loader("),
            False,
        ),
        (
            "src/jobs/pipeline_runtime.py",
            (),
            ("def initialize_task_runtime(", "def build_active_pipeline_summary("),
            True,
        ),
        (
            "src/jobs/state_source_state.py",
            (),
            (
                "def normalize_source_state_payload(",
                "def apply_successful_source_state(",
                "def apply_browser_escalation_state(",
            ),
            True,
        ),
        (
            "src/jobs/common/contracts.py",
            (),
            ("import src.jobs_fetcher",),
            True,
        ),
        (
            "src/jobs/reporting.py",
            (),
            ("import src.jobs_fetcher",),
            True,
        ),
        (
            "src/jobs/adapters/static.py",
            (),
            ("from src.jobs_fetcher import",),
            False,
        ),
    )

    for rel_path, required_tokens, forbidden_tokens, optional in checks:
        path = repo_root / rel_path
        if optional and not path.exists():
            continue
        if not path.exists():
            failures.append(f"{rel_path} is missing.")
            continue
        text = path.read_text(encoding="utf-8")
        for token in required_tokens:
            if token not in text:
                failures.append(f"{rel_path} must contain `{token}`.")
        for token in forbidden_tokens:
            if token in text:
                failures.append(f"{rel_path} must not contain `{token}`.")

    assert not failures, "Jobs package private helper boundary drift:\n- " + "\n- ".join(failures)


def test_jobs_common_broad_barrel_imports_stay_retired(repo_root: Path) -> None:
    allowed_submodules = {
        "config",
        "contracts",
        "datetime_utils",
        "diagnostics",
        "fetch",
        "health",
        "heuristics",
        "numbers",
        "registry",
        "registry_defaults",
        "social",
        "sources",
        "taxonomy",
        "url",
    }
    offenders: list[str] = []
    excluded_paths = {"tests/test_jobs_package.py"}
    for root_name in ("src", "tests"):
        for target in (repo_root / root_name).rglob("*.py"):
            if target.relative_to(repo_root).as_posix() in excluded_paths:
                continue
            tree = _module_tree(target)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name == "src.jobs.common":
                            offenders.append(str(target.relative_to(repo_root)))
                if isinstance(node, ast.ImportFrom):
                    if node.module == "src.jobs" and any(
                        alias.name == "common" for alias in node.names
                    ):
                        offenders.append(str(target.relative_to(repo_root)))
                    if node.module == "src.jobs.common":
                        for alias in node.names:
                            if alias.name not in allowed_submodules:
                                offenders.append(str(target.relative_to(repo_root)))

    assert not offenders, "Found retired broad src.jobs.common imports:\n- " + "\n- ".join(
        sorted(set(offenders))
    )


def test_jobs_legacy_runners_module_stays_retired(repo_root: Path) -> None:
    legacy_module = repo_root / "src" / "jobs" / "common" / "legacy_runners.py"
    assert not legacy_module.exists(), "src/jobs/common/legacy_runners.py should not return."

    offenders: list[str] = []
    for root_name in ("src", "tests"):
        for target in (repo_root / root_name).rglob("*.py"):
            tree = _module_tree(target)
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.ImportFrom)
                    and node.module == "src.jobs.common.legacy_runners"
                ):
                    offenders.append(str(target.relative_to(repo_root)))
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name == "src.jobs.common.legacy_runners":
                            offenders.append(str(target.relative_to(repo_root)))

    assert not offenders, "Found retired src.jobs.common.legacy_runners imports:\n- " + "\n- ".join(
        sorted(set(offenders))
    )


def test_jobs_fetcher_test_helper_barrel_stays_retired(repo_root: Path) -> None:
    offenders: list[str] = []
    for target in (repo_root / "tests").rglob("*.py"):
        text = target.read_text(encoding="utf-8")
        if "tests.jobs_fetcher_helpers" in text:
            offenders.append(str(target.relative_to(repo_root)))

    jobs_static_helper = repo_root / "tests" / "jobs_static" / "_helpers.py"
    jobs_static_helper_text = jobs_static_helper.read_text(encoding="utf-8")
    if "__all__ = [name for name in globals()" in jobs_static_helper_text:
        offenders.append("tests/jobs_static/_helpers.py uses a dynamic __all__ helper barrel.")

    assert not offenders, "Found retired jobs fetcher helper barrel patterns:\n- " + "\n- ".join(
        sorted(set(offenders))
    )


def test_jobs_common_migrated_modules_keep_direct_owning_imports(repo_root: Path) -> None:
    social_adapter = (repo_root / "src" / "jobs" / "adapters" / "social.py").read_text(
        encoding="utf-8"
    )
    registry_module = (repo_root / "src" / "jobs" / "registry.py").read_text(encoding="utf-8")
    static_sources = (repo_root / "src" / "jobs" / "adapters" / "static_sources.py").read_text(
        encoding="utf-8"
    )
    static_scrapy_adapter = (
        repo_root / "src" / "jobs" / "adapters" / "static_scrapy.py"
    ).read_text(encoding="utf-8")

    failures: list[str] = []
    if (
        "from src.jobs.common.diagnostics import SOURCE_DIAGNOSTICS, set_source_diagnostics"
        not in social_adapter
    ):
        failures.append("src/jobs/adapters/social.py must import diagnostics directly.")
    if not (
        "DEFAULT_STUDIO_SOURCE_REGISTRY" in registry_module
        and "REDUNDANT_STATIC_IF_PROVIDER" in registry_module
    ):
        failures.append("src/jobs/registry.py must own registry defaults.")
    if "registry_entries as common_registry_entries" not in registry_module:
        failures.append("src/jobs/registry.py must import common registry entries directly.")
    if "from src.jobs.common.diagnostics import set_source_diagnostics" not in static_sources:
        failures.append("src/jobs/adapters/static_sources.py must import diagnostics directly.")
    if "from src.jobs.registry import registry_entries" not in static_scrapy_adapter:
        failures.append("src/jobs/adapters/static_scrapy.py must import registry entries directly.")

    assert not failures, "Jobs common migration import drift:\n- " + "\n- ".join(failures)


def test_jobs_fetcher_facade_stays_lazy_and_small(repo_root: Path) -> None:
    from src import jobs_fetcher

    target = repo_root / "src" / "jobs_fetcher.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    function_names = set(_top_level_function_names(tree))
    assert "from src.jobs import fetcher_compat_exports as fetcher_compat_exports_mod" in text
    assert "from src.jobs import fetcher_compat_runtime as fetcher_compat_runtime_mod" in text
    assert "fetcher_compat_runtime_mod.root = sys.modules[__name__]" in text
    assert {
        "_ensure_repo_on_path",
        "__getattr__",
        "__dir__",
        "parse_args",
        "main",
        "run_pipeline",
        "run_scrapy_static_source",
        "registry_entries",
        "build_redirect_resolver",
        "maybe_fetch_kojima_job_listing_html",
    } <= function_names
    assert "_COMPAT_MODULE_EXPORTS" in text
    assert "def _module_attr_exports(" not in text
    assert "parse_google_sheets_csv = _parsers.parse_google_sheets_csv" not in text
    assert "run_static_studio_pages_source = _static.run_static_studio_pages_source" not in text
    assert "raise SystemExit(main())" in text
    assert len(text.splitlines()) <= 280, "jobs_fetcher root drifted back toward monolith size"
    assert callable(jobs_fetcher.canonicalize_job)
    assert callable(jobs_fetcher.canonicalize_job_with_reason)
    assert callable(jobs_fetcher.canonicalize_google_sheets_rows)
    assert callable(jobs_fetcher.deduplicate_jobs)
    assert "canonicalize_job" not in jobs_fetcher.__all__
    assert "canonicalize_job_with_reason" not in jobs_fetcher.__all__
    assert "canonicalize_google_sheets_rows" not in jobs_fetcher.__all__
    assert "deduplicate_jobs" not in jobs_fetcher.__all__


def test_jobs_pipeline_root_stays_thin_orchestration_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "pipeline.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert "from . import pipeline_run_setup as pipeline_run_setup_mod" in text
    assert "from . import pipeline_execution_flow as pipeline_execution_flow_mod" in text
    assert {"default_source_loaders", "run_pipeline", "parse_args", "main"} <= function_names
    assert "def _canonicalize_existing_output_row(" not in text
    assert "def _apply_final_location_quality_guardrail(" not in text
    assert "def build_runtime_timing_summary(" not in text
    assert "resolve_fetch_text_impl(" not in text
    assert "initialize_task_runtime(" not in text
    assert len(text.splitlines()) <= 520, "jobs pipeline root drifted back toward monolith size"


def test_jobs_state_root_is_optional_internal_fetcher_facade(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "state.py"
    if not target.exists():
        return
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    assert "def normalize_source_state_payload(" not in text
    assert "def apply_job_lifecycle_state(" not in text
    assert "import src.jobs_fetcher" not in text
    assert len(_top_level_function_names(tree)) <= 2


def test_jobs_common_contracts_root_is_optional_internal_fetcher_facade(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "common" / "contracts.py"
    if not target.exists():
        return
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    assert "import src.jobs_fetcher" not in text
    assert len(_top_level_function_names(tree)) <= 4


def test_jobs_reporting_root_is_optional_internal_fetcher_facade(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "reporting.py"
    if not target.exists():
        return
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    assert "import src.jobs_fetcher" not in text
    assert len(_top_level_function_names(tree)) <= 8


def test_source_sync_root_stays_thin_compat_surface(repo_root: Path) -> None:
    from src import source_sync
    from src.shared.utils import now_iso as shared_now_iso
    from src.shared.utils import now_utc as shared_now_utc

    target = repo_root / "src" / "source_sync.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    top_level_imports = _top_level_imported_modules(tree)
    function_names = set(_top_level_function_names(tree))

    assert {
        "src.source_sync_config",
        "src.source_sync_crypto",
        "src.source_sync_runtime",
        "src.source_sync_snapshot",
    } <= top_level_imports
    assert source_sync.now_iso is shared_now_iso
    assert source_sync.now_utc is shared_now_utc
    assert "now_iso" not in function_names
    assert function_names.isdisjoint(
        {
            "_snapshot_transition_text",
            "_backfill_snapshot_transition_metadata",
            "_canonicalize_snapshot_rows",
            "_row_transition_score",
            "_row_bucket_rank",
            "_row_merge_key",
            "_choose_more_recent_row",
            "_asn1_read_tlv",
            "_asn1_read_children",
            "_asn1_integer",
            "_pem_to_der",
            "_parse_rsa_private_key_der",
        }
    )
    assert len(text.splitlines()) <= 560, "source_sync root drifted back toward monolith size"


def test_local_data_store_root_stays_thin_compat_surface(repo_root: Path) -> None:
    from src import local_data_store

    target = repo_root / "src" / "local_data_store.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    class_defs = {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}
    local_data_store_class = class_defs["LocalDataStore"]
    method_names = {
        node.name for node in local_data_store_class.body if isinstance(node, ast.FunctionDef)
    }

    assert "from src import local_data_store_profiles as local_data_store_profiles_mod" in text
    assert "from src import local_data_store_saved_jobs as local_data_store_saved_jobs_mod" in text
    assert (
        "from src import local_data_store_attachments as local_data_store_attachments_mod" in text
    )
    assert "from src import local_data_store_backup as local_data_store_backup_mod" in text
    assert "from src.local_data_store_shared import (" in text
    assert "def normalize_saved_job(" not in text
    assert "def merge_saved_job(" not in text
    assert "def add_activity(" not in text
    assert "def _import_saved_jobs(" not in text
    assert "def _import_activity_rows(" not in text
    assert {
        "__init__",
        "sign_in",
        "sign_out",
        "get_current_user",
        "list_profiles",
        "list_saved_jobs",
        "get_saved_job_keys",
        "save_job_for_user",
        "remove_saved_job_for_user",
        "update_application_status",
        "update_job_notes",
        "list_activity_for_user",
        "list_attachments_for_job",
        "add_attachment_for_job",
        "get_attachment_blob",
        "delete_attachment_for_job",
        "export_profile_data",
        "import_profile_data",
        "get_admin_overview",
        "wipe_account_admin",
    } <= method_names
    assert local_data_store.LocalDataPaths.__name__ == "LocalDataPaths"
    assert local_data_store.LocalDataStore.__name__ == "LocalDataStore"
    assert callable(local_data_store.sanitize_job_url)
    assert callable(local_data_store.generate_job_key)
    assert callable(local_data_store.normalize_application_status)
    assert callable(local_data_store.can_transition_phase)
    assert callable(local_data_store.normalize_sector_value)
    assert len(text.splitlines()) <= 220, "local_data_store.py drifted back toward monolith size"


def test_source_sync_snapshot_leaf_owns_snapshot_merge_helpers(repo_root: Path) -> None:
    target = repo_root / "src" / "source_sync_snapshot.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    assert {
        "_snapshot_transition_text",
        "_backfill_snapshot_transition_metadata",
        "_canonicalize_snapshot_rows",
        "_row_transition_score",
        "_row_bucket_rank",
        "_row_merge_key",
        "_choose_more_recent_row",
    } <= set(_top_level_function_names(tree))
    assert "module._canonicalize_snapshot_rows" not in text
    assert "module._choose_more_recent_row" not in text


def test_packaged_desktop_smoke_root_stays_thin_compat_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "packaged_desktop_smoke.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert "from src.ship.packaged_smoke import common as packaged_smoke_common_mod" in text
    assert (
        "from src.ship.packaged_smoke import startup_metrics as packaged_smoke_startup_metrics_mod"
        in text
    )
    assert (
        "from src.ship.packaged_smoke import orchestrator as packaged_smoke_orchestrator_mod"
        in text
    )
    assert "from src.ship.packaged_smoke import rehearsals as packaged_smoke_rehearsals_mod" in text
    assert "packaged_smoke_orchestrator_mod.root = sys.modules[__name__]" in text
    assert "packaged_smoke_rehearsals_mod.root = sys.modules[__name__]" in text
    assert "return packaged_smoke_orchestrator_mod.run_packaged_smoke(args)" in text
    assert "def _start_packaged_sync_rehearsal_server(" not in text
    assert "def _wait_for_relaunched_runtime(" not in text
    assert "class _DesktopUpdateReleaseHandler" not in text
    assert {"run_packaged_smoke", "parse_args", "_print_failure_summary", "main"} <= function_names
    assert len(text.splitlines()) <= 420, (
        "packaged_desktop_smoke root drifted back toward monolith size"
    )


def test_desktop_updater_root_stays_thin_compat_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "ship" / "desktop_updater.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert "from src.ship import desktop_updater_ui as desktop_updater_ui_mod" in text
    assert "from src.ship import desktop_updater_release as desktop_updater_release_mod" in text
    assert "from src.ship import desktop_updater_install as desktop_updater_install_mod" in text
    assert "desktop_updater_ui_mod.root = sys.modules[__name__]" in text
    assert "desktop_updater_release_mod.root = sys.modules[__name__]" in text
    assert "desktop_updater_install_mod.root = sys.modules[__name__]" in text
    assert "HelperProgressWindow = desktop_updater_ui_mod.HelperProgressWindow" in text
    assert "run_install = desktop_updater_install_mod.run_install" in text
    assert {"parse_args", "main"} <= function_names
    assert "class HelperProgressWindow:" not in text
    assert "archive.extractall(temp_extract)" not in text
    assert "MessageBoxW(None" not in text
    assert len(text.splitlines()) <= 260, "desktop_updater root drifted back toward monolith size"


def test_python_leaf_modules_do_not_import_root_compatibility_surfaces(repo_root: Path) -> None:
    src_root = repo_root / "src"
    allowed_imports_by_path = {
        (src_root / "admin_bridge.py").resolve(): {"src.admin_bridge", "src.ship.desktop_update"},
        (src_root / "jobs_fetcher.py").resolve(): {"src.jobs_fetcher"},
        (src_root / "source_discovery.py").resolve(): {"src.source_discovery"},
        (src_root / "packaged_desktop_smoke.py").resolve(): {
            "src.packaged_desktop_smoke",
            "src.ship.desktop_update",
        },
        (src_root / "ship" / "desktop_update.py").resolve(): {"src.ship.desktop_update"},
        (src_root / "ship" / "desktop_app" / "__init__.py").resolve(): {"src.ship.desktop_update"},
        (src_root / "ship" / "desktop_updater.py").resolve(): {"src.ship.desktop_update"},
    }
    forbidden_imports = {
        "src.admin_bridge",
        "src.jobs_fetcher",
        "src.source_discovery",
        "src.packaged_desktop_smoke",
        "src.ship.desktop_update",
        "src.ship.desktop_updater",
    }

    offenders: list[str] = []
    for path in sorted(src_root.rglob("*.py")):
        imports = _imported_modules(_module_tree(path))
        allowed_imports = allowed_imports_by_path.get(path.resolve(), set())
        bad = sorted((imports & forbidden_imports) - allowed_imports)
        if bad:
            offenders.append(f"{path.relative_to(repo_root)} -> {', '.join(bad)}")

    assert not offenders, (
        "Leaf Python modules should not import root compatibility surfaces directly:\n- "
        + "\n- ".join(offenders)
    )


def test_jobs_pipeline_state_helpers_do_not_import_jobs_roots(repo_root: Path) -> None:
    forbidden_imports = {"src.jobs_fetcher", "src.jobs.pipeline", "src.jobs.state"}
    offenders: list[str] = []
    for relative_path in (
        "src/jobs/pipeline_run_setup.py",
        "src/jobs/pipeline_execution_flow.py",
        "src/jobs/pipeline_finalize.py",
        "src/jobs/state_source_state.py",
        "src/jobs/state_lifecycle.py",
    ):
        imports = _imported_modules(_module_tree(repo_root / relative_path))
        bad = sorted(imports & forbidden_imports)
        if bad:
            offenders.append(f"{relative_path} -> {', '.join(bad)}")
    assert not offenders, (
        "Pipeline/state helper leaves should not import jobs compatibility roots directly:\n- "
        + "\n- ".join(offenders)
    )


def test_jobs_leaf_closeout_helpers_do_not_import_jobs_roots(repo_root: Path) -> None:
    forbidden_imports = {
        "src.jobs_fetcher",
        "src.jobs.pipeline",
        "src.jobs.pipeline_runtime",
        "src.jobs.state",
        "src.jobs.reporting",
        "src.jobs.common.contracts",
    }
    offenders: list[str] = []
    for relative_path in (
        "src/jobs/pipeline_source_loop.py",
        "src/jobs/pipeline_source_results.py",
        "src/jobs/pipeline_source_progress.py",
        "src/jobs/pipeline_runtime_writers.py",
        "src/jobs/pipeline_runtime_summary.py",
        "src/jobs/state_source_records.py",
        "src/jobs/state_source_browser.py",
        "src/jobs/state_source_migration.py",
    ):
        path = repo_root / relative_path
        if not path.exists():
            continue
        imports = _imported_modules(_module_tree(path))
        bad = sorted(imports & forbidden_imports)
        if bad:
            offenders.append(f"{relative_path} -> {', '.join(bad)}")
    assert not offenders, (
        "Jobs closeout helper leaves should not import jobs compatibility roots directly:\n- "
        + "\n- ".join(offenders)
    )


def test_jobs_contracts_reporting_helpers_do_not_import_jobs_roots(repo_root: Path) -> None:
    forbidden_imports = {"src.jobs_fetcher", "src.jobs.common.contracts", "src.jobs.reporting"}
    offenders: list[str] = []
    for relative_path in (
        "src/jobs/common/contracts_runtime.py",
        "src/jobs/common/contracts_source_reports.py",
        "src/jobs/common/contracts_task_state.py",
        "src/jobs/common/contracts_fetch_report.py",
        "src/jobs/reporting_breakdowns.py",
        "src/jobs/reporting_summary.py",
        "src/jobs/reporting_queues.py",
        "src/jobs/reporting_social.py",
    ):
        path = repo_root / relative_path
        if not path.exists():
            continue
        imports = _imported_modules(_module_tree(path))
        bad = sorted(imports & forbidden_imports)
        if bad:
            offenders.append(f"{relative_path} -> {', '.join(bad)}")
    assert not offenders, (
        "Jobs contracts/reporting helper leaves should not import compatibility roots directly:\n- "
        + "\n- ".join(offenders)
    )


def test_source_discovery_helper_leaves_do_not_import_discovery_roots(repo_root: Path) -> None:
    forbidden_imports = {
        "src.source_discovery",
        "src.source_discovery.orchestrator",
        "src.source_discovery.gamesmap",
        "src.source_discovery.reporting",
        "src.source_discovery.web_search",
    }
    offenders: list[str] = []
    for relative_path in (
        "src/source_discovery/gamesmap_cache.py",
        "src/source_discovery/gamesmap_parsing.py",
        "src/source_discovery/gamesmap_candidates.py",
        "src/source_discovery/reporting_progress.py",
        "src/source_discovery/reporting_candidates.py",
        "src/source_discovery/reporting_backlog.py",
        "src/source_discovery/web_search_fetch.py",
        "src/source_discovery/web_search_extract.py",
        "src/source_discovery/web_search_candidates.py",
    ):
        imports = _imported_modules(_module_tree(repo_root / relative_path))
        bad = sorted(imports & forbidden_imports)
        if bad:
            offenders.append(f"{relative_path} -> {', '.join(bad)}")
    assert not offenders, (
        "Source discovery helper leaves should not import discovery compatibility roots directly:\n- "
        + "\n- ".join(offenders)
    )


def test_static_adapter_root_stays_thin_orchestration_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "adapters" / "static.py"
    text = target.read_text(encoding="utf-8")

    assert "from . import static_detail as static_detail_mod" not in text
    assert "static_detail_mod.root = sys.modules[__name__]" not in text
    assert (
        "run_static_studio_pages_source = static_sources_mod.run_static_studio_pages_source" in text
    )
    assert "def static_source_shard(" not in text
    assert "def static_source_name_for_registry_row(" not in text
    assert "fetch_pages_batched" not in text
    assert len(text.splitlines()) <= 160, "static adapter root drifted back toward monolith size"


def test_static_helpers_is_optional_internal_fetcher_facade(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "adapters" / "static_helpers.py"
    if not target.exists():
        return
    text = target.read_text(encoding="utf-8")

    assert "def " not in text
    assert "class " not in text
    assert len(text.splitlines()) <= 80, (
        "static_helpers.py should be deleted or stay a thin temporary facade"
    )


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
    admin_entrypoint_api = (repo_root / "src" / "bridge" / "admin_entrypoint_api.py").read_text(
        encoding="utf-8"
    )
    bridge_bootstrap = (repo_root / "src" / "bridge" / "bootstrap.py").read_text(encoding="utf-8")
    bridge_api = (repo_root / "src" / "bridge" / "api.py").read_text(encoding="utf-8")
    sync_service = (repo_root / "src" / "bridge" / "sync_service.py").read_text(encoding="utf-8")
    assert "return admin_entrypoint_api_mod.build_bridge_api(config)" in admin_bridge
    assert "return bridge_bootstrap.build_bridge_api(" in admin_entrypoint_api
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
    admin_entrypoint_api = (repo_root / "src" / "bridge" / "admin_entrypoint_api.py").read_text(
        encoding="utf-8"
    )
    bridge_bootstrap = (repo_root / "src" / "bridge" / "bootstrap.py").read_text(encoding="utf-8")
    bridge_api = (repo_root / "src" / "bridge" / "api.py").read_text(encoding="utf-8")
    assert "return admin_entrypoint_api_mod.build_bridge_api(config)" in admin_bridge
    assert "return bridge_bootstrap.build_bridge_api(" in admin_entrypoint_api
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


def test_desktop_app_package_exports_required_compatibility_names() -> None:
    desktop_app = importlib.import_module("src.ship.desktop_app")
    required_names = {
        "_append_startup_trace",
        "_desktop_update_restart_snapshot",
        "_wait_for_browser_reveal",
        "_write_launch_diagnostics",
        "classify_desktop_startup_state",
        "latest_browser_heartbeat_ts",
        "show_native_message",
        "wait_for_browser_heartbeat",
        "wait_for_desktop_startup_ready",
        "watch_browser_session",
    }

    missing = sorted(name for name in required_names if not hasattr(desktop_app, name))
    assert not missing, (
        "src.ship.desktop_app must preserve its root-level monkeypatch and orchestration "
        f"surface during refactors; missing: {', '.join(missing)}"
    )


def test_desktop_launcher_root_stays_thin_private_orchestration_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "ship" / "desktop_app" / "launcher.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert "from .launcher_diagnostics import (" in text
    assert "from .launcher_flow import launch_desktop_app" in text
    assert (
        "from .launcher_recovery import _runtime_ports_need_retry, _should_retry_runtime_launch"
        in text
    )
    assert {"ensure_desktop_prerequisites", "parse_args", "main"} <= function_names
    assert "def _launch_runtime_children(" not in text
    assert "def _cleanup_runtime_processes(" not in text
    assert "def _wait_for_bridge_readiness(" not in text
    assert len(text.splitlines()) <= 160, (
        "desktop_app/launcher.py drifted back toward implementation ownership"
    )


def test_desktop_startup_root_stays_thin_private_compat_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "ship" / "desktop_app" / "startup.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert "from .startup_ready import (" in text
    assert "from .startup_watchdog import (" in text
    assert function_names == set()
    assert "def wait_for_desktop_startup_ready(" not in text
    assert "def publish_success_marker_when_ready_async(" not in text
    assert "def watch_browser_session(" not in text
    assert len(text.splitlines()) <= 80, (
        "desktop_app/startup.py drifted back toward implementation ownership"
    )


def test_desktop_updater_root_exports_required_compatibility_names() -> None:
    desktop_updater = importlib.import_module("src.ship.desktop_updater")
    required_names = {
        "DESKTOP_UPDATE_MANIFEST_ASSET",
        "DesktopUpdatePaths",
        "HelperProgressWindow",
        "_show_message",
        "fetch_json",
        "run_install",
        "update_manager",
        "validate_desktop_manifest",
    }

    missing = sorted(name for name in required_names if not hasattr(desktop_updater, name))
    assert not missing, (
        "src.ship.desktop_updater must keep its helper/install/release compatibility exports "
        f"available at the root; missing: {', '.join(missing)}"
    )


def test_packaged_desktop_smoke_root_exports_required_compatibility_names() -> None:
    packaged_smoke = importlib.import_module("src.packaged_desktop_smoke")
    required_names = {
        "DESKTOP_UPDATE_MANIFEST_ASSET",
        "DESKTOP_UPDATE_SCHEMA_VERSION",
        "DESKTOP_UPDATER_VERSION",
        "Ed25519SigningClass",
        "compute_sha256",
        "fetch_json",
        "get_app_version",
        "read_startup_metrics_file",
        "run_packaged_smoke",
        "run_portable_build",
        "select_startup_probe_browser",
        "sign_manifest",
        "write_startup_summary",
    }

    missing = sorted(name for name in required_names if not hasattr(packaged_smoke, name))
    assert not missing, (
        "src.packaged_desktop_smoke must keep its stable smoke-runner surface intact during "
        f"refactors; missing: {', '.join(missing)}"
    )


def test_packaged_update_rehearsal_uses_leaf_manifest_helpers(repo_root: Path) -> None:
    target = repo_root / "src" / "ship" / "packaged_smoke" / "rehearsal_update.py"
    text = target.read_text(encoding="utf-8")
    forbidden = {
        "deps.desktop_update_mod.DESKTOP_UPDATE_MANIFEST_ASSET",
        "deps.desktop_update_mod.DESKTOP_UPDATE_SCHEMA_VERSION",
        "deps.desktop_update_mod.DESKTOP_UPDATER_VERSION",
        "deps.desktop_update_mod.Ed25519PrivateKey",
        "deps.desktop_update_mod.PUBLIC_KEYS_FILE",
        "deps.desktop_update_mod.compute_sha256",
        "deps.desktop_update_mod.DESKTOP_UPDATE_CHANNEL",
        "deps.desktop_update_mod.get_app_version",
        "deps.desktop_update_mod.sign_manifest",
    }

    present = sorted(item for item in forbidden if item in text)

    assert not present, (
        "packaged update rehearsal must use pure leaf manifest/signing helpers through "
        f"the packaged smoke root, not desktop_update facade symbols: {', '.join(present)}"
    )


def test_source_discovery_compatibility_surfaces_export_required_names() -> None:
    compatibility_surfaces = {
        "src.source_discovery.reporting": {
            "build_discovery_task_progress",
            "build_m5_strategic_backlog",
            "merge_candidate_streams",
            "write_discovery_progress_report",
        },
        "src.source_discovery.web_search": {
            "extract_links_from_html",
            "fetch_text",
            "infer_web_candidate",
        },
    }

    for module_name, required_names in compatibility_surfaces.items():
        module = importlib.import_module(module_name)
        exported = set(getattr(module, "__all__", ()))
        assert required_names <= exported, (
            f"{module_name} must keep an explicit compatibility export list for refactor safety."
        )
        missing = sorted(name for name in required_names if not hasattr(module, name))
        assert not missing, f"{module_name} is missing required exports: {', '.join(missing)}"

    gamesmap = importlib.import_module("src.source_discovery.gamesmap")
    gamesmap_required_names = {
        "discover_gamesmap_candidates",
        "fetch_text",
        "gamesmap_matches_category",
        "infer_web_candidate",
        "parse_gamesmap_detail_page",
    }
    gamesmap_missing = sorted(
        name for name in gamesmap_required_names if not hasattr(gamesmap, name)
    )
    assert not gamesmap_missing, (
        "src.source_discovery.gamesmap must preserve its stable compatibility exports during "
        f"refactors; missing: {', '.join(gamesmap_missing)}"
    )


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
        "tests/jobs_static/test_scrapy_static_runtime.py",
        "tests/jobs_static/test_static_source_execution.py",
    ):
        text = (repo_root / relative_path).read_text(encoding="utf-8")
        assert "from ._helpers import *" not in text


def test_admin_bridge_root_stays_thin_entrypoint_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "admin_bridge.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert "from src.bridge import admin_entrypoint_api as admin_entrypoint_api_mod" in text
    assert "from src.bridge import admin_entrypoint_runtime as admin_entrypoint_runtime_mod" in text
    assert (
        "from src.bridge import admin_entrypoint_services as admin_entrypoint_services_mod" in text
    )
    assert "from src.bridge import admin_registry_api as admin_registry_api_mod" in text
    assert "from src.bridge import admin_task_runtime as admin_task_runtime_mod" in text
    assert "admin_entrypoint_api_mod.root = sys.modules[__name__]" in text
    assert "admin_entrypoint_runtime_mod.root = sys.modules[__name__]" in text
    assert "admin_entrypoint_services_mod.root = sys.modules[__name__]" in text
    assert "admin_registry_api_mod.root = sys.modules[__name__]" in text
    assert "admin_task_runtime_mod.root = sys.modules[__name__]" in text
    assert "def build_bridge_api(" in text
    assert "return admin_entrypoint_api_mod.build_bridge_api(config)" in text
    assert "smoke_runtime: dict[str, Any]" not in text
    assert "find_existing_static_source_by_studio_domain(" not in text
    assert function_names.isdisjoint(
        {
            "_get_sync_service",
            "_get_sync_state",
            "_get_registry_service",
            "_get_discovery_service",
            "_get_task_launch_api",
            "_get_ops_api",
            "_get_pipeline_service",
            "_get_desktop_update_service",
            "_log_enabled",
            "bridge_log",
            "configure_runtime_paths",
            "startup_banner",
            "append_startup_metric",
            "read_startup_metrics",
            "get_desktop_session_payload",
            "update_desktop_session_lifecycle",
            "owner_session_should_exit",
            "parse_iso",
            "pid_is_running",
            "desktop_local_data_store",
            "normalize_state",
            "load_state",
            "summarize_state",
            "persist_state",
            "persist_state_and_auto_sync",
            "move_entries",
            "build_manual_candidate",
            "add_manual_source",
            "check_static_source",
            "_read_tasks_config",
            "_set_sync_status",
            "get_sync_status_payload",
            "_sync_guard",
            "sync_pull_sources",
            "sync_push_sources",
            "startup_sync_pull",
            "sync_task_running",
            "wait_for_sync_tasks",
            "_mark_discovery_sync_finished",
            "_maybe_trigger_auto_sync_push",
            "_current_fetch_output_count",
            "get_jobs_pipeline_status_payload",
            "_wait_for_sync_completion",
            "start_fetcher_task",
            "start_jobs_pipeline_task",
        }
    )
    assert len(text.splitlines()) <= 633, "admin bridge root drifted back toward monolith size"


def test_jobs_pipeline_stage_execution_root_stays_thin_private_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "pipeline_stage_source_execution.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert {
        "resolve_fetch_browser_fallback_helper",
        "_build_capped_try_playwright",
        "_default_adapter_for_loader",
        "_is_provider_family_adapter",
        "_is_social_subsource_report",
        "_failure_bucket_from_zero_extract_context",
    } <= function_names
    assert "def emit_progress_line(" not in text
    assert "def mark_task_started(" not in text
    assert "def execute_loader(" not in text
    assert len(text.splitlines()) <= 180, (
        "pipeline_stage_source_execution.py drifted back toward implementation ownership"
    )


def test_jobs_pipeline_runtime_root_is_optional_internal_fetcher_facade(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "pipeline_runtime.py"
    if not target.exists():
        return
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")

    assert "def initialize_task_runtime(" not in text
    assert "def build_active_pipeline_summary(" not in text
    assert "import src.jobs_fetcher" not in text
    assert len(_top_level_function_names(tree)) <= 2


def test_jobs_state_source_state_root_is_optional_internal_fetcher_facade(
    repo_root: Path,
) -> None:
    target = repo_root / "src" / "jobs" / "state_source_state.py"
    if not target.exists():
        return
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert {"_apply_report_to_entry", "update_source_state_rows"} <= function_names
    assert "def normalize_source_state_payload(" not in text
    assert "def apply_successful_source_state(" not in text
    assert "def apply_browser_escalation_state(" not in text
    assert "import src.jobs_fetcher" not in text


def test_bridge_post_routes_root_stays_thin_registration_surface(repo_root: Path) -> None:
    target = repo_root / "src" / "bridge" / "routes" / "post_routes.py"
    tree = _module_tree(target)
    text = target.read_text(encoding="utf-8")
    function_names = set(_top_level_function_names(tree))

    assert "from . import post_routes_admin as post_routes_admin_mod" in text
    assert "from . import post_routes_local_data as post_routes_local_data_mod" in text
    assert "from . import post_routes_update as post_routes_update_mod" in text
    assert function_names == {"handle_post"}
    assert "def _transition_registry_row(" not in text
    assert "api.store.sign_in(" not in text
    assert "api.desktop_update_service.get_status_payload(" not in text
    assert len(text.splitlines()) <= 60, (
        "bridge/routes/post_routes.py drifted back toward implementation ownership"
    )


def test_bridge_routes_use_public_response_writer(repo_root: Path) -> None:
    route_root = repo_root / "src" / "bridge" / "routes"
    offenders: list[str] = []
    for path in sorted(route_root.glob("*_routes*.py")):
        text = path.read_text(encoding="utf-8")
        if "handler._send_json" in text or "handler._send_bytes" in text:
            offenders.append(path.relative_to(repo_root).as_posix())
    assert not offenders, (
        "Bridge route modules must use the public BridgeResponseWriter surface, "
        "not private handler send methods:\n- " + "\n- ".join(offenders)
    )


def test_bridge_route_ble001_suppressions_stay_in_boundary_helper(repo_root: Path) -> None:
    route_root = repo_root / "src" / "bridge" / "routes"
    target_files = (
        route_root / "get_routes.py",
        route_root / "post_routes_admin.py",
        route_root / "post_routes_local_data.py",
        route_root / "post_routes_update.py",
    )
    offenders = [
        str(path.relative_to(repo_root))
        for path in target_files
        if "# noqa: BLE001" in path.read_text(encoding="utf-8")
    ]
    assert not offenders, (
        "Target bridge route modules must use src/bridge/routes/error_boundary.py for "
        "controlled broad exception handling:\n- " + "\n- ".join(offenders)
    )


def test_community_adapter_ble001_suppressions_stay_in_recovery_helper(
    repo_root: Path,
) -> None:
    target = repo_root / "src" / "jobs" / "adapters" / "community" / "__init__.py"
    assert "# noqa: BLE001" not in target.read_text(encoding="utf-8"), (
        "Community adapter URL fallback attempts must use "
        "src/jobs/adapters/recovery.py instead of local broad exception suppressions."
    )


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
    fetcher_args_mod = (repo_root / "src" / "bridge" / "task_launch_fetcher_args.py").read_text(
        encoding="utf-8"
    )
    source_runs_mod = (repo_root / "src" / "bridge" / "task_launch_source_runs.py").read_text(
        encoding="utf-8"
    )
    jobs_feed_mod = (repo_root / "src" / "bridge" / "task_launch_jobs_feed.py").read_text(
        encoding="utf-8"
    )
    lifecycle_mod = (repo_root / "src" / "bridge" / "task_launch_fetch_lifecycle.py").read_text(
        encoding="utf-8"
    )
    bs_storage_mod = (repo_root / "src" / "bridge" / "task_launch_bootstrap_storage.py").read_text(
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

    # Coordinator assertions
    assert "class TaskLaunchApi:" in task_launch_api
    assert "def run_background_script(" in task_launch_api
    assert "def start_fetcher_task(" in task_launch_api
    assert "def start_jobs_bootstrap_task(" in task_launch_api
    assert "def build_fetcher_args_from_payload(" in task_launch_api
    assert "def build_fetcher_extra_env_from_preset(" in task_launch_api

    # Module 1: fetcher args
    assert "def build_fetcher_args_from_payload(" in fetcher_args_mod
    assert "def build_fetcher_extra_env_from_preset(" in fetcher_args_mod
    assert "from src.bridge.task_launch_api" not in fetcher_args_mod

    # Module 2: source runs
    assert "def mirror_fetch_source_runs(" in source_runs_mod
    assert "from src.bridge.task_launch_api" not in source_runs_mod

    # Module 3: jobs feed
    assert "def mirror_jobs_feed_rows(" in jobs_feed_mod
    assert "from src.bridge.task_launch_api" not in jobs_feed_mod

    # Module 4: fetch lifecycle
    assert "class FetchLifecycleContext:" in lifecycle_mod
    assert "def watch_fetch_lifecycle(" in lifecycle_mod
    assert "def close_fetch_lifecycle_from_report(" in lifecycle_mod
    assert "def start_fetch_lifecycle_watch(" in lifecycle_mod
    assert "from src.bridge.task_launch_api" not in lifecycle_mod

    # Module 5: bootstrap storage
    assert "def snapshot_bootstrap_storage_state(" in bs_storage_mod
    assert "def restore_bootstrap_storage_state(" in bs_storage_mod
    assert "from src.bridge.task_launch_api" not in bs_storage_mod


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
    assert '("unique_sources", self.registry.unique_sources)' in bridge_api
    assert '("source_identity", self.registry.source_identity)' in bridge_api
    assert '("source_url_fingerprint", self.registry.source_url_fingerprint)' in bridge_api
    assert '("normalize_source_url", self.registry.normalize_source_url)' in bridge_api
    assert (
        "def unique_sources(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:"
        in registry_service
    )
    assert "def source_identity(row: dict[str, Any]) -> str:" in registry_service
