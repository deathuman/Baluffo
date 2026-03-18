import ast
import re
from pathlib import Path


def test_discovered_python_test_files_define_real_tests() -> None:
    root = Path(__file__).resolve().parent
    test_files = sorted(root.rglob("test_*.py"))
    pattern = re.compile(r"^(class\s+\w+Tests\s*\(|def\s+test_)", re.MULTILINE)

    for path in test_files:
        text = path.read_text(encoding="utf-8")
        assert pattern.search(text), f"{path.name} matches test discovery but does not define a real test case."


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

    def _call_name(node: ast.AST) -> str:
        if isinstance(node, ast.Attribute):
            left = _call_name(node.value)
            return f"{left}.{node.attr}" if left else node.attr
        if isinstance(node, ast.Name):
            return node.id
        return ""

    offenders: list[str] = []
    for path in sorted(bridge_root.rglob("*.py")):
        if path.resolve() in allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            target_name = ""
            value = None
            if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
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
            suspicious = suspicious or (
                isinstance(value, ast.List) and target_name.isupper()
            )
            suspicious = suspicious or (
                isinstance(value, ast.Call)
                and _call_name(value.func) in {"threading.RLock", "threading.Lock", "threading.Event"}
            )
            if suspicious:
                offenders.append(str(path.relative_to(repo_root)))
                break

    assert not offenders, (
        "Mutable bridge module state should stay in src/bridge/sync_state.py or "
        "src/bridge/server/runtime_state.py only:\n- " + "\n- ".join(offenders)
    )


def test_admin_bridge_keeps_registry_autosync_and_sync_normalization_out_of_entrypoint(repo_root: Path) -> None:
    target = repo_root / "src" / "admin_bridge.py"
    text = target.read_text(encoding="utf-8")
    assert "from src.bridge import registry_sync_flow as _registry_sync_flow" in text
    assert "_registry_sync_flow.persist_state_and_auto_sync(" in text
    assert "_registry_sync_flow.maybe_trigger_auto_sync_push(" in text
    assert "def _normalize_sync_settings" not in text
    assert "def _mask_sync_token" not in text


def test_sync_task_worker_logic_is_shared_between_admin_bridge_and_sync_service(repo_root: Path) -> None:
    admin_bridge = (repo_root / "src" / "admin_bridge.py").read_text(encoding="utf-8")
    sync_service = (repo_root / "src" / "bridge" / "sync_service.py").read_text(encoding="utf-8")
    assert "from src.bridge import sync_task_flow as _sync_task_flow" in admin_bridge
    assert "from src.bridge import sync_task_flow as _sync_task_flow" in sync_service
    assert "_sync_task_flow.run_sync_task_worker(" in admin_bridge
    assert "_sync_task_flow.run_sync_task_worker(" in sync_service


def test_bridge_api_uses_sync_service_for_sync_status_wiring(repo_root: Path) -> None:
    admin_bridge = (repo_root / "src" / "admin_bridge.py").read_text(encoding="utf-8")
    bridge_api = (repo_root / "src" / "bridge" / "api.py").read_text(encoding="utf-8")
    sync_service = (repo_root / "src" / "bridge" / "sync_service.py").read_text(encoding="utf-8")
    build_api_section = admin_bridge.split("def build_bridge_api", 1)[1].split("def load_saved_sync_settings", 1)[0]
    assert "sync_config_status=lambda: source_sync_module.config_status(refresh_sync_config())" not in build_api_section
    assert "set_sync_status=_set_sync_status" not in build_api_section
    assert 'if self._field_is_default("sync_config_status"):' in bridge_api
    assert 'if self._field_is_default("set_sync_status"):' in bridge_api
    assert "def sync_config_status(self) -> Dict[str, Any]:" in sync_service
    assert "def set_sync_status(" in sync_service


def test_bridge_api_defaults_registry_identity_helpers_to_source_registry(repo_root: Path) -> None:
    bridge_api = (repo_root / "src" / "bridge" / "api.py").read_text(encoding="utf-8")
    assert "from src.source_registry import normalize_source_url as normalize_source_url_impl" in bridge_api
    assert "from src.source_registry import source_identity as source_identity_impl" in bridge_api
    assert "from src.source_registry import source_url_fingerprint as source_url_fingerprint_impl" in bridge_api
    assert "from src.source_registry import unique_sources as unique_sources_impl" in bridge_api
    assert "unique_sources: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]] = unique_sources_impl" in bridge_api
    assert "source_identity: Callable[[Dict[str, Any]], str] = source_identity_impl" in bridge_api


def test_bridge_api_prefers_registry_service_identity_helpers_when_present(repo_root: Path) -> None:
    bridge_api = (repo_root / "src" / "bridge" / "api.py").read_text(encoding="utf-8")
    registry_service = (repo_root / "src" / "bridge" / "registry_service.py").read_text(encoding="utf-8")
    assert 'if self._field_is_default("unique_sources"):' in bridge_api
    assert 'if self._field_is_default("source_identity"):' in bridge_api
    assert 'if self._field_is_default("source_url_fingerprint"):' in bridge_api
    assert 'if self._field_is_default("normalize_source_url"):' in bridge_api
    assert "def unique_sources(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:" in registry_service
    assert "def source_identity(row: Dict[str, Any]) -> str:" in registry_service
