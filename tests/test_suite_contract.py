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
