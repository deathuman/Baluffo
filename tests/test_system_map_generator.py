import json
from pathlib import Path

from tools.repo_health import generate_system_map


def test_system_map_generator_writes_expected_payload_shape(tmp_path: Path) -> None:
    output = tmp_path / "system-map.json"

    payload = generate_system_map.generate(output)

    persisted = json.loads(output.read_text(encoding="utf-8"))
    assert persisted == payload
    assert persisted["generatedAt"]
    assert persisted["frontendPages"]
    assert persisted["taskFlows"]
    assert persisted["riskMarkers"]
    assert persisted["evidenceFiles"]
    assert isinstance(persisted["moduleCount"], int)


def test_system_map_generator_honors_custom_output(tmp_path: Path) -> None:
    output = tmp_path / "nested" / "custom-system-map.json"

    exit_code = generate_system_map.main(["--output", str(output)])

    assert exit_code == 0
    assert output.exists()


def test_system_map_generator_includes_bridge_route_inventory(tmp_path: Path) -> None:
    payload = generate_system_map.generate(tmp_path / "system-map.json")

    routes = payload["bridgeRoutes"]
    assert any(
        route["method"] == "GET"
        and route["pattern"] == "/ops/task-state"
        and route["handlerFile"] == "src/bridge/routes/get_ops_status.py"
        for route in routes
    )
    assert any(
        route["method"] == "POST"
        and route["pattern"] == "/tasks/run-fetcher"
        and route["handlerFile"] == "src/bridge/routes/post_routes_admin.py"
        for route in routes
    )


def test_system_map_generator_exposes_ai_read_hints_and_caveats(tmp_path: Path) -> None:
    payload = generate_system_map.generate(tmp_path / "system-map.json")

    hints = payload["aiReadHints"]
    assert "docs/AI_ASSISTANT_GUIDE.md" in hints["readFirst"]
    assert hints["routeInventory"] == "tools/repo_health/bridge_route_inventory.py"
    assert hints["moduleCount"] == payload["moduleCount"]
    assert hints["bridgeRouteCount"] == len(payload["bridgeRoutes"])
    assert any("canonical" in caveat for caveat in hints["caveats"])
