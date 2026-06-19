from __future__ import annotations

from pathlib import Path

from tools.repo_health import repo_guardrails


def test_routes_group_reports_bridge_api_leaf_import_failures(monkeypatch) -> None:
    monkeypatch.setattr(repo_guardrails, "check_bridge_route_inventory", lambda: [])
    monkeypatch.setattr(
        repo_guardrails,
        "check_bridge_route_leaf_bridge_api_imports",
        lambda: ["src/bridge/routes/get_example.py references BridgeApi"],
    )

    assert repo_guardrails.run_routes_group() == [
        repo_guardrails.GuardFailure(
            "routes",
            "check_bridge_route_leaf_bridge_api_imports",
            "src/bridge/routes/get_example.py references BridgeApi",
        )
    ]


def test_bridge_route_leaf_bridge_api_guard_allows_public_delegators(
    tmp_path: Path, monkeypatch
) -> None:
    route_root = tmp_path / "src" / "bridge" / "routes"
    route_root.mkdir(parents=True)
    (route_root / "get_routes.py").write_text(
        "from src.bridge.api import BridgeApi\ndef handle_get(api: BridgeApi) -> None:\n    pass\n",
        encoding="utf-8",
    )
    (route_root / "post_routes.py").write_text(
        "from src.bridge.api import BridgeApi\n"
        "def handle_post(api: BridgeApi) -> None:\n"
        "    pass\n",
        encoding="utf-8",
    )
    (route_root / "get_registry.py").write_text(
        "from typing import Protocol\n"
        "class RegistryRouteApi(Protocol):\n"
        "    def registry_summary(self) -> dict: ...\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)

    assert repo_guardrails.check_bridge_route_leaf_bridge_api_imports() == []


def test_bridge_route_leaf_bridge_api_guard_rejects_leaf_bridge_api_typing(
    tmp_path: Path, monkeypatch
) -> None:
    route_root = tmp_path / "src" / "bridge" / "routes"
    route_root.mkdir(parents=True)
    (route_root / "get_registry.py").write_text(
        "from src.bridge.api import BridgeApi\n"
        "def handle_registry(api: BridgeApi) -> bool:\n"
        "    return isinstance(api, object)\n",
        encoding="utf-8",
    )
    (route_root / "get_discovery.py").write_text(
        "import src.bridge.api as bridge_api\n"
        "def handle_discovery(api: bridge_api.BridgeApi) -> bool:\n"
        "    return True\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)

    assert repo_guardrails.check_bridge_route_leaf_bridge_api_imports() == [
        "src/bridge/routes/get_discovery.py imports BridgeApi at lines [1]; "
        "only public route delegators may depend on the full BridgeApi type.",
        "src/bridge/routes/get_discovery.py references BridgeApi at lines [2]; "
        "route leaves must type against narrow capability protocols.",
        "src/bridge/routes/get_registry.py imports BridgeApi at lines [1]; "
        "only public route delegators may depend on the full BridgeApi type.",
        "src/bridge/routes/get_registry.py references BridgeApi at lines [2]; "
        "route leaves must type against narrow capability protocols.",
    ]
