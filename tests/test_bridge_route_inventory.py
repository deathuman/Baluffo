from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from tools.repo_health import bridge_route_inventory as inventory


def _write_handler(tmp_path: Path, rel_path: str, source: str) -> str:
    path = tmp_path / rel_path
    path.parent.mkdir(parents=True)
    path.write_text(dedent(source).strip() + "\n", encoding="utf-8")
    return rel_path


def test_discovers_exact_set_membership_and_prefix_routes(tmp_path: Path, monkeypatch) -> None:
    rel_path = _write_handler(
        tmp_path,
        "src/bridge/routes/get_routes.py",
        """
        def handle(path):
            if path == "/exact":
                return True
            if path in {"/set-a", "/set-b"}:
                return True
            if path.startswith("/prefix/"):
                return True
            return False
        """,
    )
    monkeypatch.setattr(inventory, "ROUTE_HANDLER_METHODS", {rel_path: "GET"})

    routes = inventory.discover_bridge_routes(tmp_path)

    assert {(route.method, route.match_kind, route.pattern) for route in routes} == {
        ("GET", inventory.EXACT, "/exact"),
        ("GET", inventory.EXACT, "/set-a"),
        ("GET", inventory.EXACT, "/set-b"),
        ("GET", inventory.PREFIX, "/prefix/"),
    }


def test_inventory_reports_missing_discovered_route(tmp_path: Path, monkeypatch) -> None:
    rel_path = _write_handler(
        tmp_path,
        "src/bridge/routes/get_routes.py",
        """
        def handle(path):
            if path == "/missing":
                return True
            return False
        """,
    )
    monkeypatch.setattr(inventory, "ROUTE_HANDLER_METHODS", {rel_path: "GET"})
    monkeypatch.setattr(inventory, "BRIDGE_ROUTES", ())

    failures = inventory.check_bridge_route_inventory(tmp_path)

    assert failures == [
        "GET exact /missing in src/bridge/routes/get_routes.py:2 is missing from bridge route inventory."
    ]
