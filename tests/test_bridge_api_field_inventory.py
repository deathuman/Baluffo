from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from tools.repo_health import bridge_api_field_inventory as inventory
from tools.repo_health import repo_guardrails


def _write(tmp_path: Path, rel_path: str, source: str) -> Path:
    path = tmp_path / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(source).strip() + "\n", encoding="utf-8")
    return path


def test_current_bridge_api_inventory_is_complete() -> None:
    fields = inventory.collect_bridge_api_field_inventory()
    by_name = {field.name: field for field in fields}

    assert len(fields) == inventory.EXPECTED_BRIDGE_API_FIELD_COUNT
    assert inventory.check_bridge_api_field_inventory() == []
    assert {field.name for field in fields} >= {
        "runtime_config",
        "load_state",
        "desktop_local_data_store",
        "get_sync_status_payload",
    }
    assert {
        "route-used",
        "post-route-used",
    } <= set(by_name["compute_ops_dashboard_health"].categories)
    assert (
        "src/bridge/routes/get_ops_status.py" in by_name["compute_ops_dashboard_health"].references
    )
    assert (
        "src/bridge/routes/post_routes_admin.py"
        in by_name["compute_ops_dashboard_health"].references
    )
    assert "helper-used" in by_name["should_exit_for_owner_timeout"].categories
    assert "src/bridge/server/httpd.py" in by_name["should_exit_for_owner_timeout"].references
    assert all(field.categories for field in fields)
    assert [field.name for field in fields if "default-only" in field.categories] == []


def test_inventory_reports_field_count_drift(tmp_path: Path, monkeypatch) -> None:
    _write(
        tmp_path,
        "src/bridge/api.py",
        """
        from dataclasses import dataclass

        @dataclass
        class BridgeApi:
            runtime_config: object
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_BRIDGE_API_FIELD_COUNT", 2)
    monkeypatch.setattr(inventory, "RUNTIME_PATH_FIELDS", {"runtime_config"})
    monkeypatch.setattr(inventory, "SERVICE_HANDLE_FIELDS", set())

    failures = inventory.check_bridge_api_field_inventory(tmp_path)

    assert failures == [
        "BridgeApi has 1 dataclass fields; expected 2. "
        "Update EXPECTED_BRIDGE_API_FIELD_COUNT and review field classification evidence.",
    ]


def test_inventory_classifies_route_post_bootstrap_service_and_test_usage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write(
        tmp_path,
        "src/bridge/api.py",
        """
        from dataclasses import dataclass

        @dataclass
        class BridgeApi:
            runtime_config: object
            registry: object | None = None
            route_payload: object | None = None
            post_payload: object | None = None
            injected_payload: object | None = None
            wired_payload: object | None = None

            def __post_init__(self) -> None:
                if self.registry is not None:
                    self._field_is_default("wired_payload")
        """,
    )
    _write(
        tmp_path,
        "src/bridge/bootstrap.py",
        """
        from src.bridge.api import BridgeApi

        def build_bridge_api(value):
            return BridgeApi(runtime_config=value, injected_payload=value)
        """,
    )
    _write(
        tmp_path,
        "src/bridge/routes/get_example.py",
        """
        def handle(api):
            return api.route_payload
        """,
    )
    _write(
        tmp_path,
        "src/bridge/routes/post_routes_example.py",
        """
        def handle(api):
            return api.post_payload
        """,
    )
    _write(
        tmp_path,
        "tests/test_bridge_api_usage.py",
        """
        def test_usage(api):
            api.route_payload = object()
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_BRIDGE_API_FIELD_COUNT", 6)
    monkeypatch.setattr(inventory, "RUNTIME_PATH_FIELDS", {"runtime_config"})
    monkeypatch.setattr(inventory, "SERVICE_HANDLE_FIELDS", {"registry"})

    by_name = {
        field.name: set(field.categories)
        for field in inventory.collect_bridge_api_field_inventory(tmp_path)
    }

    assert by_name["runtime_config"] == {"bootstrap-injected", "runtime-path"}
    assert by_name["registry"] == {"service-handle"}
    assert by_name["route_payload"] == {"route-used", "test-overridden"}
    assert by_name["post_payload"] == {"post-route-used"}
    assert by_name["injected_payload"] == {"bootstrap-injected"}
    assert by_name["wired_payload"] == {"service-wired"}
    assert inventory.check_bridge_api_field_inventory(tmp_path) == []


def test_inventory_rejects_default_only_field_with_string_production_reference(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write(
        tmp_path,
        "src/bridge/api.py",
        """
        from dataclasses import dataclass

        @dataclass
        class BridgeApi:
            runtime_config: object
            string_only: object | None = None
        """,
    )
    _write(
        tmp_path,
        "src/bridge/helper.py",
        """
        def field_name():
            return "string_only"
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_BRIDGE_API_FIELD_COUNT", 2)
    monkeypatch.setattr(inventory, "RUNTIME_PATH_FIELDS", {"runtime_config"})
    monkeypatch.setattr(inventory, "SERVICE_HANDLE_FIELDS", set())

    failures = inventory.check_bridge_api_field_inventory(tmp_path)

    assert failures == [
        "BridgeApi field string_only is default-only but referenced by production code: "
        "src/bridge/helper.py.",
    ]


def test_inventory_classifies_dynamic_getattr_api_refs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write(
        tmp_path,
        "src/bridge/api.py",
        """
        from dataclasses import dataclass

        @dataclass
        class BridgeApi:
            route_payload: object | None = None
            post_payload: object | None = None
            helper_payload: object | None = None
            self_lookup: object | None = None

            def helper(self) -> None:
                getattr(self, "self_lookup", None)
        """,
    )
    _write(
        tmp_path,
        "src/bridge/routes/get_example.py",
        """
        def handle(api):
            return getattr(api, "route_payload", None)
        """,
    )
    _write(
        tmp_path,
        "src/bridge/routes/post_routes_example.py",
        """
        def handle(api):
            return getattr(api, "post_payload", None)
        """,
    )
    _write(
        tmp_path,
        "src/bridge/server/httpd.py",
        """
        def serve(api):
            return getattr(api, "helper_payload", None)
        """,
    )
    monkeypatch.setattr(inventory, "EXPECTED_BRIDGE_API_FIELD_COUNT", 4)
    monkeypatch.setattr(inventory, "RUNTIME_PATH_FIELDS", set())
    monkeypatch.setattr(inventory, "SERVICE_HANDLE_FIELDS", set())

    by_name = {
        field.name: field for field in inventory.collect_bridge_api_field_inventory(tmp_path)
    }

    assert by_name["route_payload"].categories == ("route-used",)
    assert by_name["route_payload"].references == ("src/bridge/routes/get_example.py",)
    assert by_name["post_payload"].categories == ("post-route-used",)
    assert by_name["post_payload"].references == ("src/bridge/routes/post_routes_example.py",)
    assert by_name["helper_payload"].categories == ("helper-used",)
    assert by_name["helper_payload"].references == ("src/bridge/server/httpd.py",)
    assert by_name["self_lookup"].categories == ("default-only",)
    assert inventory.check_bridge_api_field_inventory(tmp_path) == []


def test_repo_guardrails_compat_group_runs_bridge_api_inventory(monkeypatch) -> None:
    monkeypatch.setattr(repo_guardrails, "_run_python_checks", lambda _group, _checks: [])
    monkeypatch.setattr(
        repo_guardrails, "check_bridge_api_field_inventory", lambda: ["field drift"]
    )

    assert "compat" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["compat"] is repo_guardrails.run_compat_group
    assert repo_guardrails.run_compat_group() == [
        repo_guardrails.GuardFailure("compat", "check_bridge_api_field_inventory", "field drift")
    ]
