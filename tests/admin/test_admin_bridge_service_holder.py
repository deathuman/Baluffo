from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from src import admin_bridge


def _runtime_config(root: Path, data_dir: Path) -> admin_bridge.RuntimeConfig:
    return admin_bridge.RuntimeConfig(
        root=root,
        data_dir=data_dir,
        host="127.0.0.1",
        port=8877,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=False,
        owner_mode="",
        owner_token="",
        started_by="test",
        owner_idle_timeout_s=0.0,
    )


def test_desktop_update_service_is_owned_by_bridge_services_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))

    service = admin_bridge._get_desktop_update_service()

    assert admin_bridge.BRIDGE_SERVICES.desktop_update_service is service
    assert admin_bridge.BRIDGE_SERVICES.desktop_update_service_data_dir == data_dir.resolve()
    assert admin_bridge._DESKTOP_UPDATE_SERVICE is service
    assert admin_bridge._DESKTOP_UPDATE_SERVICE_DATA_DIR == data_dir.resolve()
    assert admin_bridge._get_desktop_update_service() is service


def test_desktop_update_service_holder_adopts_legacy_patch_surface(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))
    legacy_service = SimpleNamespace(name="legacy-desktop-update-service")
    admin_bridge.BRIDGE_SERVICES.reset_desktop_update_service()
    admin_bridge._DESKTOP_UPDATE_SERVICE = legacy_service
    admin_bridge._DESKTOP_UPDATE_SERVICE_DATA_DIR = data_dir.resolve()

    service = admin_bridge._get_desktop_update_service()

    assert service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.desktop_update_service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.desktop_update_service_data_dir == data_dir.resolve()


def test_runtime_path_reconfiguration_resets_desktop_update_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    first_data_dir = admin_bridge_entrypoint_root / "data-one"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, first_data_dir)
    )
    first_service = admin_bridge._get_desktop_update_service()

    second_data_dir = admin_bridge_entrypoint_root / "data-two"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, second_data_dir)
    )

    assert admin_bridge.BRIDGE_SERVICES.desktop_update_service is None
    assert admin_bridge.BRIDGE_SERVICES.desktop_update_service_data_dir is None
    assert admin_bridge._DESKTOP_UPDATE_SERVICE is None
    assert admin_bridge._DESKTOP_UPDATE_SERVICE_DATA_DIR is None

    second_service = admin_bridge._get_desktop_update_service()
    assert second_service is not first_service
    assert admin_bridge.BRIDGE_SERVICES.desktop_update_service is second_service
    assert admin_bridge.BRIDGE_SERVICES.desktop_update_service_data_dir == second_data_dir.resolve()


def test_registry_service_is_owned_by_bridge_services_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))
    expected_paths = (
        Path(admin_bridge.ACTIVE_PATH),
        Path(admin_bridge.PENDING_PATH),
        Path(admin_bridge.REJECTED_PATH),
    )

    service = admin_bridge._get_registry_service()

    assert admin_bridge.BRIDGE_SERVICES.registry_service is service
    assert admin_bridge.BRIDGE_SERVICES.registry_service_paths == expected_paths
    assert admin_bridge._REGISTRY_SERVICE is service
    assert admin_bridge._REGISTRY_SERVICE_PATHS == expected_paths
    assert admin_bridge._get_registry_service() is service


def test_registry_service_holder_adopts_legacy_patch_surface(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))
    expected_paths = (
        Path(admin_bridge.ACTIVE_PATH),
        Path(admin_bridge.PENDING_PATH),
        Path(admin_bridge.REJECTED_PATH),
    )
    legacy_service = SimpleNamespace(name="legacy-registry-service")
    admin_bridge.BRIDGE_SERVICES.reset_registry_service()
    admin_bridge._REGISTRY_SERVICE = legacy_service
    admin_bridge._REGISTRY_SERVICE_PATHS = expected_paths

    service = admin_bridge._get_registry_service()

    assert service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.registry_service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.registry_service_paths == expected_paths


def test_runtime_path_reconfiguration_resets_registry_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    first_data_dir = admin_bridge_entrypoint_root / "data-one"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, first_data_dir)
    )
    first_service = admin_bridge._get_registry_service()

    second_data_dir = admin_bridge_entrypoint_root / "data-two"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, second_data_dir)
    )

    assert admin_bridge.BRIDGE_SERVICES.registry_service is None
    assert admin_bridge.BRIDGE_SERVICES.registry_service_paths is None
    assert admin_bridge._REGISTRY_SERVICE is None
    assert admin_bridge._REGISTRY_SERVICE_PATHS is None

    second_service = admin_bridge._get_registry_service()
    assert second_service is not first_service
    assert admin_bridge.BRIDGE_SERVICES.registry_service is second_service
    assert admin_bridge.BRIDGE_SERVICES.registry_service_paths == (
        Path(admin_bridge.ACTIVE_PATH),
        Path(admin_bridge.PENDING_PATH),
        Path(admin_bridge.REJECTED_PATH),
    )


def test_discovery_service_is_owned_by_bridge_services_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))
    expected_paths = (
        Path(admin_bridge.DISCOVERY_REPORT_PATH),
        Path(admin_bridge.DISCOVERY_CANDIDATES_PATH),
        Path(admin_bridge.PENDING_PATH),
        Path(admin_bridge.DISCOVERY_LOG_PATH),
    )

    service = admin_bridge._get_discovery_service()

    assert admin_bridge.BRIDGE_SERVICES.discovery_service is service
    assert admin_bridge.BRIDGE_SERVICES.discovery_service_paths == expected_paths
    assert admin_bridge._DISCOVERY_SERVICE is service
    assert admin_bridge._DISCOVERY_SERVICE_PATHS == expected_paths
    assert admin_bridge._get_discovery_service() is service


def test_discovery_service_holder_adopts_legacy_patch_surface(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))
    expected_paths = (
        Path(admin_bridge.DISCOVERY_REPORT_PATH),
        Path(admin_bridge.DISCOVERY_CANDIDATES_PATH),
        Path(admin_bridge.PENDING_PATH),
        Path(admin_bridge.DISCOVERY_LOG_PATH),
    )
    legacy_service = SimpleNamespace(name="legacy-discovery-service")
    admin_bridge.BRIDGE_SERVICES.reset_discovery_service()
    admin_bridge._DISCOVERY_SERVICE = legacy_service
    admin_bridge._DISCOVERY_SERVICE_PATHS = expected_paths

    service = admin_bridge._get_discovery_service()

    assert service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.discovery_service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.discovery_service_paths == expected_paths


def test_runtime_path_reconfiguration_resets_discovery_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    first_data_dir = admin_bridge_entrypoint_root / "data-one"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, first_data_dir)
    )
    first_service = admin_bridge._get_discovery_service()

    second_data_dir = admin_bridge_entrypoint_root / "data-two"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, second_data_dir)
    )

    assert admin_bridge.BRIDGE_SERVICES.discovery_service is None
    assert admin_bridge.BRIDGE_SERVICES.discovery_service_paths is None
    assert admin_bridge._DISCOVERY_SERVICE is None
    assert admin_bridge._DISCOVERY_SERVICE_PATHS is None

    second_service = admin_bridge._get_discovery_service()
    assert second_service is not first_service
    assert admin_bridge.BRIDGE_SERVICES.discovery_service is second_service
    assert admin_bridge.BRIDGE_SERVICES.discovery_service_paths == (
        Path(admin_bridge.DISCOVERY_REPORT_PATH),
        Path(admin_bridge.DISCOVERY_CANDIDATES_PATH),
        Path(admin_bridge.PENDING_PATH),
        Path(admin_bridge.DISCOVERY_LOG_PATH),
    )


def test_sync_service_is_owned_by_bridge_services_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))

    service = admin_bridge._get_sync_service()

    assert admin_bridge.BRIDGE_SERVICES.sync_service is service
    assert admin_bridge.BRIDGE_SERVICES.sync_service_data_dir == data_dir.resolve()
    assert admin_bridge._SYNC_SERVICE is service
    assert admin_bridge._SYNC_SERVICE_DATA_DIR == data_dir.resolve()
    assert admin_bridge._get_sync_service() is service


def test_sync_service_holder_adopts_legacy_patch_surface(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))
    legacy_service = SimpleNamespace(
        name="legacy-sync-service",
        _sync_state=object(),
        wait_for_sync_tasks=lambda *_args, **_kwargs: None,
    )
    admin_bridge.BRIDGE_SERVICES.reset_sync_service()
    admin_bridge._SYNC_SERVICE = legacy_service
    admin_bridge._SYNC_SERVICE_DATA_DIR = data_dir.resolve()

    service = admin_bridge._get_sync_service()

    assert service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.sync_service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.sync_service_data_dir == data_dir.resolve()


def test_runtime_path_reconfiguration_resets_sync_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    first_data_dir = admin_bridge_entrypoint_root / "data-one"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, first_data_dir)
    )
    first_service = admin_bridge._get_sync_service()

    second_data_dir = admin_bridge_entrypoint_root / "data-two"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, second_data_dir)
    )

    assert admin_bridge.BRIDGE_SERVICES.sync_service is None
    assert admin_bridge.BRIDGE_SERVICES.sync_service_data_dir is None
    assert admin_bridge._SYNC_SERVICE is None
    assert admin_bridge._SYNC_SERVICE_DATA_DIR is None

    second_service = admin_bridge._get_sync_service()
    assert second_service is not first_service
    assert admin_bridge.BRIDGE_SERVICES.sync_service is second_service
    assert admin_bridge.BRIDGE_SERVICES.sync_service_data_dir == second_data_dir.resolve()
