from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from src import admin_bridge
from src.bridge import sync_service as sync_service_module


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
    assert admin_bridge._get_desktop_update_service() is service
    assert not hasattr(admin_bridge, "_DESKTOP_UPDATE_SERVICE")
    assert not hasattr(admin_bridge, "_DESKTOP_UPDATE_SERVICE_DATA_DIR")


def test_desktop_update_service_holder_reuses_existing_service(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))
    holder_service = SimpleNamespace(name="holder-desktop-update-service")
    admin_bridge.BRIDGE_SERVICES.reset_desktop_update_service()
    admin_bridge.BRIDGE_SERVICES.desktop_update_service = holder_service
    admin_bridge.BRIDGE_SERVICES.desktop_update_service_data_dir = data_dir.resolve()

    service = admin_bridge._get_desktop_update_service()

    assert service is holder_service
    assert admin_bridge.BRIDGE_SERVICES.desktop_update_service is holder_service
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
    assert admin_bridge._get_registry_service() is service
    assert not hasattr(admin_bridge, "_REGISTRY_SERVICE")
    assert not hasattr(admin_bridge, "_REGISTRY_SERVICE_PATHS")


def test_registry_service_holder_reuses_existing_service(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))
    expected_paths = (
        Path(admin_bridge.ACTIVE_PATH),
        Path(admin_bridge.PENDING_PATH),
        Path(admin_bridge.REJECTED_PATH),
    )
    holder_service = SimpleNamespace(name="holder-registry-service")
    admin_bridge.BRIDGE_SERVICES.reset_registry_service()
    admin_bridge.BRIDGE_SERVICES.registry_service = holder_service
    admin_bridge.BRIDGE_SERVICES.registry_service_paths = expected_paths

    service = admin_bridge._get_registry_service()

    assert service is holder_service
    assert admin_bridge.BRIDGE_SERVICES.registry_service is holder_service
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
    assert admin_bridge._get_discovery_service() is service
    assert not hasattr(admin_bridge, "_DISCOVERY_SERVICE")
    assert not hasattr(admin_bridge, "_DISCOVERY_SERVICE_PATHS")


def test_discovery_service_holder_reuses_existing_service(
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
    holder_service = SimpleNamespace(name="holder-discovery-service")
    admin_bridge.BRIDGE_SERVICES.reset_discovery_service()
    admin_bridge.BRIDGE_SERVICES.discovery_service = holder_service
    admin_bridge.BRIDGE_SERVICES.discovery_service_paths = expected_paths

    service = admin_bridge._get_discovery_service()

    assert service is holder_service
    assert admin_bridge.BRIDGE_SERVICES.discovery_service is holder_service
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

    second_service = admin_bridge._get_discovery_service()
    assert second_service is not first_service
    assert admin_bridge.BRIDGE_SERVICES.discovery_service is second_service
    assert admin_bridge.BRIDGE_SERVICES.discovery_service_paths == (
        Path(admin_bridge.DISCOVERY_REPORT_PATH),
        Path(admin_bridge.DISCOVERY_CANDIDATES_PATH),
        Path(admin_bridge.PENDING_PATH),
        Path(admin_bridge.DISCOVERY_LOG_PATH),
    )


def test_pipeline_service_is_owned_by_bridge_services_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))

    service = admin_bridge._get_pipeline_service()

    assert admin_bridge.BRIDGE_SERVICES.pipeline_service is service
    assert admin_bridge._get_pipeline_service() is service
    assert not hasattr(admin_bridge, "_PIPELINE_SERVICE")


def test_pipeline_service_holder_reuses_existing_service(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))
    holder_service = SimpleNamespace(name="holder-pipeline-service")
    admin_bridge.BRIDGE_SERVICES.reset_pipeline_service()
    admin_bridge.BRIDGE_SERVICES.pipeline_service = holder_service

    service = admin_bridge._get_pipeline_service()

    assert service is holder_service
    assert admin_bridge.BRIDGE_SERVICES.pipeline_service is holder_service


def test_runtime_path_reconfiguration_resets_pipeline_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    first_data_dir = admin_bridge_entrypoint_root / "data-one"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, first_data_dir)
    )
    first_service = admin_bridge._get_pipeline_service()

    second_data_dir = admin_bridge_entrypoint_root / "data-two"
    admin_bridge.configure_runtime_paths(
        _runtime_config(admin_bridge_entrypoint_root, second_data_dir)
    )

    assert admin_bridge.BRIDGE_SERVICES.pipeline_service is None

    second_service = admin_bridge._get_pipeline_service()
    assert second_service is not first_service
    assert admin_bridge.BRIDGE_SERVICES.pipeline_service is second_service


def test_sync_service_is_owned_by_bridge_services_holder(
    admin_bridge_entrypoint_root: Path,
) -> None:
    data_dir = admin_bridge_entrypoint_root / "data"
    admin_bridge.configure_runtime_paths(_runtime_config(admin_bridge_entrypoint_root, data_dir))

    service = admin_bridge._get_sync_service()

    assert admin_bridge.BRIDGE_SERVICES.sync_service is service
    assert admin_bridge.BRIDGE_SERVICES.sync_service_data_dir == data_dir.resolve()
    assert admin_bridge._get_sync_service() is service
    assert not hasattr(admin_bridge, "_SYNC_SERVICE")
    assert not hasattr(admin_bridge, "_SYNC_SERVICE_DATA_DIR")


def test_sync_service_holder_reuses_existing_service(
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
    admin_bridge.BRIDGE_SERVICES.sync_service = legacy_service
    admin_bridge.BRIDGE_SERVICES.sync_service_data_dir = data_dir.resolve()

    service = admin_bridge._get_sync_service()

    assert service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.sync_service is legacy_service
    assert admin_bridge.BRIDGE_SERVICES.sync_service_data_dir == data_dir.resolve()


def test_refresh_sync_config_is_owned_by_bridge_services_holder(monkeypatch) -> None:
    sync_config = object()
    service = SimpleNamespace(refresh_sync_config=lambda: sync_config)
    admin_bridge.BRIDGE_SERVICES.reset_sync_service()
    monkeypatch.setattr(admin_bridge.BRIDGE_SERVICES, "sync_config", None)
    monkeypatch.setattr(admin_bridge, "_get_sync_service", lambda: service)

    result = admin_bridge.refresh_sync_config()

    assert result is sync_config
    assert admin_bridge.BRIDGE_SERVICES.sync_config is sync_config
    assert not hasattr(admin_bridge, "SYNC_CONFIG")


def test_sync_config_refresh_avoids_explicit_global_declaration() -> None:
    admin_bridge_source = Path(admin_bridge.__file__).read_text(encoding="utf-8")
    sync_service_source = Path(sync_service_module.__file__).read_text(encoding="utf-8")

    assert "global SYNC_CONFIG" not in admin_bridge_source
    assert "global SYNC_CONFIG" not in sync_service_source


def test_former_admin_entrypoint_modules_do_not_own_root_injection_seams() -> None:
    admin_bridge_source = Path(admin_bridge.__file__).read_text(encoding="utf-8")
    former_root_modules = (
        admin_bridge.admin_entrypoint_api_mod,
        admin_bridge.admin_entrypoint_runtime_mod,
        admin_bridge.admin_entrypoint_services_mod,
        admin_bridge.admin_registry_api_mod,
        admin_bridge.admin_task_runtime_mod,
    )

    for module in former_root_modules:
        module_source = Path(module.__file__).read_text(encoding="utf-8")

        assert not hasattr(module, "root")
        assert "root: Any" not in module_source

    assert "admin_entrypoint_api_mod.root" not in admin_bridge_source
    assert "admin_entrypoint_runtime_mod.root" not in admin_bridge_source
    assert "admin_entrypoint_services_mod.root" not in admin_bridge_source
    assert "admin_registry_api_mod.root" not in admin_bridge_source
    assert "admin_task_runtime_mod.root" not in admin_bridge_source


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
    assert admin_bridge.BRIDGE_SERVICES.sync_config is None
    assert not hasattr(admin_bridge, "SYNC_CONFIG")

    second_service = admin_bridge._get_sync_service()
    assert second_service is not first_service
    assert admin_bridge.BRIDGE_SERVICES.sync_service is second_service
    assert admin_bridge.BRIDGE_SERVICES.sync_service_data_dir == second_data_dir.resolve()
