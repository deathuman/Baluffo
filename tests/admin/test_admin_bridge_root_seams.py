from src import admin_bridge


def test_admin_bridge_entrypoint_modules_keep_remaining_root_bindings() -> None:
    assert not hasattr(admin_bridge.admin_entrypoint_api_mod, "root")
    assert not hasattr(admin_bridge.admin_entrypoint_runtime_mod, "root")
    assert not hasattr(admin_bridge.admin_registry_api_mod, "root")
    assert not hasattr(admin_bridge.admin_task_runtime_mod, "root")
    assert admin_bridge.admin_entrypoint_services_mod.root is admin_bridge
