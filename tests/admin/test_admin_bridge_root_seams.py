from src import admin_bridge


def test_admin_bridge_entrypoint_modules_are_bound_to_root_module() -> None:
    assert admin_bridge.admin_entrypoint_api_mod.root is admin_bridge
    assert admin_bridge.admin_entrypoint_runtime_mod.root is admin_bridge
    assert admin_bridge.admin_entrypoint_services_mod.root is admin_bridge
    assert admin_bridge.admin_registry_api_mod.root is admin_bridge
    assert admin_bridge.admin_task_runtime_mod.root is admin_bridge
