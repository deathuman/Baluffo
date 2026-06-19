#!/usr/bin/env python3
"""Public API and CLI facade for the Baluffo ship update manager."""

from __future__ import annotations

from .update_manager_apply import (
    apply_update,
    create_data_backup,
    locate_staged_version_dir,
    restore_data_backup,
    rollback_migrations,
    run_migrations,
)
from .update_manager_bootstrap import (
    _BOOTSTRAP_ROOT_HTML,
    _BOOTSTRAP_VERSION_TAG,
    refresh_runtime_bootstrap,
    repair_version_from_runtime_bootstrap,
)
from .update_manager_cli import main, parse_args
from .update_manager_paths import (
    BOOTSTRAP_DIR_NAME,
    CURRENT_NAME,
    LOG_NAME,
    REQUIRED_VERSION_FILES,
    ROOT,
    STATE_NAME,
    UPDATER_VERSION,
    ShipPaths,
)
from .update_manager_recovery import create_support_bundle, recover_previous, startup_check
from .update_manager_state import (
    _healthy_version_name,
    _list_healthy_version_names,
    _prefer_higher_semver,
    _recover_current_version,
    _write_atomic,
    ensure_state,
    iso_now,
    log_event,
    read_json,
    write_json_atomic,
    write_state,
    write_text_atomic,
)
from .update_manager_validation import (
    compute_sha256,
    health_check_version,
    is_downgrade,
    sign_manifest,
    validate_data_dir,
    validate_manifest,
    verify_artifact,
)

__all__ = [
    "BOOTSTRAP_DIR_NAME",
    "CURRENT_NAME",
    "LOG_NAME",
    "REQUIRED_VERSION_FILES",
    "ROOT",
    "STATE_NAME",
    "UPDATER_VERSION",
    "ShipPaths",
    "_BOOTSTRAP_ROOT_HTML",
    "_BOOTSTRAP_VERSION_TAG",
    "_healthy_version_name",
    "_list_healthy_version_names",
    "_prefer_higher_semver",
    "_recover_current_version",
    "_write_atomic",
    "apply_update",
    "compute_sha256",
    "create_data_backup",
    "create_support_bundle",
    "ensure_state",
    "health_check_version",
    "is_downgrade",
    "iso_now",
    "locate_staged_version_dir",
    "log_event",
    "main",
    "parse_args",
    "read_json",
    "recover_previous",
    "refresh_runtime_bootstrap",
    "repair_version_from_runtime_bootstrap",
    "restore_data_backup",
    "rollback_migrations",
    "run_migrations",
    "sign_manifest",
    "startup_check",
    "validate_data_dir",
    "validate_manifest",
    "verify_artifact",
    "write_json_atomic",
    "write_state",
    "write_text_atomic",
]


if __name__ == "__main__":
    raise SystemExit(main())
