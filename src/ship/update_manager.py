#!/usr/bin/env python3
"""Public API and CLI facade for the Baluffo ship update manager."""

from __future__ import annotations

import os

from . import update_manager_state, update_manager_validation
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
    ensure_state,
    iso_now,
    log_event,
    read_json,
    write_state,
)
from .update_manager_validation import (
    compute_sha256,
    health_check_version,
    is_downgrade,
    sign_manifest,
    validate_data_dir,
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


def _sync_leaf_compat() -> None:
    update_manager_state.os = os
    update_manager_validation.UPDATER_VERSION = UPDATER_VERSION


def _write_atomic(path, payload: str) -> None:
    _sync_leaf_compat()
    update_manager_state._write_atomic(path, payload)


def write_json_atomic(path, payload) -> None:
    _sync_leaf_compat()
    update_manager_state.write_json_atomic(path, payload)


def write_text_atomic(path, text: str) -> None:
    _sync_leaf_compat()
    update_manager_state.write_text_atomic(path, text)


def validate_manifest(manifest) -> None:
    _sync_leaf_compat()
    update_manager_validation.validate_manifest(manifest)


def verify_artifact(bundle_zip, manifest, signing_key: str) -> None:
    _sync_leaf_compat()
    update_manager_validation.verify_artifact(bundle_zip, manifest, signing_key)


if __name__ == "__main__":
    raise SystemExit(main())
