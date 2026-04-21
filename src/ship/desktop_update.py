"""Desktop update service and helper contracts for portable Baluffo installs."""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import shutil
import ssl
import sys
import subprocess
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from src.app_version import get_app_version
from src.baluffo_version import compare_baluffo_versions
from src.shared.github_https import (
    GITHUB_CA_BUNDLE_ENV,
    build_github_ssl_context,
    wrap_github_request_error,
)
from src.ship import desktop_update_service as desktop_update_service_mod
from src.ship import desktop_update_shared as desktop_update_shared_mod
from src.ship import desktop_update_state as desktop_update_state_mod

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey,
        Ed25519PublicKey,
    )
except Exception:  # noqa: BLE001
    Ed25519PrivateKey = None  # type: ignore[assignment]
    Ed25519PublicKey = None  # type: ignore[assignment]

try:
    import psutil
except Exception:  # noqa: BLE001
    psutil = None  # type: ignore[assignment]


DESKTOP_UPDATE_SCHEMA_VERSION = 1
DESKTOP_UPDATE_CHANNEL = "stable"
DESKTOP_UPDATE_MANIFEST_ASSET = "baluffo-desktop-update-manifest.json"
DESKTOP_UPDATE_HELPER_NAME = "BaluffoUpdater.exe"
DESKTOP_UPDATER_VERSION = "2.0.0"
DEFAULT_RELEASE_CHECK_THROTTLE_SECONDS = 6 * 60 * 60
DOWNLOAD_CHUNK_SIZE = 1024 * 256
GITHUB_API_BASE = "https://api.github.com"
GITHUB_API_BASE_ENV = "BALUFFO_DESKTOP_UPDATE_GITHUB_API_BASE"
DESKTOP_UPDATE_CA_BUNDLE_ENV = "BALUFFO_DESKTOP_UPDATE_CA_BUNDLE"
INSTALL_STATE_FILE = "install-state.json"
INSTALL_PLAN_FILE = "install-plan.json"
MANIFEST_CACHE_FILE = "manifest-cache.json"
SUCCESS_MARKER_FILE = "post-install-success.json"
HANDOFF_REQUEST_FILE = "handoff-requested.json"
HELPER_STDOUT_LOG_FILE = "desktop-updater-helper.stdout.log"
HELPER_STDERR_LOG_FILE = "desktop-updater-helper.stderr.log"
HELPER_DIAGNOSTICS_LOG_FILE = "desktop-updater-helper.diagnostics.jsonl"
HELPER_RUNTIME_TMP_ROOT_NAME = "BaluffoUpdaterRuntime"
PUBLIC_KEYS_FILE = "desktop-update-public-keys.json"
DESKTOP_UPDATE_CONFIG_FILE = "desktop-update-config.json"
USER_AGENT = f"BaluffoDesktopUpdater/{DESKTOP_UPDATER_VERSION}"
ATOMIC_WRITE_RETRY_ATTEMPTS = 20
ATOMIC_WRITE_RETRY_BASE_DELAY_S = 0.05
ATOMIC_WRITE_RETRY_MAX_DELAY_S = 0.5
INSTALL_STATE_STAGE_DEFAULTS = {
    "handoff_requested": "preparing",
    "waiting_for_exit": "waiting_for_exit",
    "installing": "installing",
    "verifying": "verifying",
    "installed": "installed",
    "failed": "failed",
}
INSTALL_STAGE_LABELS = {
    "idle": "",
    "preparing": "Preparing update",
    "waiting_for_exit": "Closing Baluffo",
    "extracting": "Installing update",
    "snapshotting": "Installing update",
    "backup": "Installing update",
    "replacing": "Installing update",
    "migrating": "Installing update",
    "relaunching": "Restarting Baluffo",
    "verifying": "Restarting Baluffo",
    "recovering": "Installing update",
    "rolling_back": "Installing update",
    "installed": "",
    "failed": "",
}
INSTALL_STATES_PRESERVING_DOWNLOADED_ARTIFACT = frozenset(
    {
        "handoff_requested",
        "waiting_for_exit",
        "installing",
        "verifying",
        "installed",
        "failed",
    }
)
HANDOFF_PENDING_INSTALL_STATES = frozenset({"handoff_requested", "waiting_for_exit"})
_RUNTIME_SESSION_ROOT_FALLBACK: Path | None = None

desktop_update_shared_mod.root = sys.modules[__name__]
desktop_update_state_mod.root = sys.modules[__name__]
desktop_update_service_mod.root = sys.modules[__name__]

iso_now = desktop_update_shared_mod.iso_now
resolve_github_api_base = desktop_update_shared_mod.resolve_github_api_base
_uses_github_https = desktop_update_shared_mod._uses_github_https
_build_desktop_update_ssl_context = desktop_update_shared_mod._build_desktop_update_ssl_context
normalize_install_stage = desktop_update_shared_mod.normalize_install_stage
install_stage_label = desktop_update_shared_mod.install_stage_label
_replace_with_retry = desktop_update_shared_mod._replace_with_retry
_write_atomic = desktop_update_shared_mod._write_atomic
write_json_atomic = desktop_update_shared_mod.write_json_atomic
read_json = desktop_update_shared_mod.read_json
compute_sha256 = desktop_update_shared_mod.compute_sha256
compare_versions = desktop_update_shared_mod.compare_versions
sort_json = desktop_update_shared_mod.sort_json
canonical_manifest_bytes = desktop_update_shared_mod.canonical_manifest_bytes
_decode_public_keys_payload = desktop_update_shared_mod._decode_public_keys_payload
desktop_update_public_key_candidate_paths = (
    desktop_update_shared_mod.desktop_update_public_key_candidate_paths
)
load_desktop_update_public_keys = desktop_update_shared_mod.load_desktop_update_public_keys
verify_manifest_signature = desktop_update_shared_mod.verify_manifest_signature
sign_manifest = desktop_update_shared_mod.sign_manifest
validate_desktop_manifest = desktop_update_shared_mod.validate_desktop_manifest
_resolve_ship_current_version = desktop_update_shared_mod._resolve_ship_current_version
resolve_release_repo = desktop_update_shared_mod.resolve_release_repo
_runtime_session_root_candidate_fallback = (
    desktop_update_shared_mod._runtime_session_root_candidate_fallback
)
_resolve_desktop_session_root_fallback = (
    desktop_update_shared_mod._resolve_desktop_session_root_fallback
)
resolve_desktop_session_root = desktop_update_shared_mod.resolve_desktop_session_root
_looks_like_windows_absolute_path = desktop_update_shared_mod._looks_like_windows_absolute_path
_resolve_runtime_path = desktop_update_shared_mod._resolve_runtime_path
read_desktop_session_state = desktop_update_shared_mod.read_desktop_session_state
pid_is_running = desktop_update_shared_mod.pid_is_running
_json_headers = desktop_update_shared_mod._json_headers
fetch_json = desktop_update_shared_mod.fetch_json
download_file = desktop_update_shared_mod.download_file
DesktopUpdatePaths = desktop_update_shared_mod.DesktopUpdatePaths

default_status_payload = desktop_update_state_mod.default_status_payload
_load_credible_handoff_install_plan = desktop_update_state_mod._load_credible_handoff_install_plan
_handoff_status_pending = desktop_update_state_mod._handoff_status_pending
_apply_credible_handoff_status = desktop_update_state_mod._apply_credible_handoff_status
_reconcile_handoff_status = desktop_update_state_mod._reconcile_handoff_status
load_status = desktop_update_state_mod.load_status
save_status = desktop_update_state_mod.save_status
updater_install_requested = desktop_update_state_mod.updater_install_requested
clear_success_marker = desktop_update_state_mod.clear_success_marker
clear_handoff_request = desktop_update_state_mod.clear_handoff_request
clear_install_plan = desktop_update_state_mod.clear_install_plan
clear_staged_helper = desktop_update_state_mod.clear_staged_helper
helper_runtime_tmpdir = desktop_update_state_mod.helper_runtime_tmpdir
launch_staged_update_helper = desktop_update_state_mod.launch_staged_update_helper
write_success_marker = desktop_update_state_mod.write_success_marker
read_cached_manifest = desktop_update_state_mod.read_cached_manifest
_normalize_release_notes_payload = desktop_update_state_mod._normalize_release_notes_payload
_cached_release_notes = desktop_update_state_mod._cached_release_notes
_portable_artifact_name = desktop_update_state_mod._portable_artifact_name
_manifest_to_status = desktop_update_state_mod._manifest_to_status
_reconcile_downloaded_artifact_status = desktop_update_state_mod._reconcile_downloaded_artifact_status
_stale_download_failed_status = desktop_update_state_mod._stale_download_failed_status
_normalize_installed_status = desktop_update_state_mod._normalize_installed_status
_failure_result = desktop_update_state_mod._failure_result
_retryable_install_status = desktop_update_state_mod._retryable_install_status
validate_install_plan = desktop_update_state_mod.validate_install_plan

DesktopUpdateService = desktop_update_service_mod.DesktopUpdateService


__all__ = [
    "DESKTOP_UPDATE_HELPER_NAME",
    "DESKTOP_UPDATE_MANIFEST_ASSET",
    "DESKTOP_UPDATE_SCHEMA_VERSION",
    "DESKTOP_UPDATER_VERSION",
    "DesktopUpdatePaths",
    "DesktopUpdateService",
    "canonical_manifest_bytes",
    "clear_success_marker",
    "compare_versions",
    "compute_sha256",
    "desktop_update_public_key_candidate_paths",
    "default_status_payload",
    "download_file",
    "fetch_json",
    "install_stage_label",
    "iso_now",
    "launch_staged_update_helper",
    "load_desktop_update_public_keys",
    "load_status",
    "normalize_install_stage",
    "read_cached_manifest",
    "read_desktop_session_state",
    "resolve_desktop_session_root",
    "resolve_github_api_base",
    "resolve_release_repo",
    "save_status",
    "sign_manifest",
    "updater_install_requested",
    "validate_desktop_manifest",
    "validate_install_plan",
    "verify_manifest_signature",
    "write_json_atomic",
    "write_success_marker",
]
