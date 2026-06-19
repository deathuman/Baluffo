from __future__ import annotations

import threading
from types import SimpleNamespace

from src.ship import desktop_update_constants as du_constants
from src.ship import desktop_update_manifest as du_manifest
from src.ship import desktop_update_service as du_service
from src.ship import desktop_update_shared as du_shared
from src.ship import desktop_update_state as update_state

du = SimpleNamespace(
    DESKTOP_UPDATE_CHANNEL=du_manifest.DESKTOP_UPDATE_CHANNEL,
    DESKTOP_UPDATE_HELPER_NAME=du_constants.DESKTOP_UPDATE_HELPER_NAME,
    DesktopUpdatePaths=du_shared.DesktopUpdatePaths,
    DesktopUpdateService=du_service.DesktopUpdateService,
    Ed25519PrivateKey=du_manifest.Ed25519SigningClass,
    GITHUB_API_BASE_ENV=du_constants.GITHUB_API_BASE_ENV,
    PUBLIC_KEYS_FILE=du_manifest.PUBLIC_KEYS_FILE,
    _manifest_to_status=update_state._manifest_to_status,
    canonical_manifest_bytes=du_shared.canonical_manifest_bytes,
    compare_versions=du_shared.compare_versions,
    compute_sha256=du_shared.compute_sha256,
    default_status_payload=update_state.default_status_payload,
    desktop_update_public_key_candidate_paths=du_shared.desktop_update_public_key_candidate_paths,
    download_file=du_shared.download_file,
    fetch_json=du_shared.fetch_json,
    helper_runtime_tmpdir=update_state.helper_runtime_tmpdir,
    iso_now=du_shared.iso_now,
    launch_staged_update_helper=update_state.launch_staged_update_helper,
    load_desktop_update_public_keys=du_shared.load_desktop_update_public_keys,
    load_status=update_state.load_status,
    pid_is_running=du_shared.pid_is_running,
    read_json=du_shared.read_json,
    resolve_desktop_session_root=du_shared.resolve_desktop_session_root,
    resolve_github_api_base=du_shared.resolve_github_api_base,
    resolve_release_repo=du_shared.resolve_release_repo,
    save_status=update_state.save_status,
    threading=threading,
    updater_install_requested=update_state.updater_install_requested,
    validate_install_plan=update_state.validate_install_plan,
    verify_manifest_signature=du_shared.verify_manifest_signature,
    write_json_atomic=du_shared.write_json_atomic,
    write_success_marker=update_state.write_success_marker,
)
