"""Pure desktop update constants shared by update leaves and compatibility roots."""

from __future__ import annotations

from src.shared.github_https import GITHUB_CA_BUNDLE_ENV
from src.ship.desktop_update_manifest import DESKTOP_UPDATER_VERSION

DESKTOP_UPDATE_HELPER_NAME = "BaluffoUpdater.exe"
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
HANDOFF_DIAGNOSTICS_FILE = "handoff-diagnostics.json"
HELPER_STDOUT_LOG_FILE = "desktop-updater-helper.stdout.log"
HELPER_STDERR_LOG_FILE = "desktop-updater-helper.stderr.log"
HELPER_DIAGNOSTICS_LOG_FILE = "desktop-updater-helper.diagnostics.jsonl"
HELPER_RUNTIME_TMP_ROOT_NAME = "BaluffoUpdaterRuntime"
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

__all__ = [
    "ATOMIC_WRITE_RETRY_ATTEMPTS",
    "ATOMIC_WRITE_RETRY_BASE_DELAY_S",
    "ATOMIC_WRITE_RETRY_MAX_DELAY_S",
    "DEFAULT_RELEASE_CHECK_THROTTLE_SECONDS",
    "DESKTOP_UPDATE_CA_BUNDLE_ENV",
    "DESKTOP_UPDATE_CONFIG_FILE",
    "DESKTOP_UPDATE_HELPER_NAME",
    "DOWNLOAD_CHUNK_SIZE",
    "GITHUB_API_BASE",
    "GITHUB_API_BASE_ENV",
    "GITHUB_CA_BUNDLE_ENV",
    "HANDOFF_DIAGNOSTICS_FILE",
    "HANDOFF_PENDING_INSTALL_STATES",
    "HANDOFF_REQUEST_FILE",
    "HELPER_DIAGNOSTICS_LOG_FILE",
    "HELPER_RUNTIME_TMP_ROOT_NAME",
    "HELPER_STDERR_LOG_FILE",
    "HELPER_STDOUT_LOG_FILE",
    "INSTALL_PLAN_FILE",
    "INSTALL_STAGE_LABELS",
    "INSTALL_STATES_PRESERVING_DOWNLOADED_ARTIFACT",
    "INSTALL_STATE_FILE",
    "INSTALL_STATE_STAGE_DEFAULTS",
    "MANIFEST_CACHE_FILE",
    "SUCCESS_MARKER_FILE",
    "USER_AGENT",
]
