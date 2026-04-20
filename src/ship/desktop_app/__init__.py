#!/usr/bin/env python3
"""Desktop entrypoint for portable Baluffo executable builds."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import runpy
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.request
import uuid
import webbrowser
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timezone
from pathlib import Path
from typing import Any

if os.name == "nt":
    import ctypes
    import ctypes.wintypes  # noqa: F401
    import winreg

from src.app_version import get_app_version
from src.ship.desktop_update import (
    DesktopUpdatePaths,
    launch_staged_update_helper,
    load_status,
    updater_install_requested,
    write_success_marker,
)
from src.ship.startup_profile import summarize_startup_metrics, write_startup_summary
from src.ship.startup_telemetry import (
    append_startup_trace as _append_startup_trace,
)
from src.ship.startup_telemetry import (
    read_startup_metrics,
    wait_for_url,
)

from ._windows import *  # noqa: F401,F403
from ._windows import (
    _HANDLE_FLAG_INHERIT,
    _IO_COUNTERS,
    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    _JOBOBJECT_BASIC_LIMIT_INFORMATION,
    _JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
    _PROCESS_ASSIGN_TO_JOB_ACCESS,
    _PROCESS_QUERY_LIMITED_INFORMATION,
    _PROCESS_SET_QUOTA,
    _PROCESS_SYNCHRONIZE,
    _PROCESS_TERMINATE,
    _STILL_ACTIVE,
    _WAIT_TIMEOUT,
    _current_exe_path,
    _enumerate_visible_desktop_windows,
    _filetime_to_unix_seconds,
    _find_baluffo_visible_window,
    _get_windows_process_image_path,
    _get_windows_process_start_ts,
    _is_chromium_window_class,
    _local_address_matches_listen_port,
    _normalize_path_text,
    _pids_listening_on_tcp_port_windows,
    _stale_runtime_reclaim_result,
    _trace_stale_runtime_reclaim,
    _wait_for_process_exit_pid,
    _windows_close_desktop_job,
    _windows_create_kill_on_close_job,
    _windows_process_image_matches,
    _windows_raise_last_error,
    _windows_reclaim_stale_runtime_children,
    _windows_terminate_process_tree_by_pid,
    _windows_try_assign_pid_to_job,
    _windows_try_reclaim_stale_bridge_process,
    _windows_try_reclaim_stale_site_process,
    _windows_window_class_name,
    _windows_window_is_cloaked,
)
from .browser import *  # noqa: F401,F403
from .browser import (
    launch_browser_for_url,
    resolve_chromium_browser_candidates,
)
from .config import *  # noqa: F401,F403
from .config import (
    DesktopRuntimeConfig,
    _port_is_available,
    _truthy_env,
    resolve_ship_root,
)
from .launcher import *  # noqa: F401,F403
from .launcher import (
    _desktop_update_restart_snapshot,
    _recoverable_active_work_browser_loss_result,
    _recoverable_browser_launch_result,
    _runtime_ports_need_retry,
    _should_retry_runtime_launch,
    _trace_already_running_rejection,
    _write_launch_diagnostics,
    launch_desktop_app,
    main,
)
from .process import *  # noqa: F401,F403
from .process import (
    _entry_command,
    _isolated_src_package,
    _patched_syspath,
    _pushd,
    build_child_command,
    start_child_process,
    terminate_process,
)
from .session import *  # noqa: F401,F403
from .session import (
    InstanceLock,
    _bridge_health_matches_owner_session,
    _clear_stale_instance_artifacts,
    _load_active_critical_desktop_tasks,
    _make_lock_payload,
    _normalize_active_task_descriptor,
    _process_identity_matches,
    _read_instance_lock_payload,
    _reclaim_stale_instance_artifacts,
    _task_descriptor_is_active,
    _truncate_reason,
    _write_lock_payload,
    _write_lock_payload_to_handle,
)
from .startup import *  # noqa: F401,F403
from .startup import (
    DesktopStartupReadyTimeout,
    _attempt_active_work_browser_relaunch,
    _find_reveal_handoff_window,
    _is_baluffo_browser_window_open,
    _parse_metric_ts,
    _startup_handoff_signal_events,
    _wait_for_bridge_activity_after,
    _wait_for_browser_reveal,
    watch_browser_session,
)

__all__ = [
    "DesktopRuntimeConfig",
    "InstanceLock",
    "DesktopStartupReadyTimeout",
    "resolve_ship_root",
    "build_child_command",
    "start_child_process",
    "terminate_process",
    "resolve_chromium_browser_candidates",
    "launch_browser_for_url",
    "watch_browser_session",
    "launch_desktop_app",
    "read_startup_metrics",
    "main",
]
