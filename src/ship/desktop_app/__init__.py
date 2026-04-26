#!/usr/bin/env python3
"""Stable desktop runtime facade for focused desktop_app modules."""

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
    import ctypes.wintypes
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

from . import _windows as _windows_module
from . import browser as _browser_module
from . import config as _config_module
from . import launcher as _launcher_module
from . import launcher_diagnostics as _launcher_diagnostics_module
from . import process as _process_module
from . import session as _session_module
from . import startup as _startup_module

_COMPAT_MODULES = (
    _startup_module,
    _session_module,
    _process_module,
    _launcher_module,
    _launcher_diagnostics_module,
    _config_module,
    _browser_module,
    _windows_module,
)

__all__ = [
    "DesktopRuntimeConfig",
    "DesktopStartupReadyTimeout",
    "InstanceLock",
    "build_child_command",
    "launch_browser_for_url",
    "launch_desktop_app",
    "main",
    "read_startup_metrics",
    "resolve_chromium_browser_candidates",
    "resolve_ship_root",
    "start_child_process",
    "terminate_process",
    "watch_browser_session",
]


def __getattr__(name: str) -> object:
    for module in _COMPAT_MODULES:
        if hasattr(module, name):
            value = getattr(module, name)
            globals()[name] = value
            return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    names = set(globals())
    for module in _COMPAT_MODULES:
        names.update(
            attr for attr in dir(module) if not (attr.startswith("__") and attr.endswith("__"))
        )
    return sorted(names)
