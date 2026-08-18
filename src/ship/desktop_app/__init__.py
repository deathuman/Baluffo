#!/usr/bin/env python3
"""Stable desktop runtime facade.

AI boundary owns: desktop app compatibility facade, dynamic re-exports, and _COMPAT_MODULES ordering.
AI boundary implement in: this file for facade surface changes; desktop behavior stays in desktop_app leaf modules.
AI boundary search before contracts: launcher flow, platform helpers, packaged smoke, and desktop app import-closure tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused packaged desktop rehearsal tests.
"""

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
from src.ship.desktop_update_shared import DesktopUpdatePaths
from src.ship.desktop_update_state import (
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

from . import _linux as _linux_platform_module
from . import _windows as _windows_platform_module
from . import browser as _browser_module
from . import config as _config_module
from . import launcher as _launcher_module
from . import launcher_diagnostics as _launcher_diagnostics_module
from . import process as _process_module
from . import session as _session_module
from . import startup as _startup_module

# Static imports keep both platform modules in the PyInstaller bundle graph;
# a dynamic importlib call here is invisible to the frozen module analysis and
# the packaged EXE fails at startup with ModuleNotFoundError. Both modules are
# safe to import on either platform (no platform-only module-level code).
_platform_module = _windows_platform_module if os.name == "nt" else _linux_platform_module

_COMPAT_MODULES = (
    _startup_module,
    _session_module,
    _process_module,
    _launcher_module,
    _launcher_diagnostics_module,
    _config_module,
    _browser_module,
    _platform_module,
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
