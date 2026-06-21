"""Shared imports and helpers for packaged desktop rehearsal tests."""

import base64
import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from urllib.request import Request, urlopen

import pytest

from src import packaged_desktop_smoke as smoke
from src import source_sync
from src.ship.packaged_smoke import runtime_process
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT
from tests.helpers.temp_paths import workspace_tmpdir

from ._helpers import _write_packaged_sync_bundle_config

__all__ = [
    "ADMIN_BRIDGE_TEST_PORT",
    "Path",
    "Request",
    "SimpleNamespace",
    "_PathReadFailure",
    "_write_packaged_sync_bundle_config",
    "base64",
    "json",
    "mock",
    "os",
    "pytest",
    "runtime_process",
    "smoke",
    "source_sync",
    "subprocess",
    "urlopen",
    "workspace_tmpdir",
]


class _PathReadFailure:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def exists(self) -> bool:
        return True

    def is_file(self) -> bool:
        return True

    def read_text(self, *args, **kwargs) -> str:  # noqa: ANN002, ANN003
        raise self._exc
