"""Linux packaged user data root.

An AppImage payload is mounted read-only, so runtime data cannot live beside the
executable the way a source checkout keeps it in `ship/data`. The packaged build
uses the XDG data directory instead, mirroring how the Windows build uses
`%APPDATA%\\Baluffo`.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path


def linux_data_home(env_map: Mapping[str, str] | None = None) -> Path:
    env = os.environ if env_map is None else env_map
    raw = str(env.get("XDG_DATA_HOME") or "").strip()
    if raw:
        return Path(raw).expanduser()
    home = str(env.get("HOME") or "").strip()
    if home:
        return Path(home).expanduser() / ".local" / "share"
    return Path.home() / ".local" / "share"


def default_linux_packaged_data_dir(env_map: Mapping[str, str] | None = None) -> Path:
    return linux_data_home(env_map) / "Baluffo"


__all__ = ["default_linux_packaged_data_dir", "linux_data_home"]
