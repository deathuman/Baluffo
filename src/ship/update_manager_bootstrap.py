from __future__ import annotations

"""Runtime-bootstrap repair helpers for ship updates."""

import shutil
from pathlib import Path

from .update_manager_paths import (
    BOOTSTRAP_DIR_NAME,
    BOOTSTRAP_ROOT_HTML,
    BOOTSTRAP_VERSION_TAG,
    REQUIRED_VERSION_FILES,
    ShipPaths,
)
from .update_manager_state import write_text_atomic

_BOOTSTRAP_ROOT_HTML = BOOTSTRAP_ROOT_HTML
_BOOTSTRAP_VERSION_TAG = BOOTSTRAP_VERSION_TAG


def refresh_runtime_bootstrap(
    paths: ShipPaths, canonical_version_dir: Path, *, version_name: str
) -> None:
    """Mirror required runtime files from a healthy version dir into ``app/runtime-bootstrap``."""
    root = paths.app / BOOTSTRAP_DIR_NAME
    if root.exists():
        shutil.rmtree(root)
    canonical = canonical_version_dir.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    write_text_atomic(root / BOOTSTRAP_VERSION_TAG, f"{version_name.strip()}\n")
    shutil.copytree(canonical / "src", root / "src")
    for rel in BOOTSTRAP_ROOT_HTML:
        shutil.copy2(canonical / rel, root / rel)


def repair_version_from_runtime_bootstrap(
    paths: ShipPaths, version_dir: Path, active_version_name: str
) -> int:
    """Copy missing required and ``src`` files from ``app/runtime-bootstrap``. Returns files restored."""
    root = paths.app / BOOTSTRAP_DIR_NAME
    tag_path = root / BOOTSTRAP_VERSION_TAG
    if not tag_path.is_file():
        return 0
    if tag_path.read_text(encoding="utf-8").strip() != str(active_version_name).strip():
        return 0
    src_mirror = root / "src"
    if not src_mirror.is_dir():
        return 0
    copied = 0
    for rel in REQUIRED_VERSION_FILES:
        dest = version_dir / rel
        if dest.exists():
            continue
        candidate = root / rel
        if not candidate.is_file():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(candidate, dest)
        copied += 1
    for path in src_mirror.rglob("*"):
        if not path.is_file():
            continue
        rel_under_src = path.relative_to(src_mirror)
        dest = version_dir / "src" / rel_under_src
        if dest.exists():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, dest)
        copied += 1
    return copied
