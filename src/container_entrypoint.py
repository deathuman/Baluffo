#!/usr/bin/env python3
"""Container startup wrapper that prepares /data before dropping privileges."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, cast

from src import container_gateway, container_server

DEFAULT_RUNTIME_USER = "baluffo"
DEFAULT_RUNTIME_UID = 1000
DEFAULT_RUNTIME_GID = 1000
DEFAULT_DATA_ROOT = Path("/data")


def _coerce_int(value: object, default: int) -> int:
    try:
        return int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(default)


def _runtime_identity(env: dict[str, str] | None = None) -> tuple[int, int, str]:
    env_map = env if isinstance(env, dict) else os.environ
    username = str(env_map.get("BALUFFO_RUNTIME_USER") or DEFAULT_RUNTIME_USER).strip()
    uid = _coerce_int(env_map.get("BALUFFO_RUNTIME_UID"), DEFAULT_RUNTIME_UID)
    gid = _coerce_int(env_map.get("BALUFFO_RUNTIME_GID"), DEFAULT_RUNTIME_GID)
    if username:
        try:
            import pwd

            entry = cast(Any, pwd).getpwnam(username)
            return int(entry.pw_uid), int(entry.pw_gid), username
        except (ImportError, KeyError):
            pass
    return uid, gid, username


def _path_is_under(path: Path, root: Path) -> bool:
    resolved_path = Path(path).resolve()
    resolved_root = Path(root).resolve()
    return resolved_path == resolved_root or resolved_root in resolved_path.parents


def _chown_path_no_follow(path: Path, uid: int, gid: int) -> None:
    lchown = getattr(os, "lchown", None)
    if lchown is not None:
        lchown(path, uid, gid)
        return
    cast(Any, os).chown(path, uid, gid)


def _chown_tree(path: Path, uid: int, gid: int) -> None:
    _chown_path_no_follow(path, uid, gid)
    for dirpath, dirnames, filenames in os.walk(path):
        current = Path(dirpath)
        for name in dirnames:
            _chown_path_no_follow(current / name, uid, gid)
        for name in filenames:
            _chown_path_no_follow(current / name, uid, gid)


def _ensure_data_dir_permissions(
    data_dir: Path,
    *,
    uid: int,
    gid: int,
    allowed_root: Path = DEFAULT_DATA_ROOT,
) -> None:
    data_path = Path(data_dir)
    data_path.mkdir(parents=True, exist_ok=True)
    if not _path_is_under(data_path, allowed_root):
        return
    _chown_tree(data_path, uid, gid)


def _drop_privileges(uid: int, gid: int, username: str) -> None:
    if hasattr(os, "initgroups") and username:
        os.initgroups(username, gid)
    cast(Any, os).setgid(gid)
    cast(Any, os).setuid(uid)
    os.environ["HOME"] = f"/home/{username}" if username else "/"
    if username:
        os.environ["USER"] = username
        os.environ["LOGNAME"] = username


def prepare_runtime(argv: list[str] | None = None) -> None:
    if not hasattr(os, "geteuid") or not hasattr(os, "setuid") or not hasattr(os, "setgid"):
        return
    if hasattr(os, "geteuid") and os.geteuid() != 0:
        return
    uid, gid, username = _runtime_identity()
    config = container_server.parse_args(argv)
    _ensure_data_dir_permissions(Path(config.data_dir), uid=uid, gid=gid)
    _drop_privileges(uid, gid, username)


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    prepare_runtime(args)
    return container_gateway.main(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
