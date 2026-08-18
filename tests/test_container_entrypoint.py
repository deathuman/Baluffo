from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from src import container_entrypoint


def test_ensure_data_dir_permissions_chowns_allowed_tree(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    nested = data_dir / "contracts"
    nested.mkdir(parents=True)
    payload = nested / "country_acceptance.json"
    payload.write_text("{}", encoding="utf-8")
    calls: list[tuple[Path, int, int]] = []

    def record_chown(path: Any, uid: int, gid: int) -> None:
        calls.append((Path(path), uid, gid))

    monkeypatch.setattr(os, "chown", record_chown, raising=False)
    monkeypatch.setattr(os, "lchown", record_chown, raising=False)

    container_entrypoint._ensure_data_dir_permissions(
        data_dir,
        uid=1000,
        gid=1000,
        allowed_root=data_dir,
    )

    assert (data_dir, 1000, 1000) in calls
    assert (nested, 1000, 1000) in calls
    assert (payload, 1000, 1000) in calls


def test_ensure_data_dir_permissions_refuses_outside_allowed_root(
    tmp_path: Path, monkeypatch
) -> None:
    data_dir = tmp_path / "data"
    calls: list[Path] = []

    def record_chown(path: Any, uid: int, gid: int) -> None:
        calls.append(Path(path))

    monkeypatch.setattr(os, "chown", record_chown, raising=False)
    monkeypatch.setattr(os, "lchown", record_chown, raising=False)

    container_entrypoint._ensure_data_dir_permissions(
        data_dir,
        uid=1000,
        gid=1000,
        allowed_root=tmp_path / "other",
    )

    assert data_dir.exists()
    assert calls == []


def test_chown_tree_does_not_follow_data_symlinks(tmp_path: Path, monkeypatch) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    outside_target = tmp_path / "outside.txt"
    outside_target.write_text("outside", encoding="utf-8")
    symlink_path = data_dir / "outside-link"
    try:
        symlink_path.symlink_to(outside_target)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"symlink creation is unavailable in this environment: {exc}")
    lchown_calls: list[Path] = []
    chown_calls: list[Path] = []

    monkeypatch.setattr(
        os,
        "lchown",
        lambda path, uid, gid: lchown_calls.append(Path(path)),
        raising=False,
    )
    monkeypatch.setattr(
        os,
        "chown",
        lambda path, uid, gid: chown_calls.append(Path(path)),
        raising=False,
    )

    container_entrypoint._ensure_data_dir_permissions(
        data_dir,
        uid=1000,
        gid=1000,
        allowed_root=data_dir,
    )

    assert symlink_path in lchown_calls
    assert outside_target not in lchown_calls
    assert chown_calls == []


def test_drop_privileges_sets_runtime_home_and_user(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(
        os, "initgroups", lambda user, gid: calls.append(("initgroups", user)), raising=False
    )
    monkeypatch.setattr(os, "setgid", lambda gid: calls.append(("setgid", gid)), raising=False)
    monkeypatch.setattr(os, "setuid", lambda uid: calls.append(("setuid", uid)), raising=False)
    monkeypatch.setenv("HOME", "/root")

    container_entrypoint._drop_privileges(1000, 1000, "baluffo")

    assert calls == [("initgroups", "baluffo"), ("setgid", 1000), ("setuid", 1000)]
    assert os.environ["HOME"] == "/home/baluffo"
    assert os.environ["USER"] == "baluffo"
    assert os.environ["LOGNAME"] == "baluffo"


def test_prepare_runtime_skips_privilege_drop_when_posix_uid_tools_are_missing(
    monkeypatch,
) -> None:
    monkeypatch.delattr(os, "geteuid", raising=False)
    monkeypatch.delattr(os, "setuid", raising=False)
    monkeypatch.delattr(os, "setgid", raising=False)
    monkeypatch.setattr(
        container_entrypoint.container_server,
        "parse_args",
        lambda _argv=None: (_ for _ in ()).throw(AssertionError("parse_args called")),
    )

    container_entrypoint.prepare_runtime([])
