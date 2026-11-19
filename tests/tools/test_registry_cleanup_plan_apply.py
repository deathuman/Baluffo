"""Registry cleanup apply gate and retirement behavior."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from src.source_registry_io import load_json_array
from tests.tools.registry_cleanup_plan_helpers import (
    REPOINT_MAP,
    ROWS,
    TOOL,
    _active_bytes,
    _fixture,
    _run,
    _verdict,
    _write_registry,
)


def _load_tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location("registry_cleanup_plan_under_test", TOOL)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_unreachable_root_is_planned_but_never_retired(tmp_path: Path) -> None:
    data = tmp_path / "data"
    _write_registry(
        data,
        [
            {
                "id": "static:name:timeout",
                "name": "Timeout Studio",
                "studio": "Timeout Studio",
                "adapter": "static",
                "registryState": "active",
                "pages": ["https://timeout.example/careers"],
            }
        ],
    )
    verdicts = tmp_path / "verdicts.json"
    verdicts.write_text(
        json.dumps(
            [
                _verdict(
                    "https://timeout.example/careers",
                    root_status=0,
                    url_status=0,
                    verdict="retire_dead_host",
                    had_success=False,
                )
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    plan = _run("--data-dir", str(data), "--verdicts", str(verdicts))
    assert plan.returncode == 0, plan.stderr
    assert "applicable 0" in plan.stdout
    assert "[review]" in plan.stdout

    applied = _run(
        "--data-dir",
        str(data),
        "--verdicts",
        str(verdicts),
        "--apply",
        "--expect-retire",
        "0",
        "--expect-repoint",
        "0",
        "--backup-dir",
        str(tmp_path / "backup"),
    )
    assert applied.returncode == 0, applied.stderr
    assert "verification ok" in applied.stdout
    active = load_json_array(data / "source-registry-active.json", [])
    assert [row["id"] for row in active] == ["static:name:timeout"]


def test_terminal_root_with_nonterminal_registered_url_is_never_retired(tmp_path: Path) -> None:
    """A 404 host root does not prove the registered board is gone when the URL is 500."""
    data = tmp_path / "data"
    _write_registry(
        data,
        [
            {
                "id": "static:name:gone",
                "name": "Gone Studio",
                "studio": "Gone Studio",
                "adapter": "static",
                "registryState": "active",
                "pages": ["https://gone.example/careers"],
            }
        ],
    )
    verdicts = tmp_path / "verdicts.json"
    verdicts.write_text(
        json.dumps(
            [
                _verdict(
                    "https://gone.example/careers",
                    root_status=404,
                    url_status=500,
                    verdict="retire_dead_host",
                )
            ],
            indent=2,
        ),
        encoding="utf-8",
    )

    plan = _run("--data-dir", str(data), "--verdicts", str(verdicts))
    assert plan.returncode == 0, plan.stderr
    assert "applicable 0" in plan.stdout
    assert "[review]" in plan.stdout

    applied = _run(
        "--data-dir",
        str(data),
        "--verdicts",
        str(verdicts),
        "--apply",
        "--expect-retire",
        "0",
        "--expect-repoint",
        "0",
        "--backup-dir",
        str(tmp_path / "backup"),
    )
    assert applied.returncode == 0, applied.stderr
    active = load_json_array(data / "source-registry-active.json", [])
    assert [row["id"] for row in active] == ["static:name:gone"]


def test_apply_refuses_without_expected_sizes(tmp_path: Path) -> None:
    data, verdicts, repoint_map = _fixture(tmp_path)
    before = _active_bytes(data)
    result = _run(
        "--data-dir",
        str(data),
        "--verdicts",
        str(verdicts),
        "--repoint-map",
        str(repoint_map),
        "--apply",
        "--backup-dir",
        str(tmp_path / "backup"),
    )
    assert result.returncode == 2
    assert "refusing to apply" in result.stderr
    assert _active_bytes(data) == before


def test_board_without_control_evidence_is_never_retired(tmp_path: Path) -> None:
    """A terminal root alone is not enough: the artifact must carry the passing control."""
    data = tmp_path / "data"
    _write_registry(data, ROWS)
    verdicts = tmp_path / "verdicts.json"
    verdicts.write_text(
        json.dumps(
            [
                _verdict(
                    "https://gone.example/careers",
                    root_status=404,
                    verdict="retire_dead_host",
                    control=None,
                ),
                _verdict(
                    "https://stale.example/jobs",
                    verdict="retire_stale_seed",
                    had_success=False,
                ),
                _verdict("https://moved.example/careers/old"),
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    repoint_map = tmp_path / "repoint.json"
    repoint_map.write_text(json.dumps(REPOINT_MAP, indent=2), encoding="utf-8")

    plan = _run(
        "--data-dir",
        str(data),
        "--verdicts",
        str(verdicts),
        "--repoint-map",
        str(repoint_map),
    )
    assert plan.returncode == 0, plan.stderr
    assert "applicable 1" in plan.stdout
    assert "control=missing" in plan.stdout

    result = _run(
        "--data-dir",
        str(data),
        "--verdicts",
        str(verdicts),
        "--repoint-map",
        str(repoint_map),
        "--apply",
        "--expect-retire",
        "1",
        "--expect-repoint",
        "1",
        "--backup-dir",
        str(tmp_path / "backup"),
    )
    assert result.returncode == 0, result.stderr
    active = load_json_array(data / "source-registry-active.json", [])
    rejected = load_json_array(data / "source-registry-rejected.json", [])
    assert {row["id"] for row in active} == {
        "static:name:gone",
        "static:name:moved",
        "static:name:blocked",
    }
    assert {row["id"] for row in rejected} == {"static:name:stale"}


def test_apply_retires_and_repoints_then_verifies(tmp_path: Path) -> None:
    data, verdicts, repoint_map = _fixture(tmp_path)
    backup = tmp_path / "backup"
    result = _run(
        "--data-dir",
        str(data),
        "--verdicts",
        str(verdicts),
        "--repoint-map",
        str(repoint_map),
        "--apply",
        "--expect-retire",
        "2",
        "--expect-repoint",
        "1",
        "--backup-dir",
        str(backup),
    )
    assert result.returncode == 0, result.stderr
    assert "verification ok" in result.stdout

    # The canonical writer stores the lean entrypoint as .json.gz, so read back through
    # the same loader the app uses rather than assuming the plain .json survived.
    active = load_json_array(data / "source-registry-active.json", [])
    rejected = load_json_array(data / "source-registry-rejected.json", [])
    assert {row["id"] for row in active} == {"static:name:moved", "static:name:blocked"}
    assert {row["id"] for row in rejected} == {"static:name:gone", "static:name:stale"}
    moved = next(row for row in active if row["id"] == "static:name:moved")
    assert moved["pages"] == ["https://moved.example/careers"]
    assert moved["listing_url"] == "https://moved.example/careers"
    assert moved["careersUrl"] == "https://moved.example/careers"
    assert all(row["registryState"] == "rejected" for row in rejected)

    backups = list(backup.glob("backup-*"))
    assert len(backups) == 1
    saved = json.loads((backups[0] / "active.json").read_text(encoding="utf-8"))
    assert {row["id"] for row in saved} == {
        "static:name:gone",
        "static:name:stale",
        "static:name:moved",
        "static:name:blocked",
    }


def test_apply_restores_the_backup_when_post_write_verification_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A write that loses a bucket must not leave the registry half-applied."""
    data, verdicts, repoint_map = _fixture(tmp_path)
    backup_dir = tmp_path / "backup"
    (data / "source-registry-rejected.json").write_text(
        json.dumps(
            [
                {
                    "id": "static:name:archived",
                    "studio": "Archived Studio",
                    "pages": ["https://archived.example/jobs"],
                    "registryState": "rejected",
                }
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    before = {
        bucket: load_json_array(data / f"source-registry-{bucket}.json", [])
        for bucket in ("active", "pending", "rejected")
    }
    module = _load_tool()
    real_save = module.save_registry_state_atomic
    save_calls = {"count": 0}

    def drop_rejected_once(active_path, pending_path, rejected_path, state) -> None:
        save_calls["count"] += 1
        written = dict(state)
        if save_calls["count"] == 1:
            # Simulate a writer bug: the applied retirement rows never reach the
            # rejected bucket, so the real read-back verification reports them.
            written["rejected"] = []
        real_save(active_path, pending_path, rejected_path, written)

    monkeypatch.setattr(module, "save_registry_state_atomic", drop_rejected_once)
    code = module.main(
        [
            "--data-dir",
            str(data),
            "--verdicts",
            str(verdicts),
            "--repoint-map",
            str(repoint_map),
            "--apply",
            "--expect-retire",
            "2",
            "--expect-repoint",
            "1",
            "--backup-dir",
            str(backup_dir),
        ]
    )
    captured = capsys.readouterr()

    backups = list(backup_dir.glob("backup-*"))
    assert len(backups) == 1
    assert code == 1
    assert "verification FAILED" in captured.err
    assert "missing from rejected after retire" in captured.err
    assert f"restored backup: {backups[0]}" in captured.out
    assert save_calls["count"] == 2

    restored = {
        bucket: load_json_array(data / f"source-registry-{bucket}.json", [])
        for bucket in ("active", "pending", "rejected")
    }
    assert {row["id"] for row in restored["active"]} == {row["id"] for row in before["active"]}
    assert restored["pending"] == before["pending"]
    assert {row["id"] for row in restored["rejected"]} == {row["id"] for row in before["rejected"]}
    assert "static:name:archived" in {row["id"] for row in restored["rejected"]}
    moved = next(row for row in restored["active"] if row["id"] == "static:name:moved")
    assert moved["pages"] == ["https://moved.example/careers/old"]


@pytest.mark.parametrize(
    ("bucket", "payload"),
    [
        ("active", None),
        ("active", "{not json"),
        ("active", '{"active": []}'),
        ("active", "[3]"),
        ("pending", "{not json"),
        ("rejected", '{"rejected": []}'),
    ],
)
def test_restore_backup_refuses_an_unreadable_bucket_before_writing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bucket: str,
    payload: str | None,
) -> None:
    """A missing or malformed bucket must abort before any file is rewritten."""
    data, _verdicts, _repoint_map = _fixture(tmp_path)
    module = _load_tool()
    paths = module.registry_paths(data)
    before = {name: path.read_bytes() for name, path in paths.items()}
    backup = tmp_path / "backup"
    backup.mkdir()
    for name in ("active", "pending", "rejected"):
        if name == bucket:
            continue
        (backup / f"{name}.json").write_text("[]\n", encoding="utf-8")
    if payload is not None:
        (backup / f"{bucket}.json").write_text(payload, encoding="utf-8")

    writes: list[tuple[Any, ...]] = []

    def record_write(*args: Any, **_kwargs: Any) -> None:
        writes.append(args)

    monkeypatch.setattr(module, "save_registry_state_atomic", record_write)

    with pytest.raises(ValueError, match=bucket):
        module.restore_backup(backup, paths)

    assert writes == []
    assert {name: path.read_bytes() for name, path in paths.items()} == before
