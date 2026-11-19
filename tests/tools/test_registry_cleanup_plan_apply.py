"""Registry cleanup apply gate and retirement behavior."""

from __future__ import annotations

import json
from pathlib import Path

from src.source_registry_io import load_json_array
from tests.tools.registry_cleanup_plan_helpers import (
    REPOINT_MAP,
    ROWS,
    _active_bytes,
    _fixture,
    _run,
    _verdict,
    _write_registry,
)


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
