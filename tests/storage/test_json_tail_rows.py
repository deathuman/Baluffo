"""Tests for ``src.shared.json_io.read_jsonl_rows_tail``.

The reader is the trim-on-read tail slice used by the startup metrics readers
(``startup_telemetry`` and the bridge ``runtime_state``), so the row-ordering,
invalid-line-skipping, and missing-file behaviors must match a full-file scan.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

from src.shared import json_io


def _row(number: int) -> str:
    return json.dumps({"schemaVersion": 1, "n": number, "row": f"row-{number}"})


def _big_row(number: int) -> str:
    return json.dumps(
        {"schemaVersion": 1, "n": number, "row": f"row-{number}", "padding": "x" * 200}
    )


def _write_rows(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_read_jsonl_rows_tail_returns_last_rows_in_file_order(tmp_path: Path) -> None:
    path = tmp_path / "metrics.jsonl"
    _write_rows(path, [_row(1), _row(2), _row(3), _row(4), _row(5)])

    rows = json_io.read_jsonl_rows_tail(path, limit=2)

    assert [row["n"] for row in rows] == [4, 5]


def test_read_jsonl_rows_tail_skips_invalid_and_blank_rows(tmp_path: Path) -> None:
    path = tmp_path / "metrics.jsonl"
    path.write_text(
        "\n".join(["{bad json", "", _row(1), _row(2)] + ["", "not json"]),
        encoding="utf-8",
    )

    rows = json_io.read_jsonl_rows_tail(path, limit=10)

    assert [row["n"] for row in rows] == [1, 2]


def test_read_jsonl_rows_tail_missing_file_returns_empty(tmp_path: Path) -> None:
    rows = json_io.read_jsonl_rows_tail(tmp_path / "missing.jsonl", limit=5)
    assert rows == []


def test_read_jsonl_rows_tail_empty_file_returns_empty(tmp_path: Path) -> None:
    path = tmp_path / "metrics.jsonl"
    path.write_text("", encoding="utf-8")
    assert json_io.read_jsonl_rows_tail(path, limit=5) == []


def test_read_jsonl_rows_tail_limit_zero_reads_all_rows(tmp_path: Path) -> None:
    path = tmp_path / "metrics.jsonl"
    _write_rows(path, [_row(1), _row(2), _row(3)])

    rows = json_io.read_jsonl_rows_tail(path, limit=0)

    assert [row["n"] for row in rows] == [1, 2, 3]


def test_read_jsonl_rows_tail_multiblock_window_keeps_order(tmp_path: Path) -> None:
    """A window spanning several backward read blocks loses no rows and keeps order."""
    lines = [_big_row(index) for index in range(1, 21)]
    path = tmp_path / "metrics.jsonl"
    _write_rows(path, lines)

    with mock.patch.object(json_io, "_TAIL_BLOCK_SIZE", 64):
        rows = json_io.read_jsonl_rows_tail(path, limit=6)

    assert [row["n"] for row in rows] == [15, 16, 17, 18, 19, 20]
    assert all(len(row.get("row", "")) > 0 for row in rows)


def test_read_jsonl_rows_tail_fallback_reads_all_when_window_is_sparse(tmp_path: Path) -> None:
    """Few valid rows amid many invalid lines still reach past the window."""
    lines: list[str] = []
    for index in range(1, 4):
        lines.append(_row(index))
        lines.extend(["garbage-line", "{bad json", ""])
    path = tmp_path / "metrics.jsonl"
    _write_rows(path, lines)

    with mock.patch.object(json_io, "_TAIL_BLOCK_SIZE", 64):
        rows = json_io.read_jsonl_rows_tail(path, limit=2)

    assert [row["n"] for row in rows] == [2, 3]
