from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import cast

import pytest

from src.storage.db_storage_diagnostic import (
    StorageDiagnosticCancelled,
    _dbstat_tables,
    measure_storage_diagnostic,
)


def _seed(path: Path) -> None:
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE job_sources (
            feed_generation TEXT NOT NULL,
            job_key TEXT NOT NULL,
            source_ordinal INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (feed_generation, job_key, source_ordinal)
        );
        CREATE INDEX idx_job_sources_generation
            ON job_sources(feed_generation);
        CREATE TABLE job_feed_state (
            id INTEGER PRIMARY KEY,
            current_generation TEXT NOT NULL,
            run_id TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            source_count INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            published_at TEXT NOT NULL
        );
        """
    )
    rows = [
        ("gen-current", "job-a", 0, '{"title":"A"}', "2026-11-09T10:00:00Z"),
        ("gen-current", "job-b", 0, '{"title":"B"}', "2026-11-09T10:01:00Z"),
        ("gen-old", "job-c", 0, '{"title":"C"}', "2026-11-08T10:00:00Z"),
    ]
    con.executemany(
        "INSERT INTO job_sources VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    con.execute(
        """
        INSERT INTO job_feed_state VALUES
            (1, 'gen-current', 'fetch_1', 2, 1, '2026-11-09T10:01:00Z', '2026-11-09T10:01:00Z')
        """
    )
    con.commit()
    con.close()


def test_exact_diagnostic_groups_job_sources_by_generation(tmp_path: Path) -> None:
    path = tmp_path / "runtime.db"
    _seed(path)

    result = measure_storage_diagnostic(path)

    assert result["status"] == "complete"
    assert result["exactContent"] is True
    assert result["databaseBytes"] > 0
    job_sources = result["jobSources"]
    assert job_sources["table"] == "job_sources"
    assert job_sources["rowCount"] == 3
    assert job_sources["generationCount"] == 2
    assert job_sources["currentGeneration"] == "gen-current"
    assert job_sources["payloadBytes"] == sum(
        len(row[3].encode("utf-8"))
        for row in [
            ("gen-current", "job-a", 0, '{"title":"A"}', "2026-11-09T10:00:00Z"),
            ("gen-current", "job-b", 0, '{"title":"B"}', "2026-11-09T10:01:00Z"),
            ("gen-old", "job-c", 0, '{"title":"C"}', "2026-11-08T10:00:00Z"),
        ]
    )
    by_generation = {row["generation"]: row for row in job_sources["byGeneration"]}
    assert by_generation["gen-current"]["rowCount"] == 2
    assert by_generation["gen-current"]["isCurrent"] is True
    assert by_generation["gen-current"]["isOrphan"] is False
    assert by_generation["gen-old"]["isCurrent"] is False
    assert by_generation["gen-old"]["isOrphan"] is True
    assert (
        by_generation["gen-current"]["contentBytes"] >= by_generation["gen-current"]["payloadBytes"]
    )
    table_sizes = result["tableSizes"]
    assert table_sizes["status"] in {"ok", "unavailable"}
    assert table_sizes["databaseBytes"] > 0
    if table_sizes["method"] == "dbstat":
        assert result["exactAllocation"] is True
        assert any(row["name"] == "job_sources" for row in table_sizes["tables"])
        assert job_sources["tableBytes"] is not None
    else:
        assert table_sizes["status"] == "unavailable"
        assert result["exactAllocation"] is False
        assert "dbstat" in table_sizes["error"].lower()
        assert job_sources["tableBytes"] is None


def test_exact_diagnostic_is_cancellable_from_the_progress_handler(tmp_path: Path) -> None:
    path = tmp_path / "runtime.db"
    _seed(path)

    with pytest.raises(StorageDiagnosticCancelled):
        measure_storage_diagnostic(path, cancel_requested=lambda: True)


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, _sql):
        return _FakeCursor(self._rows)


def test_dbstat_rows_are_attributed_to_tables_and_indexes() -> None:
    result = _dbstat_tables(
        cast(
            sqlite3.Connection,
            _FakeConnection(
                [
                    ("job_sources", 4096, 1),
                    ("idx_job_sources_generation", 2048, 1),
                    ("sqlite_schema", 4096, 1),
                ]
            ),
        ),
        ["job_sources"],
        {"idx_job_sources_generation": "job_sources"},
    )

    assert result is not None
    rows, unassigned = result
    assert rows == [
        {
            "name": "job_sources",
            "tableBytes": 4096,
            "indexBytes": 2048,
            "bytes": 6144,
            "pageCount": 2,
        }
    ]
    assert unassigned == 4096
