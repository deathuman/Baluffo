from __future__ import annotations

import sqlite3
from pathlib import Path

from src.storage.db_table_sizes import measure_table_sizes


def _seed_database(path: Path) -> None:
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, payload TEXT)")
    con.execute("CREATE TABLE small (id INTEGER PRIMARY KEY, label TEXT)")
    con.executemany("INSERT INTO big(payload) VALUES (?)", [("x" * 2000,) for _ in range(300)])
    con.executemany("INSERT INTO small(label) VALUES (?)", [("s",) for _ in range(10)])
    con.commit()
    con.close()


def test_table_sizes_ranks_the_largest_table_first(tmp_path: Path) -> None:
    path = tmp_path / "probe.db"
    _seed_database(path)

    result = measure_table_sizes(path, sample_rows=48, budget_ms=5_000, exact_max_bytes=0)

    assert result["status"] == "ok"
    assert result["method"] == "rowid-sample"
    assert result["estimated"] is True
    assert result["complete"] is True
    assert result["allocatedBytes"] > 0
    names = [row["name"] for row in result["tables"]]
    by_name = {row["name"]: row for row in result["tables"]}
    assert names[0] == "big"
    assert by_name["big"]["bytes"] > by_name["small"]["bytes"]
    assert by_name["big"]["rowCount"] > 0
    assert result["otherBytes"] >= 0


def test_table_sizes_bounds_the_reported_table_list(tmp_path: Path) -> None:
    path = tmp_path / "probe.db"
    _seed_database(path)

    result = measure_table_sizes(
        path, sample_rows=8, max_tables=1, budget_ms=5_000, exact_max_bytes=0
    )

    assert len(result["tables"]) == 1
    assert result["truncated"] is True


def test_table_sizes_reports_partial_when_the_budget_is_spent(tmp_path: Path) -> None:
    path = tmp_path / "probe.db"
    _seed_database(path)

    result = measure_table_sizes(path, budget_ms=0)

    assert result["status"] == "partial"
    assert result["complete"] is False
    # timing-ok: the zero budget must return without doing any sampling work.
    assert result["durationMs"] < 1_000
    assert result["tables"] == []


def test_table_sizes_skips_without_rowid_tables(tmp_path: Path) -> None:
    path = tmp_path / "probe.db"
    _seed_database(path)
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT) WITHOUT ROWID")
    con.execute("INSERT INTO kv VALUES ('a', 'b')")
    con.commit()
    con.close()

    result = measure_table_sizes(path, sample_rows=8, budget_ms=5_000, exact_max_bytes=0)

    assert result["method"] == "rowid-sample"
    assert "kv" in result["skippedTables"]
    assert all(row["name"] != "kv" for row in result["tables"])


def test_table_sizes_reports_a_missing_database(tmp_path: Path) -> None:
    result = measure_table_sizes(tmp_path / "missing.db")

    assert result["status"] == "unavailable"
    assert "not found" in result["error"]
    assert result["tables"] == []
    assert result["durationMs"] >= 0


def test_table_sizes_measures_large_payloads_without_materializing_them(tmp_path: Path) -> None:
    path = tmp_path / "large-row.db"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE payloads (id INTEGER PRIMARY KEY, payload TEXT)")
    con.execute("INSERT INTO payloads(payload) VALUES (?)", ("x" * (5 * 1024 * 1024),))
    con.commit()
    con.close()

    result = measure_table_sizes(path, sample_rows=4, budget_ms=5_000, exact_max_bytes=0)

    assert result["status"] == "ok"
    assert result["method"] == "rowid-sample"
    assert result["tables"][0]["name"] == "payloads"
    assert result["tables"][0]["bytes"] >= 5 * 1024 * 1024


def test_table_sizes_returns_partial_when_sampling_is_interrupted(
    tmp_path: Path, monkeypatch
) -> None:
    path = tmp_path / "probe.db"
    _seed_database(path)

    def interrupted(*_args, **_kwargs):
        raise sqlite3.OperationalError("interrupted")

    monkeypatch.setattr("src.storage.db_table_sizes._sample_rows", interrupted)

    result = measure_table_sizes(path, sample_rows=8, budget_ms=5_000, exact_max_bytes=0)

    assert result["status"] == "partial"
    assert result["method"] == "rowid-sample"
    assert result["complete"] is False
    assert result["tables"] == []
