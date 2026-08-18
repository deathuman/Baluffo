from pathlib import Path
from typing import Any

from src import jobs_fetcher as jf
from src.jobs.state_lifecycle import (
    _index_lifecycle_entry_aliases,
    _initialize_carried_lifecycle_rows,
    _lifecycle_alias_index,
    _resolve_lifecycle_key,
    apply_job_lifecycle_state,
    lifecycle_archive_state_path,
    read_job_lifecycle_archive_state,
    write_job_lifecycle_archive_state,
)
from src.shared.json_io import read_json
from tests.helpers.temp_paths import workspace_tmpdir


def _job_row(index: int, *, availability_id: str = "", url: str | None = None) -> dict[str, object]:
    return {
        "title": f"Role {index}",
        "company": "Lifecycle Studio",
        "city": "Berlin",
        "country": "Germany",
        "jobLink": url or f"https://example.com/role-{index}",
        "source": "static_source::example",
        "sourceJobId": f"job-{index}",
        "postedAt": "2026-03-01",
        "availabilityId": availability_id,
        "firstSeenAt": "2026-03-01T00:00:00+00:00",
        "lastSeenAt": "2026-03-01T00:00:00+00:00",
    }


def _lifecycle_entry(
    key: str,
    *,
    availability_id: str,
    url: str,
    extra_aliases: list[str] | None = None,
) -> tuple[str, dict[str, Any]]:
    entry = {
        "status": "active",
        "title": f"Role {key}",
        "company": "Lifecycle Studio",
        "jobLink": url,
        "source": "static_source::example",
        "sourceJobId": key,
        "availabilityId": availability_id,
        "availabilityAliases": [
            f"availability:{availability_id}",
            *(extra_aliases or []),
        ],
    }
    return key, entry


def test_carried_initialization_index_matches_full_rebuild() -> None:
    """Incremental index updates must equal a full rebuild after the same inserts."""
    next_rows: dict[str, dict[str, object]] = {
        key: dict(entry)
        for key, entry in (
            _lifecycle_entry("existing-1", availability_id="av-1", url="https://example.com/a"),
            _lifecycle_entry("existing-2", availability_id="av-2", url="https://example.com/b"),
        )
    }
    incremental = _lifecycle_alias_index(next_rows)
    for index in range(3):
        key, entry = _lifecycle_entry(
            f"fresh-{index}",
            availability_id=f"av-fresh-{index}",
            url=f"https://example.com/fresh-{index}",
        )
        next_rows[key] = entry
        _index_lifecycle_entry_aliases(incremental, key, entry)
    assert incremental == _lifecycle_alias_index(next_rows)


def test_carried_initialization_later_rows_resolve_fresh_aliases() -> None:
    """Rows following an initialization must resolve the new entry's URL alias."""
    seen: set[str] = set()
    next_rows: dict[str, dict[str, object]] = {}
    fresh_key = "availability:av-fresh-9"
    first = _job_row(9, availability_id="av-fresh-9")
    later = _job_row(10, availability_id="")  # no id: resolves via URL alias only
    later["jobLink"] = first["jobLink"]  # same URL as the initialized row
    payload = [first, later]

    initialized = _initialize_carried_lifecycle_rows(
        [dict(row) for row in payload],
        next_rows,
        seen_keys=seen,
        finished_at="2026-03-01T00:00:00+00:00",
    )
    assert initialized == 1
    assert fresh_key in next_rows
    index = _lifecycle_alias_index(next_rows)
    assert _resolve_lifecycle_key(later, next_rows, index) == fresh_key


def test_carried_initialization_regression_bounded_time() -> None:
    """O(N·K) per-row index rebuilds must not return: ~300 fresh rows over
    8k lifecycle entries must stay far under a 15 s budget (the pre-fix
    implementation took 90 s+ at this scale)."""
    import time as _time

    next_rows: dict[str, dict[str, object]] = {
        key: dict(entry)
        for key, entry in (
            _lifecycle_entry(
                f"seed-{index}",
                availability_id=f"seed-{index}",
                url=f"https://example.com/seed-{index}",
            )
            for index in range(8_000)
        )
    }
    payload = [_job_row(index, availability_id=f"fresh-{index}") for index in range(300)]
    started = _time.perf_counter()
    initialized = _initialize_carried_lifecycle_rows(
        [dict(row) for row in payload],
        next_rows,
        seen_keys=set(),
        finished_at="2026-03-01T00:00:00+00:00",
    )
    elapsed = _time.perf_counter() - started
    assert initialized == 300
    assert elapsed < 15.0, f"carried initialization took {elapsed:.1f}s"


def test_pipeline_lifecycle_state_retains_city_and_country_for_removed_rows() -> None:
    def one_job_loader(**_: object):
        return [
            {
                "title": "Engine Programmer",
                "company": "Lifecycle Studio",
                "city": "Berlin",
                "country": "Germany",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/lifecycle/engine-programmer",
                "sector": "Game",
                "sourceJobId": "life-1",
                "postedAt": "2026-03-01",
            }
        ]

    def empty_loader(**_: object):
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-lifecycle-state") as tmp:
            out = Path(tmp)
            jf.default_source_loaders = lambda: [("only_source", one_job_loader)]
            jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            jf.default_source_loaders = lambda: [("only_source", empty_loader)]
            jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            payload = read_json(out / "jobs-lifecycle-state.json", {})
            jobs_map = payload.get("jobs") or {}
            assert len(jobs_map) == 1
            entry = list(jobs_map.values())[0]
            assert entry["status"] == "likely_removed"
            assert entry["city"] == "Berlin"
            assert entry["country"] == "DE"
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_pipeline_marks_reappeared_rows_in_output() -> None:
    def one_job_loader(**_: object):
        return [
            {
                "title": "Engine Programmer",
                "company": "Lifecycle Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/lifecycle/engine-programmer",
                "sector": "Game",
                "sourceJobId": "life-1",
                "postedAt": "2026-03-01",
            }
        ]

    def empty_loader(**_: object):
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-reappeared") as tmp:
            out = Path(tmp)
            jf.default_source_loaders = lambda: [("only_source", one_job_loader)]
            jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            jf.default_source_loaders = lambda: [("only_source", empty_loader)]
            jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            jf.default_source_loaders = lambda: [("only_source", one_job_loader)]
            third = jf.run_pipeline(
                output_dir=out, preserve_previous_on_empty=False, force_refresh_all=True
            )

            assert int(third["summary"].get("outputCount") or 0) == 1
            rows = read_json(out / "jobs-unified.json", [])
            assert len(rows) == 1
            assert rows[0]["lifecycleEvent"] == "reappeared"
            assert rows[0]["lifecycleReason"] == ""
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_pipeline_lifecycle_state_moves_old_archived_rows_to_cold_archive() -> None:
    with workspace_tmpdir("jobs-fetcher-lifecycle-archive") as tmp:
        state_path = Path(tmp) / "jobs-lifecycle-state.json"
        hot_rows = {
            "job-1": {
                "status": "archived",
                "archivedAt": "2024-05-04T12:00:00+00:00",
                "removedAt": "2024-04-01T12:00:00+00:00",
                "title": "Legacy Engine Programmer",
            }
        }

        _rows, next_rows, archive_rows_by_year, _summary = apply_job_lifecycle_state(
            deduped_rows=[],
            lifecycle_rows=hot_rows,
            finished_at="2026-05-04T12:00:00+00:00",
            allow_mark_missing=False,
            archive_retention_days=1,
        )

        assert next_rows == {}
        assert archive_rows_by_year[2024]["job-1"]["status"] == "archived"

        archive_state_path = lifecycle_archive_state_path(state_path, 2024)
        write_job_lifecycle_archive_state(archive_state_path, archive_rows_by_year[2024])

        archived_rows = read_job_lifecycle_archive_state(archive_state_path)
        assert archived_rows["job-1"]["title"] == "Legacy Engine Programmer"
