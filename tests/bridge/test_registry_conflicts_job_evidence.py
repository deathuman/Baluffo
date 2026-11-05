"""Registry conflicts report which job-evidence path the refresh used."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.bridge.registry_conflicts import (
    load_registry_conflicts_payload,
    summarize_registry_conflicts_payload,
)
from src.bridge.registry_conflicts_summary import load_registry_conflicts_summary_payload
from src.storage import BaluffoStore, JobRuntimeStore
from tests.bridge.test_registry_conflicts_independent_providers import (
    _greenhouse_job_row,
    _sony_greenhouse_state,
)


def test_summary_preserves_the_job_evidence_block() -> None:
    summary = summarize_registry_conflicts_payload(
        {
            "summary": {"conflictCount": 0},
            "warnings": [],
            "jobEvidence": {
                "source": "sqlite_source_index",
                "rowCount": 5,
                "truncated": False,
            },
            "conflicts": [],
        }
    )

    assert summary["jobEvidence"] == {
        "source": "sqlite_source_index",
        "rowCount": 5,
        "truncated": False,
    }


def test_summary_reports_unavailable_evidence_before_the_first_refresh(
    tmp_path: Path,
) -> None:
    payload = load_registry_conflicts_summary_payload(
        registry_summary={"activeCount": 1},
        source_state_path=tmp_path / "jobs-source-state.json",
    )

    assert payload["summaryStatus"] == "pending"
    assert payload["jobEvidence"] == {
        "source": "unavailable",
        "rowCount": 0,
        "truncated": False,
        "feedPublishedAt": "",
        "stale": False,
    }


def _seed_sqlite_feed(tmp_path: Path) -> None:
    with BaluffoStore(tmp_path) as store:
        store.set_authority_mode("jobsFeed", "sqlite", reason="test")
        JobRuntimeStore(store, now_iso=lambda: "2026-05-12T10:00:00+00:00").replace_feed(
            run_id="seed",
            rows=[_greenhouse_job_row("siei", "100001")],
        )


def _payload_with_source_state(tmp_path: Path, source_state: dict[str, Any]) -> dict[str, Any]:
    source_state_path = tmp_path / "jobs-source-state.json"
    source_state_path.write_text(json.dumps(source_state), encoding="utf-8")
    return load_registry_conflicts_payload(
        load_state=_sony_greenhouse_state,
        load_json_object=lambda _path, default: default,
        source_state_path=source_state_path,
    )


def test_sqlite_generation_older_than_the_source_state_warns(tmp_path: Path) -> None:
    _seed_sqlite_feed(tmp_path)

    payload = _payload_with_source_state(
        tmp_path,
        {"sources": {"greenhouse:siei": {"lastRunAt": "2026-05-12T11:00:00+00:00"}}},
    )

    assert payload["jobEvidence"] == {
        "source": "sqlite_source_index",
        "rowCount": 1,
        "truncated": False,
        "feedPublishedAt": "2026-05-12T10:00:00+00:00",
        "stale": True,
    }
    assert "sqlite_jobs_feed_stale" in payload["warnings"]


def test_sqlite_generation_newer_than_the_source_state_is_fresh(tmp_path: Path) -> None:
    _seed_sqlite_feed(tmp_path)

    payload = _payload_with_source_state(
        tmp_path,
        {"sources": {"greenhouse:siei": {"lastRunAt": "2026-05-12T09:00:00+00:00"}}},
    )

    assert payload["jobEvidence"]["stale"] is False
    assert "sqlite_jobs_feed_stale" not in payload.get("warnings", [])
