from __future__ import annotations

import json
from typing import Any

import pytest

from src.source_discovery import gamedevmap_active_dry_run as dry_run
from src.source_discovery import recovery_escalation
from src.source_discovery.config import DEFAULT_DISCOVERY_CONFIG
from src.source_discovery.directory_page_recovery import fetch_recovery_jobs

from ._helpers import sd, workspace_tmpdir
from .gamedevmap_test_helpers import (
    gamedevmap_config,
    gamedevmap_fetcher,
    gamedevmap_payloads,
)


@pytest.fixture(autouse=True)
def _reset_recovery_escalation_config():
    recovery_escalation.set_escalation_config(None)
    yield
    recovery_escalation.set_escalation_config(None)


def test_batch_scoped_recovery_cache_clears_at_batch_boundaries() -> None:
    cache: dict[str, dict[str, Any]] = {"https://example.com/jobs": {"text": "cached"}}
    events: list[dict[str, Any]] = []
    callback = dry_run._with_batch_scoped_recovery_cache(cache, events.append)

    callback({"phase": "recovery_wave1_fetch", "batch": 1})
    assert cache and events[-1]["phase"] == "recovery_wave1_fetch"

    callback({"phase": "batch_start", "batch": 2})
    assert cache == {}
    cache["https://example.com/jobs"] = {"text": "next batch"}
    callback({"phase": "batch_start", "batch": 3})
    assert cache == {}
    assert [event["phase"] for event in events] == [
        "recovery_wave1_fetch",
        "batch_start",
        "batch_start",
    ]


def test_recovery_cache_preserves_wave_local_results_until_batch_reset() -> None:
    calls: list[list[str]] = []

    def fetch_pages(
        _timeout_s: int, jobs: list[dict[str, Any]], **_kwargs: Any
    ) -> list[dict[str, Any]]:
        urls = [str(job.get("url") or "") for job in jobs]
        calls.append(urls)
        return [
            {
                "job": job,
                "url": str(job.get("url") or ""),
                "ok": True,
                "text": "shared body",
                "error": "",
            }
            for job in jobs
        ]

    cache: dict[str, dict[str, Any]] = {}
    job = {
        "url": "https://example.com/jobs",
        "payload": {"homepageUrl": "https://example.com"},
        "name": "example",
        "adapter": "gamedevmap",
        "failureStage": "recovery_fetch",
    }
    first = fetch_recovery_jobs(
        5,
        [job],
        fetcher=lambda *_args: "",
        total_concurrency=1,
        per_host_concurrency=1,
        progress_label="wave 1",
        recovery_cache=cache,
        fetch_pages=fetch_pages,
    )
    second = fetch_recovery_jobs(
        5,
        [job],
        fetcher=lambda *_args: "",
        total_concurrency=1,
        per_host_concurrency=1,
        progress_label="wave 2",
        recovery_cache=cache,
        fetch_pages=fetch_pages,
    )
    assert first[2] == 1
    assert second[2] == 0
    assert first[0][0]["text"] == second[0][0]["text"] == "shared body"
    assert calls == [["https://example.com/jobs"], []]

    cache.clear()
    third = fetch_recovery_jobs(
        5,
        [job],
        fetcher=lambda *_args: "",
        total_concurrency=1,
        per_host_concurrency=1,
        progress_label="next batch",
        recovery_cache=cache,
        fetch_pages=fetch_pages,
    )
    assert third[2] == 1
    assert third[0][0]["text"] == "shared body"
    assert calls[-1] == ["https://example.com/jobs"]


def test_gamedevmap_recovery_cache_scope_default_and_compatibility_fallback() -> None:
    assert DEFAULT_DISCOVERY_CONFIG["gamedevmap"]["activeAuditRecoveryCacheScope"] == "batch"
    assert dry_run._gamedevmap_recovery_cache_scope({}) == "run"
    assert (
        dry_run._gamedevmap_recovery_cache_scope({"activeAuditRecoveryCacheScope": "batch"})
        == "batch"
    )
    assert (
        dry_run._gamedevmap_recovery_cache_scope({"activeAuditRecoveryCacheScope": "other"})
        == "run"
    )


def _is_volatile_measurement(key: str) -> bool:
    """Exclude operational measurements, not business outcomes.

    Timing and artifact-size measurements vary between otherwise identical runs.
    ``phaseRuntime`` records runtime/lane measurements; phase/lane shapes remain
    asserted independently below. ``pageFetchLifecycle`` contains run-specific
    batch identities and physical recovery workload, which naturally differ
    across cache scopes and are not business outcomes. Each run's receipts are
    validated independently rather than compared.
    """
    return key in {
        "artifactSizeBytes",
        "discoveredAt",
        "lastProbedAt",
        "pageFetchLifecycle",
        "phaseRuntime",
    } or key.endswith(("DurationMs", "ElapsedMs", "Ms"))


def _without_volatile_timestamps(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_volatile_timestamps(item)
            for key, item in value.items()
            if not _is_volatile_measurement(key)
        }
    if isinstance(value, list):
        # Sorted, not just stripped: discovery emits candidates in probe
        # completion order, which is not deterministic, so comparing the lists
        # positionally asserted an ordering the cache scope never promised.
        return sorted(
            (_without_volatile_timestamps(item) for item in value),
            key=lambda item: json.dumps(item, sort_keys=True, default=str),
        )
    return value


def test_batch_scoped_recovery_cache_preserves_fixture_outcomes() -> None:
    payloads = gamedevmap_payloads()
    with workspace_tmpdir("gamedevmap-recovery-cache-parity") as root:
        run_output = root / "run.json"
        batch_output = root / "batch.json"
        run_config = gamedevmap_config(activeAuditRecoveryEscalationEnabled=False)
        batch_config = gamedevmap_config(
            activeAuditRecoveryEscalationEnabled=False,
            activeAuditRecoveryCacheScope="batch",
        )
        common: dict[str, Any] = {
            "timeout_s": 5,
            "batch_size": 2,
            "max_batches": 0,
            "reset": True,
        }
        run_result = sd.run_gamedevmap_active_source_dry_run(
            config=run_config,
            fetcher=gamedevmap_fetcher(payloads),
            output_path=run_output,
            **common,
        )
        batch_result = sd.run_gamedevmap_active_source_dry_run(
            config=batch_config,
            fetcher=gamedevmap_fetcher(payloads),
            output_path=batch_output,
            **common,
        )

    for key in (
        "summary",
        "activeCandidates",
        "zeroJobCandidates",
        "rejectedForActivation",
        "failures",
    ):
        assert _without_volatile_timestamps(batch_result[key]) == _without_volatile_timestamps(
            run_result[key]
        )

    # The receipt's numbers are excluded above; which phases produced them is not a measurement, so
    # the two scopes are required to have run the same phases with the same lane budgets.
    def _receipt_shape(result: dict[str, Any]) -> dict[str, int]:
        rows = result["summary"].get("phaseRuntime", {}).get("phases", [])
        assert rows, "both cache scopes record an in-situ runtime receipt"
        return {str(row["phase"]): int(row["configuredLanes"]) for row in rows}

    assert _receipt_shape(batch_result) == _receipt_shape(run_result)

    for result in (run_result, batch_result):
        assert "pageFetchLifecycle" in result["summary"]
        receipts = result["summary"]["pageFetchLifecycle"]
        assert isinstance(receipts, list)
        batch_ids: set[str] = set()
        total_started = 0
        for receipt in receipts:
            assert receipt["schemaVersion"] == 1
            assert receipt["receiptType"] == "pageFetchLifecycle"
            for key in ("source", "batchId"):
                assert isinstance(receipt[key], str) and receipt[key].strip()
            assert receipt["batchId"] not in batch_ids
            batch_ids.add(receipt["batchId"])
            counts = [
                receipt[key]
                for key in (
                    "submitted",
                    "started",
                    "finished",
                    "failed",
                    "cancelledBeforeStart",
                    "queuedNow",
                    "outstandingNow",
                )
            ]
            assert all(isinstance(value, int) and value >= 0 for value in counts)
            submitted, started, finished, failed, cancelled, queued, outstanding = counts
            assert submitted == started + cancelled + queued
            assert started == finished + outstanding
            assert failed <= finished
            assert queued == outstanding == 0
            assert receipt["deadlineExit"] == "completed"
            total_started += started
        assert total_started > 0  # Zero-request batches are valid; each run must do real work.


def test_cache_parity_comparator_excludes_only_telemetry() -> None:
    results: list[dict[str, Any]] = [
        {
            "summary": {
                "activeCandidateCount": 1,
                "pageFetchLifecycle": [{"batchId": batch_id, "started": started}],
            },
            "activeCandidates": [{"name": "Fixture Studio"}],
        }
        for batch_id, started in (("run", 1), ("batch", 2))
    ]
    assert _without_volatile_timestamps(results[0]) == _without_volatile_timestamps(results[1])
    results[1]["summary"]["activeCandidateCount"] = 2
    assert _without_volatile_timestamps(results[0]) != _without_volatile_timestamps(results[1])
    results[1]["summary"]["activeCandidateCount"] = 1
    results[1]["activeCandidates"][0]["name"] = "Changed Studio"
    assert _without_volatile_timestamps(results[0]) != _without_volatile_timestamps(results[1])
