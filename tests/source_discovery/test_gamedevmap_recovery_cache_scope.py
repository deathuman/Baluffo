from __future__ import annotations

from typing import Any

from src.source_discovery import gamedevmap_active_dry_run as dry_run
from src.source_discovery.directory_page_recovery import fetch_recovery_jobs

from ._helpers import sd, workspace_tmpdir
from .gamedevmap_test_helpers import (
    gamedevmap_config,
    gamedevmap_fetcher,
    gamedevmap_payloads,
)


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


def test_gamedevmap_recovery_cache_scope_defaults_to_run() -> None:
    assert dry_run._gamedevmap_recovery_cache_scope({}) == "run"
    assert (
        dry_run._gamedevmap_recovery_cache_scope({"activeAuditRecoveryCacheScope": "batch"})
        == "batch"
    )
    assert (
        dry_run._gamedevmap_recovery_cache_scope({"activeAuditRecoveryCacheScope": "other"})
        == "run"
    )


def _without_volatile_timestamps(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_volatile_timestamps(item)
            for key, item in value.items()
            if key not in {"artifactSizeBytes", "discoveredAt", "lastProbedAt"}
        }
    if isinstance(value, list):
        return [_without_volatile_timestamps(item) for item in value]
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
