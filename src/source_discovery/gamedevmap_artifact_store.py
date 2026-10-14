"""GameDevMap active-audit artifact store and freshness helpers.

AI boundary owns: artifact read/init/summary, browser-recovery artifact merge, artifact signature and freshness rules.
AI boundary implement in: this file for artifact persistence and freshness; audit pipeline stays in gamedevmap_active_dry_run.
AI boundary search before contracts: artifact schema keys, active_audit_runtime, and GameDevMap active audit tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_gamedevmap_active_dry_run.py -q`.
"""

from __future__ import annotations

from datetime import (
    UTC,
    datetime,
    timedelta,
)
from pathlib import Path
from typing import Any

from src import source_registry as source_registry_module
from src.shared.json_shapes import as_json_list as _shared_as_list
from src.shared.json_shapes import as_json_object as _shared_as_dict
from src.shared.utils import int_or_default as _shared_int_or_default
from src.shared.utils import now_iso
from src.source_registry import unique_sources

from . import (
    active_audit_runtime,
    audit_ledger,
)
from . import (
    browser_recovery as browser_recovery_helpers,
)
from .gamedevmap import (
    GAMEDEVMAP_CSV_URL,
    GAMEDEVMAP_INDEX_URL,
    _gamedevmap_cache_signature,
)
from .gamedevmap_rejection import (
    _gamedevmap_probe_failed_rejection,
    _gamedevmap_zero_jobs_rejection,
    _normalize_failure_bucket,
)
from .phase_runtime_receipt import (
    PHASE_RECEIPT_SCHEMA_VERSION as PHASE_RECEIPT_SCHEMA_VERSION,
)
from .probe_runtime import (
    ProbeBatchLifecycle as ProbeBatchLifecycle,
)
from .probe_runtime import (
    candidate_id as probe_candidate_id,
)
from .probe_runtime import (
    classify_probe_results,
)

DRY_RUN_SCHEMA_VERSION = 3

# The probe-wave receipt published for readers that must not open the artifact: a full run's
# artifact runs to tens of MB, so the counters an operator watches live in their own small file
# beside it. `summary.probeLifecycle` stays the authoritative copy inside the artifact.
PROBE_LIFECYCLE_RECEIPT_FILENAME = "gamedevmap-audit-receipt.json"
PROBE_LIFECYCLE_RECEIPT_SCHEMA_VERSION = 1


FAILURE_SAMPLE_LIMIT = 200


def gamedevmap_active_dry_run_path() -> Path:
    return source_registry_module.ACTIVE_PATH.parent / "gamedevmap-active-source-dry-run.json"


def _as_list(value: Any) -> list[Any]:
    return list(_shared_as_list(value))


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(_shared_as_dict(value))


def _safe_int(value: Any, default: int = 0) -> int:
    if int(default) == 0:
        return _shared_int_or_default(value)
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return int(default)


def _initial_artifact(
    *,
    run_id: str,
    started_at: str,
    timeout_s: int,
    csv_url: str,
    index_url: str,
    cfg: dict[str, Any],
    batch_size: int,
    fetch_concurrency: int,
    per_host_concurrency: int,
    homepage_fetch_concurrency: int,
    recovery_fetch_concurrency: int,
    recovery_per_host_concurrency: int,
    recovery_timeout_s: int,
) -> dict[str, Any]:
    return active_audit_runtime.create_active_audit_artifact(
        schema_version=DRY_RUN_SCHEMA_VERSION,
        run_id=run_id,
        started_at=started_at,
        mode="gamedevmap_active_source_dry_run",
        progress={
            "complete": False,
            "cursorPosition": 0,
            "batchSize": int(batch_size),
            "batchesCompleted": 0,
            "completedUrlsCount": 0,
        },
        runtime={
            "timeoutSeconds": int(timeout_s),
            "fetchConcurrency": int(fetch_concurrency),
            "perHostConcurrency": int(per_host_concurrency),
            "homepageFetchConcurrency": int(homepage_fetch_concurrency),
            "recoveryFetchConcurrency": int(recovery_fetch_concurrency),
            "recoveryPerHostConcurrency": int(recovery_per_host_concurrency),
            "recoveryTimeoutSeconds": int(recovery_timeout_s),
            "csvUrl": csv_url,
            "indexUrl": index_url,
            "configSignature": _gamedevmap_cache_signature(cfg),
        },
        list_keys=[
            "failureSamples",
            "completedUrls",
            "activeCandidates",
            "zeroJobCandidates",
            "rejectedForActivation",
            "browserRecoveryCandidates",
            "failures",
            "allCandidates",
        ],
        dict_keys=["failureCounts", "failureErrorCounts"],
    )


def _load_or_initialize_artifact(
    output_path: Path,
    *,
    reset: bool,
    run_id: str,
    started_at: str,
    timeout_s: int,
    csv_url: str,
    index_url: str,
    cfg: dict[str, Any],
    batch_size: int,
    fetch_concurrency: int,
    per_host_concurrency: int,
    homepage_fetch_concurrency: int,
    recovery_fetch_concurrency: int,
    recovery_per_host_concurrency: int,
    recovery_timeout_s: int,
) -> dict[str, Any]:
    return active_audit_runtime.load_or_initialize_active_audit_artifact(
        output_path,
        reset=reset,
        schema_version=DRY_RUN_SCHEMA_VERSION,
        initial_artifact=_initial_artifact(
            run_id=run_id,
            started_at=started_at,
            timeout_s=timeout_s,
            csv_url=csv_url,
            index_url=index_url,
            cfg=cfg,
            batch_size=batch_size,
            fetch_concurrency=fetch_concurrency,
            per_host_concurrency=per_host_concurrency,
            homepage_fetch_concurrency=homepage_fetch_concurrency,
            recovery_fetch_concurrency=recovery_fetch_concurrency,
            recovery_per_host_concurrency=recovery_per_host_concurrency,
            recovery_timeout_s=recovery_timeout_s,
        ),
        runtime_updates={
            "timeoutSeconds": int(timeout_s),
            "fetchConcurrency": int(fetch_concurrency),
            "perHostConcurrency": int(per_host_concurrency),
            "homepageFetchConcurrency": int(homepage_fetch_concurrency),
            "recoveryFetchConcurrency": int(recovery_fetch_concurrency),
            "recoveryPerHostConcurrency": int(recovery_per_host_concurrency),
            "recoveryTimeoutSeconds": int(recovery_timeout_s),
            "csvUrl": csv_url,
            "indexUrl": index_url,
            "configSignature": _gamedevmap_cache_signature(cfg),
        },
        progress_updates={
            "batchSize": int(batch_size),
        },
        list_keys=[
            "completedUrls",
            "activeCandidates",
            "zeroJobCandidates",
            "rejectedForActivation",
            "browserRecoveryCandidates",
            "failures",
            "allCandidates",
        ],
        dict_keys=["failureCounts", "failureErrorCounts"],
        failure_sample_limit=FAILURE_SAMPLE_LIMIT,
        load_json_object=source_registry_module.load_json_object,
    )


def _probe_lifecycle_rollup(lifecycles: list[Any]) -> dict[str, Any]:
    """One receipt for a run's probe waves: the totals, and the worst retirement any batch reported.

    Read from the *live* receipts at artifact-write time rather than from the per-batch snapshots,
    because the interesting half of the answer arrives late: a batch whose per-call bound abandoned a
    fetch leaves its shared client open past its own exit, and that fetch closes it when it finally
    returns. So a batch here that reports `clientClosed: false` still has a fetch inside its client,
    and `clientsUnclosed` counts exactly the waves whose close never happened.
    """
    receipts = [lifecycle.snapshot() for lifecycle in lifecycles]
    if not receipts:
        return {}
    return {
        "batches": len(receipts),
        "fetchesStarted": sum(int(row.get("fetchesStarted") or 0) for row in receipts),
        "fetchesBoundedOut": sum(int(row.get("fetchesBoundedOut") or 0) for row in receipts),
        "fetchesOutstandingAtExit": sum(
            int(row.get("fetchesOutstandingAtExit") or 0) for row in receipts
        ),
        "maxFetchesOutstandingAtExit": max(
            int(row.get("fetchesOutstandingAtExit") or 0) for row in receipts
        ),
        "clientFetchesOutstandingAtExit": sum(
            int(row.get("clientFetchesOutstandingAtExit") or 0) for row in receipts
        ),
        "clientsRetiredByBatch": sum(
            1 for row in receipts if row.get("clientRetiredBy") == "batch"
        ),
        "clientsRetiredByLastFetch": sum(
            1 for row in receipts if row.get("clientRetiredBy") == "last-fetch"
        ),
        "clientsUnclosed": sum(1 for row in receipts if not row.get("clientClosed")),
        "maxClientCloseDeferredMs": max(
            int(row.get("clientCloseDeferredMs") or 0) for row in receipts
        ),
        "poolThreadsAliveNow": max(int(row.get("poolThreadsAliveNow") or 0) for row in receipts),
        "deadlineExits": sum(1 for row in receipts if row.get("deadlineExit")),
    }


def probe_lifecycle_receipt_path(artifact_path: Path | None = None) -> Path:
    """The receipt file that sits beside an artifact (default: the configured one)."""
    base = artifact_path if artifact_path is not None else gamedevmap_active_dry_run_path()
    return Path(base).parent / PROBE_LIFECYCLE_RECEIPT_FILENAME


def write_probe_lifecycle_receipt(
    *,
    artifact_path: Path,
    artifact: dict[str, Any],
    lifecycles: list[ProbeBatchLifecycle] | None,
) -> dict[str, Any]:
    """Publish this run's probe-wave receipt next to the artifact, small enough to be polled.

    The artifact carries the same rollup under `summary.probeLifecycle`, but it is tens of MB on a
    real walk, so a reader that wants these counters cannot parse it on a poll. This file is the
    bridge's source for the Ops view: a run that reported no probe wave writes nothing (the artifact
    keeps the same shape it had before receipts existed). Returns the payload written, or `{}`.
    """
    receipt = _probe_lifecycle_rollup(list(lifecycles or []))
    if not receipt:
        return {}
    payload = {
        "schemaVersion": PROBE_LIFECYCLE_RECEIPT_SCHEMA_VERSION,
        "runId": str(artifact.get("runId") or ""),
        "startedAt": str(artifact.get("startedAt") or ""),
        "finishedAt": str(artifact.get("finishedAt") or ""),
        "sourceArtifact": Path(artifact_path).name,
        "probeLifecycle": receipt,
    }
    audit_ledger.save_artifact_atomic(payload, probe_lifecycle_receipt_path(artifact_path))
    return payload


def _phase_runtime_rollup(receipts: list[Any]) -> dict[str, Any]:
    """A run's per-phase CPU and lane receipt, aggregated by phase name.

    This is the number a wall-clock reading cannot give: two runs of the same tree differ by ~8% on a
    phase's wall, while the CPU the phase spent and the share of its lane budget its fetches occupied
    are properties of the work. Aggregate is by phase, because a phase runs once per batch and the
    interesting comparison is against the same phase in another run.

    `meanInFlight` is weighted by each occurrence's wall (`Σ laneBusyMs / Σ wallMs`), so a long batch
    counts for more than a short one; `configuredLanes` and `peakInFlight` are the maxima over the
    phase's occurrences, which is the honest reading of a budget that did not change mid-run.
    """
    rows = [receipt.snapshot() for receipt in receipts]
    rows = [row for row in rows if str(row.get("phase") or "").strip()]
    if not rows:
        return {}
    by_phase: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_phase.setdefault(str(row.get("phase")), []).append(row)
    phases: list[dict[str, Any]] = []
    for phase, occurrences in by_phase.items():
        wall_ms = sum(int(row.get("wallMs") or 0) for row in occurrences)
        cpu_ms = sum(int(row.get("cpuMs") or 0) for row in occurrences)
        items = sum(int(row.get("items") or 0) for row in occurrences)
        lane_busy_ms = sum(int(row.get("laneBusyMs") or 0) for row in occurrences)
        configured_lanes = max(int(row.get("configuredLanes") or 0) for row in occurrences)
        mean_in_flight = (lane_busy_ms / wall_ms) if wall_ms > 0 else 0.0
        phases.append(
            {
                "phase": phase,
                "occurrences": len(occurrences),
                "wallMs": wall_ms,
                "cpuMs": cpu_ms,
                "cpuOverWallPct": round(cpu_ms / wall_ms * 100, 2) if wall_ms else 0.0,
                "items": items,
                "wallPerItemMs": round(wall_ms / items, 3) if items else 0.0,
                "cpuPerItemMs": round(cpu_ms / items, 3) if items else 0.0,
                "configuredLanes": configured_lanes,
                "peakInFlight": max(int(row.get("peakInFlight") or 0) for row in occurrences),
                "meanInFlight": round(mean_in_flight, 3),
                "laneOccupancyPct": (
                    round(mean_in_flight / configured_lanes * 100, 2) if configured_lanes else 0.0
                ),
                "laneBusyMs": lane_busy_ms,
                "inFlightAtFinish": max(
                    int(row.get("inFlightAtFinish") or 0) for row in occurrences
                ),
            }
        )
    # Sorted by CPU: the phase order in the artifact answers "where did the run's work go".
    phases.sort(key=lambda row: (-int(row["cpuMs"]), str(row["phase"])))
    total_wall_ms = sum(int(row["wallMs"]) for row in phases)
    total_cpu_ms = sum(int(row["cpuMs"]) for row in phases)
    return {
        "schemaVersion": PHASE_RECEIPT_SCHEMA_VERSION,
        "phases": phases,
        "phaseCount": len(phases),
        "occurrences": len(rows),
        "wallMs": total_wall_ms,
        "cpuMs": total_cpu_ms,
        "cpuOverWallPct": round(total_cpu_ms / total_wall_ms * 100, 2) if total_wall_ms else 0.0,
    }


def _summarize_artifact(
    artifact: dict[str, Any],
    *,
    parsed_rows: list[dict[str, str]],
    representative_rows: list[dict[str, Any]],
    completed_urls: set[str],
    probe_lifecycles: list[ProbeBatchLifecycle] | None = None,
    phase_receipts: list[Any] | None = None,
    page_fetch_lifecycles: list[Any] | None = None,
) -> None:
    prior_summary = _as_dict(artifact.get("summary"))
    counts = active_audit_runtime.active_audit_artifact_counts(
        artifact,
        all_candidates_key="allCandidates",
        active_candidates_key="activeCandidates",
        zero_candidates_key="zeroJobCandidates",
        rejected_key="rejectedForActivation",
        browser_candidates_key="browserRecoveryCandidates",
        recovered_predicate=lambda row: bool(row.get("gamedevmapRecovery")),
        failure_bucket_fn=lambda row: str(
            row.get("failureBucket") or _normalize_failure_bucket(row.get("reason", ""))
        ),
    )
    csv_rows = len(parsed_rows) if parsed_rows else _safe_int(prior_summary.get("csvRows"))
    eligible_rows = (
        len(representative_rows)
        if representative_rows
        else _safe_int(prior_summary.get("eligibleRows"))
    )
    completed_count = (
        len(completed_urls) if completed_urls else _safe_int(prior_summary.get("completedUrls"))
    )
    artifact["summary"] = {
        "csvRows": csv_rows,
        "eligibleRows": eligible_rows,
        "completedUrls": completed_count,
        "remainingUrls": max(0, eligible_rows - completed_count),
        "homepageFetchAttempts": _safe_int(
            _as_dict(artifact.get("summary")).get("homepageFetchAttempts")
        ),
        "homepagesFetched": _safe_int(_as_dict(artifact.get("summary")).get("homepagesFetched")),
        "recoveryFetchAttempts": _safe_int(
            _as_dict(artifact.get("summary")).get("recoveryFetchAttempts")
        ),
        "recoveryUniqueFetchAttempts": _safe_int(
            _as_dict(artifact.get("summary")).get("recoveryUniqueFetchAttempts")
        ),
        "recoveryNetworkFetchAttempts": _safe_int(
            _as_dict(artifact.get("summary")).get("recoveryNetworkFetchAttempts")
        ),
        "recoveryPagesFetched": _safe_int(
            _as_dict(artifact.get("summary")).get("recoveryPagesFetched")
        ),
        "providerCandidates": len(
            [row for row in counts.all_candidates if str(row.get("adapter") or "") != "static"]
        ),
        "staticCandidates": len(
            [row for row in counts.all_candidates if str(row.get("adapter") or "") == "static"]
        ),
        "recoveredCandidates": len(counts.recovered_candidates),
        "recoveredActiveCandidates": len(counts.recovered_active),
        "probedCandidates": len(counts.all_candidates),
        "activeCandidates": len(counts.active_rows),
        "zeroJobCandidates": counts.zero_job_count,
        "probeFailures": int(counts.reason_counts.get("probe_failed") or 0),
        "technicalFailures": len(counts.technical_failures),
        "coverageMisses": len(counts.coverage_misses),
        "failures": counts.failure_count,
        "failureSampleCount": counts.failure_sample_count,
        "artifactSizeBytes": _safe_int(_as_dict(artifact.get("runtime")).get("artifactSizeBytes")),
        "rejectedForActivation": len(counts.rejected_rows),
        "rejectedReasonCounts": counts.reason_counts,
        "rejectedReasonDetailCounts": counts.detail_counts,
        "activeAdapterCounts": counts.active_adapter_counts,
        "browserRecoveryCandidates": counts.browser_recovery_candidate_count,
        "browserRecoveryProcessed": counts.browser_recovery_processed_count,
        "browserRecoveredActiveCandidates": counts.browser_recovered_active_count,
        "lostRecoveredActiveCandidates": counts.lost_recovered_active_count,
    }
    # Only a run that reported receipts gets the sections, so every other artifact keeps the summary
    # shape it already had. A re-summarize that passes no receipts (browser recovery rewrites the
    # artifact after the probe wave has run) keeps the receipt already published on it rather than
    # erasing the run's answer, which is also what the sidecar file beside the artifact projects.
    probe_lifecycle = _probe_lifecycle_rollup(list(probe_lifecycles or []))
    if not probe_lifecycle:
        prior_receipt = prior_summary.get("probeLifecycle")
        probe_lifecycle = dict(prior_receipt) if isinstance(prior_receipt, dict) else {}
    if probe_lifecycle:
        artifact["summary"]["probeLifecycle"] = probe_lifecycle
    phase_runtime = _phase_runtime_rollup(list(phase_receipts or []))
    if phase_runtime:
        artifact["summary"]["phaseRuntime"] = phase_runtime
    if page_fetch_lifecycles:
        artifact["summary"]["pageFetchLifecycle"] = [
            lifecycle.snapshot() for lifecycle in page_fetch_lifecycles
        ]
    else:
        prior_page_fetch_receipt = prior_summary.get("pageFetchLifecycle")
        if isinstance(prior_page_fetch_receipt, list) and prior_page_fetch_receipt:
            artifact["summary"]["pageFetchLifecycle"] = list(prior_page_fetch_receipt)


def _load_browser_recovery_artifact(
    *,
    output_path: Path,
    cfg: dict[str, Any],
    timeout_s: int,
    run_id: str,
    started_at: str,
) -> dict[str, Any]:
    artifact = source_registry_module.load_json_object(output_path, {})
    if isinstance(artifact, dict) and artifact:
        return artifact
    return _initial_artifact(
        run_id=run_id,
        started_at=started_at or now_iso(),
        timeout_s=timeout_s,
        csv_url=str(cfg.get("csvUrl") or GAMEDEVMAP_CSV_URL),
        index_url=str(cfg.get("indexUrl") or GAMEDEVMAP_INDEX_URL),
        cfg=cfg,
        batch_size=max(1, int(cfg.get("activeAuditBatchSize") or 1000)),
        fetch_concurrency=max(1, int(cfg.get("fetchConcurrency") or 24)),
        per_host_concurrency=max(1, int(cfg.get("perHostConcurrency") or 3)),
        homepage_fetch_concurrency=max(
            1, int(cfg.get("activeAuditHomepageFetchConcurrency") or 32)
        ),
        recovery_fetch_concurrency=max(
            1, int(cfg.get("activeAuditRecoveryFetchConcurrency") or 72)
        ),
        recovery_per_host_concurrency=max(
            1, int(cfg.get("activeAuditRecoveryPerHostConcurrency") or 4)
        ),
        recovery_timeout_s=max(
            1, min(int(timeout_s), int(cfg.get("activeAuditRecoveryTimeoutSeconds") or 5))
        ),
    )


def _mark_browser_recovery_probe_results(
    probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
    *,
    rendered_count: int,
) -> None:
    for index, (candidate, ok, jobs_found, _error, _duration_ms) in enumerate(probe_results):
        if ok and jobs_found > 0:
            candidate["gamedevmapBrowserRecovery"] = True
        if index < rendered_count:
            candidate["probeStatus"] = "ok"
            candidate["candidateState"] = "validated"


def _merge_browser_recovery_artifact_updates(
    *,
    artifact: dict[str, Any],
    batch: browser_recovery_helpers.BrowserRecoveryBatch,
    combined_probe_results: list[tuple[dict[str, Any], bool, int, str, int]],
) -> None:
    artifact["allCandidates"] = active_audit_runtime.merge_unique_candidate_rows(
        artifact.get("allCandidates"),
        batch.analysis.all_candidates,
        unique_rows=unique_sources,
    )
    active_audit_runtime.append_artifact_rows(
        artifact,
        "rejectedForActivation",
        list(batch.analysis.rejected_rows or []),
    )
    active_audit_runtime.apply_active_audit_probe_results(
        artifact,
        combined_probe_results,
        classify_probe_results=classify_probe_results,
        probe_failed_rejection=_gamedevmap_probe_failed_rejection,
        zero_jobs_rejection=_gamedevmap_zero_jobs_rejection,
        active_key="activeCandidates",
        zero_candidates_key="zeroJobCandidates",
        rejected_key="rejectedForActivation",
        identity_fn=probe_candidate_id,
    )


def _gamedevmap_artifact_signature_matches(
    artifact: dict[str, Any],
    *,
    expected_signature: dict[str, Any],
) -> bool:
    if int(artifact.get("schemaVersion") or 0) != int(DRY_RUN_SCHEMA_VERSION):
        return False
    existing = _as_dict(_as_dict(artifact.get("runtime")).get("configSignature"))
    if existing == expected_signature:
        return True
    existing_without_chunking = dict(existing)
    expected_without_chunking = dict(expected_signature)
    existing_without_chunking.pop("activeAuditBatchSize", None)
    expected_without_chunking.pop("activeAuditBatchSize", None)
    return existing_without_chunking == expected_without_chunking


def _gamedevmap_artifact_is_fresh(
    artifact: dict[str, Any],
    *,
    expected_signature: dict[str, Any],
    ttl_minutes: int,
) -> bool:
    if not bool(_as_dict(artifact.get("progress")).get("complete")):
        return False
    if not _gamedevmap_artifact_signature_matches(
        artifact,
        expected_signature=expected_signature,
    ):
        return False
    if ttl_minutes <= 0:
        return False
    updated_at = audit_ledger.parse_artifact_time(
        artifact.get("updatedAt") or artifact.get("finishedAt")
    )
    return bool(updated_at and datetime.now(UTC) - updated_at <= timedelta(minutes=ttl_minutes))
