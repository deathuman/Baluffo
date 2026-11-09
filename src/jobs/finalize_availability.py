"""Availability artifacts, live-row merging, and feed reconciliation for finalization.

AI boundary owns: availability identity/tombstone artifact writes, concurrent live-row
merging, and the jobs feed reconciliation serialization guard.
AI boundary implement in: this file for availability/feed finalization; output writing and
the finalization conductor stay in sibling finalize_* leaves and ``pipeline_finalize.py``.
AI boundary search before contracts: availability identity contracts, feed reconciliation,
and pipeline finalization tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline finalization tests.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from typing import Any

from src.bridge.fetch_report_summary import load_fetch_report_summary_artifact
from src.jobs.availability_schedule import build_availability_sweep_plan
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_run_setup import canonicalize_existing_output_row
from src.jobs.state_lifecycle import (
    build_availability_history_payload,
    lifecycle_state_fingerprint,
)
from src.jobs.text_utils import clean_text
from src.pipeline_io import read_existing_output
from src.shared.json_io import existing_json_candidate


def _previous_availability_health(paths: Any) -> dict[str, Any] | None:
    """Previous terminal run's availabilityHealth payload, for run-over-run deltas.

    Reads the dedicated terminal-only baseline artifact when it exists — it is
    written only next to the terminal report, so mid-run progress overwrites of
    the summary artifact can never poison it. Falls back to the compact
    fetch-report summary artifact for pre-baseline continuity. Failed runs never
    observed the lifecycle, so their payload carries no ``overdueCount`` — that
    is no baseline at all. Any read/shape failure returns ``None`` so the
    health verdict simply skips the run-over-run signals instead of blocking
    finalization.
    """

    baseline_path = getattr(paths, "availability_health_baseline_path", None)
    if baseline_path is not None:
        try:
            baseline = json.loads(Path(baseline_path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            baseline = None
        if isinstance(baseline, dict) and _is_terminal_health_baseline(baseline):
            return dict(baseline)
    try:
        summary = load_fetch_report_summary_artifact(paths.report_path)
    except (OSError, ValueError):
        return None
    health = summary.get("availabilityHealth")
    if not isinstance(health, dict):
        return None
    if not _is_terminal_health_baseline(health):
        return None
    return dict(health)


def _is_terminal_health_baseline(health: dict[str, Any]) -> bool:
    """True only for payloads a terminal finalize could have produced.

    Failed runs never observed the lifecycle, so their payload carries no
    ``overdueCount`` — that is no baseline at all. Mid-run progress overwrites
    of the summary artifact carry a normalizer-default availabilityHealth
    (empty status, overdueCount=0), which would read as a real "0 overdue"
    baseline and fake a full overdue rise on the next terminal run. The builder
    emits exactly "healthy" or "degraded"; progress overwrites carry
    "running"-class statuses, failed runs "failed", defaults "" — none is a
    baseline.
    """

    if not isinstance(health, dict):
        return False
    raw = health.get("overdueCount")
    if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
        return False
    return str(health.get("status") or "").strip() in {"healthy", "degraded"}


def write_availability_health_baseline(
    paths: Any, health: dict[str, Any], *, finished_at: str
) -> bool:
    """Persist this terminal run's health payload as the next run's baseline.

    Called only from the terminal finalize path after the report payload is
    complete — mid-run progress writers never touch this artifact, closing the
    baseline-poisoning channel for good. Returns whether bytes changed.
    """

    payload = dict(health)
    payload["capturedAt"] = clean_text(finished_at) or None
    from src.jobs import pipeline_finalize as _pf

    return _pf.write_atomic_if_changed(
        Path(paths.availability_health_baseline_path),
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
    )


def _write_availability_artifacts(
    *, paths: Any, lifecycle_rows: dict[str, dict[str, Any]], finished_at: str
) -> dict[str, Any]:
    from src.jobs import pipeline_finalize as _pf

    history = build_availability_history_payload(lifecycle_rows, finished_at=finished_at)
    wrote_history = _pf.write_atomic_if_changed(
        paths.availability_history_path,
        json.dumps(history, ensure_ascii=False, separators=(",", ":")),
    )
    priority_manifest: dict[str, Any] = {}
    priority_path = paths.output_dir / "jobs-availability-priority.json"
    if priority_path.exists():
        try:
            loaded = json.loads(priority_path.read_text(encoding="utf-8"))
            priority_manifest = loaded if isinstance(loaded, dict) else {}
        except (OSError, json.JSONDecodeError):
            pass
    shadow_payload: dict[str, Any] = {}
    shadow_path = paths.output_dir / "jobs-availability-shadow-results.json"
    if shadow_path.exists():
        try:
            loaded = json.loads(shadow_path.read_text(encoding="utf-8"))
            shadow_payload = loaded if isinstance(loaded, dict) else {}
        except (OSError, json.JSONDecodeError):
            pass
    direct_checkpoints: dict[str, Any] = {}
    checkpoint_path = paths.output_dir / "jobs-availability-direct-checkpoints.json"
    if checkpoint_path.exists():
        try:
            loaded = json.loads(checkpoint_path.read_text(encoding="utf-8"))
            direct_checkpoints = loaded if isinstance(loaded, dict) else {}
        except (OSError, json.JSONDecodeError):
            pass
    sweep = build_availability_sweep_plan(
        lifecycle_rows,
        priority_manifest,
        finished_at=finished_at,
        direct_checkpoints=direct_checkpoints,
    )
    wrote_sweep = _pf.write_atomic_if_changed(
        paths.availability_sweep_plan_path,
        json.dumps(sweep, ensure_ascii=False, separators=(",", ":")),
    )
    shadow_rows = [row for row in shadow_payload.get("rows") or [] if isinstance(row, dict)]
    counts = dict(Counter(clean_text(row.get("kind")) or "unknown" for row in shadow_rows))
    lifecycle_by_id = {
        clean_text(entry.get("availabilityId")): entry
        for entry in lifecycle_rows.values()
        if clean_text(entry.get("availabilityId"))
    }
    conflicts = []
    for row in shadow_rows[-200:]:
        availability_id = clean_text(row.get("availabilityId"))
        source_status = clean_text(
            (lifecycle_by_id.get(availability_id) or {}).get("availabilityStatus") or "available"
        )
        direct_kind = clean_text(row.get("kind"))
        if (direct_kind, source_status) in {
            ("direct_live", "unavailable"),
            ("direct_closed", "available"),
        }:
            conflicts.append(
                {
                    "availabilityId": availability_id,
                    "sourceStatus": source_status,
                    "directKind": direct_kind,
                    "checkedAt": clean_text(row.get("checkedAt")),
                }
            )
    return {
        "sweep": sweep,
        "conflicts": conflicts,
        "shadowCounts": counts,
        "previousAvailabilityHealth": _previous_availability_health(paths),
        "wroteHistory": wrote_history,
        "wroteSweep": wrote_sweep,
    }


def _merge_concurrent_direct_live_rows(
    canonical_rows: list[CanonicalJob],
    current_rows: list[Any],
    lifecycle_rows: dict[str, dict[str, Any]],
) -> list[CanonicalJob]:
    merged = list(canonical_rows)
    known_availability_ids = {
        clean_text(row.availabilityId) for row in merged if clean_text(row.availabilityId)
    }
    lifecycle_by_id = {
        clean_text(entry.get("availabilityId")): entry
        for entry in lifecycle_rows.values()
        if isinstance(entry, dict) and clean_text(entry.get("availabilityId"))
    }
    for row in current_rows:
        availability_id = clean_text(
            row.availabilityId if isinstance(row, CanonicalJob) else row.get("availabilityId")
        )
        entry = lifecycle_by_id.get(availability_id) or {}
        evidence = entry.get("availabilityEvidence")
        direct_live = (
            clean_text(entry.get("availabilityStatus")) == "available"
            and isinstance(evidence, dict)
            and clean_text(evidence.get("kind")) == "direct_live"
            and clean_text(evidence.get("confidence")) == "definitive"
        )
        if availability_id and availability_id not in known_availability_ids and direct_live:
            if isinstance(row, CanonicalJob):
                merged.append(row)
            else:
                merged.append(CanonicalJob.from_mapping(row))
            known_availability_ids.add(availability_id)
    return merged


def _merge_latest_lifecycle_availability(
    computed: dict[str, dict[str, Any]],
    latest: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Preserve availability evidence written while finalize was computing.

    Finalize owns lifecycle status/removal transitions, but direct availability
    checks may update the availability fields during the unlocked compute phase.
    Merge those fields back only when the latest check is newer than the value
    finalize computed; do not resurrect rows finalize intentionally removed.
    """
    merged = dict(computed)
    for key, latest_entry in latest.items():
        current = merged.get(key)
        if not isinstance(current, dict) or not isinstance(latest_entry, dict):
            continue
        latest_checked_at = clean_text(latest_entry.get("availabilityCheckedAt"))
        current_checked_at = clean_text(current.get("availabilityCheckedAt"))
        if not latest_checked_at or latest_checked_at <= current_checked_at:
            continue
        availability_fields = {
            field: value
            for field, value in latest_entry.items()
            if field.startswith("availability")
        }
        if availability_fields:
            merged[key] = {**current, **availability_fields}
    return merged


def _refresh_commit_rows(
    *,
    paths: Any,
    lifecycle_rows: dict[str, dict[str, Any]],
    canonical_rows: list[CanonicalJob],
    payload_rows: list[dict[str, Any]],
    lifecycle_fingerprint_at_setup: tuple[int, int] | None,
) -> tuple[dict[str, dict[str, Any]], list[CanonicalJob], list[dict[str, Any]]]:
    """Refresh the lifecycle and merge direct-live rows at commit time.

    This is the only expensive read that belongs to the authoritative commit;
    the preceding dedupe, lifecycle application, audits, and report shaping run
    without the cross-process reconciliation lock.
    """
    from src.jobs import pipeline_finalize as _pf

    latest_lifecycle = lifecycle_rows
    if existing_json_candidate(paths.lifecycle_state_path) is not None:
        current_fingerprint = lifecycle_state_fingerprint(paths.lifecycle_state_path)
        if (
            lifecycle_fingerprint_at_setup is None
            or current_fingerprint != lifecycle_fingerprint_at_setup
        ):
            latest_lifecycle = _pf.read_job_lifecycle_state(paths.lifecycle_state_path)
    lifecycle_rows = _merge_latest_lifecycle_availability(lifecycle_rows, latest_lifecycle)

    current_rows = read_existing_output(
        paths.json_path,
        _pf.now_iso(),
        canonicalize_job=canonicalize_existing_output_row,
        clean_text=clean_text,
        canonical_job_cls=CanonicalJob,
        row_predicate=_direct_live_row_predicate(lifecycle_rows),
    )
    merged_rows = list(canonical_rows)
    if current_rows:
        merged_rows = _merge_concurrent_direct_live_rows(merged_rows, current_rows, lifecycle_rows)

    payload_availability_ids = {
        clean_text(row.get("availabilityId")) for row in payload_rows if isinstance(row, dict)
    }
    lifecycle_by_id = {
        clean_text(entry.get("availabilityId")): entry
        for entry in lifecycle_rows.values()
        if isinstance(entry, dict) and clean_text(entry.get("availabilityId"))
    }
    for row in merged_rows:
        availability_id = clean_text(getattr(row, "availabilityId", ""))
        if not availability_id or availability_id in payload_availability_ids:
            continue
        payload_rows.append(row.to_dict())
        payload_availability_ids.add(availability_id)

    for payload_row in payload_rows:
        entry = lifecycle_by_id.get(clean_text(payload_row.get("availabilityId")))
        if not isinstance(entry, dict):
            continue
        for field, value in entry.items():
            if field.startswith("availability"):
                payload_row[field] = value
    return lifecycle_rows, merged_rows, payload_rows


def _direct_live_row_predicate(
    lifecycle_rows: dict[str, dict[str, Any]],
) -> Callable[[dict[str, Any]], bool]:
    """Build a filter that only accepts rows matching the direct-live merge test.

    Lets `_serialize_jobs_feed_reconciliation` skip CanonicalJob conversion for
    the 40k+ pre-existing jobs that aren't direct-live — that conversion alone
    was the single biggest memory allocation during finalize.
    """
    lifecycle_by_id = {
        clean_text(entry.get("availabilityId")): entry
        for entry in lifecycle_rows.values()
        if isinstance(entry, dict) and clean_text(entry.get("availabilityId"))
    }

    def _matches(row: dict[str, Any]) -> bool:
        availability_id = clean_text(row.get("availabilityId"))
        if not availability_id:
            return False
        entry = lifecycle_by_id.get(availability_id) or {}
        evidence = entry.get("availabilityEvidence")
        return (
            clean_text(entry.get("availabilityStatus")) == "available"
            and isinstance(evidence, dict)
            and clean_text(evidence.get("kind")) == "direct_live"
            and clean_text(evidence.get("confidence")) == "definitive"
        )

    return _matches


def _serialize_jobs_feed_reconciliation(func):
    """Refresh lifecycle/direct-live rows before finalize computes.

    This used to hold the cross-process reconciliation lock for the whole
    finalize call. It is now a lock-free advisory refresh: the authoritative
    commit re-reads and re-merges under the lock.
    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        from src.jobs import pipeline_finalize as _pf

        paths = kwargs.get("paths")
        if paths is None or not hasattr(paths, "output_dir"):
            raise TypeError("finalize_pipeline_run requires paths with an output_dir")
        if existing_json_candidate(paths.lifecycle_state_path) is not None:
            kwargs = dict(kwargs)
            # ponytail: skip the 300+ MB re-parse of jobs-lifecycle-state.json
            # when the file hasn't changed since setup loaded it. mtime+size
            # is the cheapest change signal that still catches an external
            # writer between setup and finalize.
            fingerprint_at_setup = kwargs.get("lifecycle_state_fingerprint")
            current_fingerprint = lifecycle_state_fingerprint(paths.lifecycle_state_path)
            rows_arg = kwargs.get("lifecycle_rows")
            if rows_arg is None:
                # ponytail: deferred lifecycle tree — setup dropped the parsed
                # rows to keep fetch-window RSS flat, so finalize owns the only
                # parse; reload from the file even when the fingerprint matches.
                latest_lifecycle = _pf.read_job_lifecycle_state(paths.lifecycle_state_path)
            elif fingerprint_at_setup is not None and current_fingerprint == fingerprint_at_setup:
                latest_lifecycle = rows_arg or {}
            else:
                latest_lifecycle = _pf.read_job_lifecycle_state(paths.lifecycle_state_path)
            kwargs["lifecycle_rows"] = latest_lifecycle
            # ponytail: only convert direct-live rows to CanonicalJob; skip
            # the 40k+ pre-existing rows that will be filtered out anyway.
            current_rows = read_existing_output(
                paths.json_path,
                clean_text(kwargs.get("started_at")) or _pf.now_iso(),
                canonicalize_job=canonicalize_existing_output_row,
                clean_text=clean_text,
                canonical_job_cls=CanonicalJob,
                row_predicate=_direct_live_row_predicate(latest_lifecycle),
            )
            kwargs["canonical_rows"] = _merge_concurrent_direct_live_rows(
                list(kwargs.get("canonical_rows") or []), current_rows, latest_lifecycle
            )
        return func(*args, **kwargs)

    return wrapped
