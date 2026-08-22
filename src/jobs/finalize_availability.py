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
from typing import Any

from src.jobs.availability_schedule import build_availability_sweep_plan
from src.jobs.feed_reconciliation_lock import jobs_feed_reconciliation_lock
from src.jobs.models import CanonicalJob
from src.jobs.pipeline_run_setup import canonicalize_existing_output_row
from src.jobs.state_lifecycle import (
    build_availability_history_payload,
    lifecycle_state_fingerprint,
)
from src.jobs.text_utils import clean_text
from src.pipeline_io import read_existing_output
from src.shared.json_io import existing_json_candidate


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
    @wraps(func)
    def wrapped(*args, **kwargs):
        from src.jobs import pipeline_finalize as _pf

        paths = kwargs.get("paths")
        if paths is None or not hasattr(paths, "output_dir"):
            raise TypeError("finalize_pipeline_run requires paths with an output_dir")
        with jobs_feed_reconciliation_lock(paths.output_dir):
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
                elif (
                    fingerprint_at_setup is not None and current_fingerprint == fingerprint_at_setup
                ):
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
