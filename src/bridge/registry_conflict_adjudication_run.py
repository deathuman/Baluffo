"""Registry conflict adjudication — run orchestrator and result overlay.

AI boundary owns: the background adjudication run loop, registry state demotion, summary-cache writes, and adjudication overlay.
AI boundary implement in: this registry_conflict_adjudication_run.py leaf.
AI boundary search before contracts: conflict adjudication routes, progress payloads, and adjudication tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry adjudication tests."""

# ruff: noqa: F822

from __future__ import annotations

import threading
import uuid
from typing import Any

from src.bridge.registry_conflict_adjudication_core import (
    ADJUDICATION_REASON,
    _artifact_path,
    _as_dict,
    _as_list,
    _clean,
    _now_iso,
    _row_id,
)
from src.bridge.registry_conflict_adjudication_decide import (
    _build_family_adjudication,
    _demote_ids,
    _selected_conflicts,
    _summary_from_families,
)
from src.bridge.registry_conflict_adjudication_progress import (
    _AdjudicationProgress,
    _progress_throttle_seconds,
    _task_progress_payload,
)
from src.bridge.registry_conflicts import (
    build_registry_conflicts_summary_cache_key,
    derive_registry_conflict_queue,
    summarize_registry_conflicts_payload,
    write_registry_conflicts_summary_cache,
)

_ADJUDICATION_LOCK = threading.RLock()


def run_registry_conflict_adjudication(
    api: Any, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    data = _as_dict(payload)
    apply_autopilot = bool(data.get("applyAutopilot"))
    timeout_s = max(3, min(20, int(data.get("timeoutSeconds") or 8)))
    throttle_s = _progress_throttle_seconds(data)
    run_id = _clean(data.get("runId")) or f"conflict_check_{uuid.uuid4().hex[:10]}"
    started_at = _clean(data.get("startedAt")) or _now_iso()
    progress = _AdjudicationProgress(
        api,
        data,
        run_id=run_id,
        started_at=started_at,
        throttle_s=throttle_s,
    )
    with _ADJUDICATION_LOCK:
        progress.phase("loading_registry", "Loading registry state")
        state = api.load_state()
        progress.phase("building_queue", "Building conflict queue")
        source_state_path = api.JOBS_FETCH_REPORT_PATH.with_name("jobs-source-state.json")
        source_state_payload = api.load_json_object(source_state_path, {})
        conflict_payload = derive_registry_conflict_queue(state, source_state_payload)
        selected = _selected_conflicts(conflict_payload, data)
        selected_source_ids = {
            _row_id(_as_dict(row))
            for card in selected
            for row in _as_list(card.get("rows"))
            if _row_id(_as_dict(row))
        }
        progress.set_totals(
            total_family_count=len(selected),
            total_source_count=len(selected_source_ids),
        )
        progress.phase("probing_sources", "Checking conflicting sources")
        families: list[dict[str, Any]] = []
        target_ids: set[str] = set()

        for family_index, card in enumerate(selected, start=1):
            family_key = _clean(card.get("familyKey"))

            def _record_progress(
                event: str,
                row: dict[str, Any],
                probe: dict[str, Any] | None = None,
                *,
                current_family_key: str = family_key,
                current_family_index: int = family_index,
            ) -> None:
                if event == "source_started":
                    progress.source_started(
                        row,
                        family_key=current_family_key,
                        family_index=current_family_index,
                    )
                elif event == "source_finished" and probe is not None:
                    progress.source_finished(row, probe)

            family, family_target_ids = _build_family_adjudication(
                card,
                timeout_s=timeout_s,
                apply_autopilot=apply_autopilot,
                progress_callback=_record_progress,
            )
            progress.family_finished(family_key, family_index)
            if not family:
                continue
            families.append(family)
            target_ids.update(family_target_ids)
        applied_ids: list[str] = []
        if apply_autopilot and target_ids:
            progress.phase("applying_autopilot", "Applying high-confidence recommendations")
            state, applied_ids = _demote_ids(state, target_ids, _now_iso())
            if applied_ids:
                state = api.persist_state_and_auto_sync(state, reason=ADJUDICATION_REASON)
        finished_at = _now_iso()
        summary = _summary_from_families(families)
        checked_source_count = len(
            {source_id for row in families for source_id in row["checkedSourceIds"]}
        )
        terminal_counts = {
            "checkedFamilies": len(families),
            "totalFamilies": len(selected),
            "checkedSources": checked_source_count,
            "totalSources": len(selected_source_ids),
        }
        result = {
            "ok": True,
            "status": "succeeded",
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "heartbeatAt": finished_at,
            "applyAutopilot": apply_autopilot,
            "checkedFamilyCount": len(families),
            "checkedSourceCount": checked_source_count,
            "demoted": len(applied_ids),
            "appliedIds": applied_ids,
            "families": families,
            "taskProgress": _task_progress_payload(
                active=False,
                phase_key="succeeded",
                phase_label="Conflict source check finished",
                ratio=1.0,
                counts=terminal_counts,
                updated_at=finished_at,
            ),
            "progress": {
                **progress._progress,
                "checkedFamilyCount": len(families),
                "checkedSourceCount": checked_source_count,
                "lastProgressAt": finished_at,
            },
            "summary": summary,
        }
        api.save_json_atomic(_artifact_path(api), result)
        try:
            registry_summary = api.get_registry_summary_payload()
            source_state_path = api.JOBS_FETCH_REPORT_PATH.with_name("jobs-source-state.json")
            cache_payload = overlay_adjudication(
                {
                    **conflict_payload,
                    "registrySummary": api.summarize_state(state),
                    "registryAutoHeal": api.get_registry_auto_heal_report(),
                    "ok": True,
                },
                result,
            )
            cache_key = build_registry_conflicts_summary_cache_key(
                registry_summary=registry_summary,
                source_state_path=source_state_path,
                adjudication_payload=result,
            )
            write_registry_conflicts_summary_cache(
                source_state_path=source_state_path,
                cache_key=cache_key,
                payload=summarize_registry_conflicts_payload(cache_payload),
            )
        except (AttributeError, OSError, TypeError, ValueError):
            pass
        return result


def overlay_adjudication(
    conflict_payload: dict[str, Any], adjudication: dict[str, Any]
) -> dict[str, Any]:
    if not adjudication:
        return conflict_payload
    by_family = {
        _clean(row.get("familyKey")): row
        for row in _as_list(adjudication.get("families"))
        if isinstance(row, dict)
    }
    payload = dict(conflict_payload)
    conflicts = []
    for card in _as_list(payload.get("conflicts")):
        if not isinstance(card, dict):
            continue
        next_card = dict(card)
        family = by_family.get(_clean(card.get("familyKey")))
        if family:
            next_card["adjudication"] = family
        conflicts.append(next_card)
    payload["conflicts"] = conflicts
    payload["adjudication"] = {
        key: value for key, value in adjudication.items() if key != "families"
    }
    return payload


__all__ = [
    "ADJUDICATION_REASON",
    "load_registry_conflict_adjudication",
    "overlay_adjudication",
    "run_registry_conflict_adjudication",
    "start_registry_conflict_adjudication",
]
