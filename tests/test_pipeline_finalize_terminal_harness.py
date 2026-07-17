from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.jobs import pipeline_finalize
from src.jobs.pipeline_bootstrap import build_pipeline_paths


@dataclass(frozen=True)
class _FakeJob:
    row: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return dict(self.row)


def _base_job(source: str = "source_1") -> _FakeJob:
    return _FakeJob(
        {
            "id": "job-1",
            "title": "Gameplay Engineer",
            "company": "Studio One",
            "location": "Remote",
            "source": source,
            "url": "https://studio.example/jobs/1",
        }
    )


def _install_terminal_harness(
    monkeypatch: pytest.MonkeyPatch,
    *,
    deduped_rows: list[_FakeJob] | None = None,
    dedup_stats: dict[str, Any] | None = None,
    detailed_source_rows: list[dict[str, Any]] | None = None,
    preserved_previous: bool = False,
    sector_gate_dropped: int = 0,
    dedup_warning: str = "",
    redirect_cache: dict[str, str] | None = None,
    updated_source_state_rows: dict[str, dict[str, Any]] | None = None,
    provider_static_overlap_summary: Any | None = None,
) -> dict[str, Any]:
    calls: dict[str, Any] = {
        "progress_reports": [],
        "task_states": [],
        "success_cache": [],
        "source_state": [],
        "lifecycle_state": [],
        "lifecycle_archive": [],
        "source_policy_exports": [],
    }
    rows = deduped_rows if deduped_rows is not None else [_base_job()]
    stats = dedup_stats or {
        "sourceCount": 1,
        "successfulSources": 1,
        "failedSources": 0,
        "excludedSources": 0,
    }

    monkeypatch.setattr(pipeline_finalize, "now_iso", lambda: "2026-06-21T10:00:00+00:00")
    monkeypatch.setattr(
        pipeline_finalize,
        "_deduplicate_or_preserve_previous",
        lambda **_kwargs: (list(rows), dict(stats), preserved_previous),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "_apply_lifecycle_state",
        lambda **kwargs: (
            kwargs["deduped_rows"],
            {"job-1": {"state": "active"}},
            {"2026": [{"id": "job-1"}]},
            {"active": 1},
        ),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "_apply_sector_gate",
        lambda payload_rows, _source_reports: (list(payload_rows), sector_gate_dropped),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "_quality_reports",
        lambda _rows: (
            {"location": "ok"},
            {"sector": "ok"},
            {"contamination": "ok"},
            {"city": "ok"},
        ),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "_write_review_queue_artifacts",
        lambda **_kwargs: ([], [{"name": "source_1"}]),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "_write_social_review_artifact",
        lambda **kwargs: (
            {"rows": [], "artifactPath": str(kwargs["paths"].output_dir / "social-review.json")},
            kwargs["paths"].output_dir / "social-review.json",
        ),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "_update_runtime_timing_payload",
        lambda **kwargs: (
            list(detailed_source_rows)
            if detailed_source_rows is not None
            else [
                {
                    "name": "source_1",
                    "status": "ok",
                    "durationMs": 25,
                    "keptCount": len(rows),
                }
            ],
            {"totalDurationMs": 25},
        ),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "build_pipeline_summary",
        lambda *args, **kwargs: {
            "sourceCount": 1,
            "successfulSources": 1,
            "failedSources": 0,
            "excludedSources": 0,
            "outputCount": int(args[0].get("outputCount") or 0),
            "jsonBytes": kwargs["json_bytes"],
            "lightJsonBytes": kwargs["light_json_bytes"],
            "preservedPrevious": bool(args[4]),
        },
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "read_dedup_review_state_artifact",
        lambda _path: ({"summary": {"totalPairs": 3}}, dedup_warning),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "build_dedup_evidence",
        lambda *_args, **_kwargs: {"evidence": "dedup"},
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "summarize_social_experiment",
        lambda *_args, **_kwargs: {"pilot": "summary"},
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "snapshot_task_rows",
        lambda rows_arg: [{"id": row.get("id", "task")} for row in rows_arg],
    )
    monkeypatch.setattr(
        pipeline_finalize.common_sources,
        "load_registry_from_file",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        pipeline_finalize.common_sources,
        "read_approved_since_last_run",
        lambda _path: [],
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "normalize_fetch_report_payload",
        lambda payload: payload,
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "update_source_state_rows",
        lambda **_kwargs: (
            dict(updated_source_state_rows)
            if updated_source_state_rows is not None
            else {
                "source_1": {
                    "name": "source_1",
                    "provider": "greenhouse",
                    "lastKeptCount": len(rows),
                }
            }
        ),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "_merge_source_health_report_payload",
        lambda report_payload, _rows: report_payload.setdefault(
            "sourceHealth", {"status": "merged"}
        ),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "build_provider_coverage_summary",
        lambda _rows: {"providerCount": 1},
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "build_provider_static_overlap_summary",
        provider_static_overlap_summary or (lambda **_kwargs: {"overlapCount": 0}),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "refresh_static_suppression_policy_with_current_evidence",
        lambda policy, **_kwargs: {"refreshed": True, **dict(policy)},
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "build_redundant_static_proposals_summary",
        lambda **_kwargs: {"proposals": []},
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "_export_source_policy_recommendations",
        lambda **kwargs: calls["source_policy_exports"].append(kwargs),
    )
    monkeypatch.setattr(
        pipeline_finalize.health_module,
        "get_top_failing_sources",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        pipeline_finalize.health_module,
        "get_top_zero_kept_sources",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        pipeline_finalize.health_module,
        "get_top_slow_sources",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        pipeline_finalize.health_module,
        "get_quarantined_sources",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "count_site_changed_diagnosed_sources",
        lambda _reports: 0,
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "count_site_changed_missing_old_url_sources",
        lambda _reports: 0,
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "validate_canonical_jobs_payload",
        lambda _rows: None,
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "serialize_rows_for_json",
        lambda rows_arg, _fields: json.dumps(rows_arg),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "write_success_cache",
        lambda path, reports: calls["success_cache"].append((path, reports)),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "write_source_state",
        lambda path, rows_arg: calls["source_state"].append((path, rows_arg)),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "write_job_lifecycle_state",
        lambda path, rows_arg: calls["lifecycle_state"].append((path, rows_arg)),
    )
    monkeypatch.setattr(
        pipeline_finalize,
        "_write_lifecycle_archive_rows",
        lambda **kwargs: calls["lifecycle_archive"].append(kwargs),
    )

    calls["redirect_resolver"] = SimpleNamespace(snapshot_cache=lambda: dict(redirect_cache or {}))
    return calls


def _run_finalize(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    source_reports: list[dict[str, Any]] | None = None,
    source_state_rows: dict[str, Any] | None = None,
    **harness_options: Any,
) -> tuple[dict[str, Any], dict[str, Any], Any, dict[str, str]]:
    calls = _install_terminal_harness(monkeypatch, **harness_options)
    paths = build_pipeline_paths(tmp_path)
    progress_phase = {"key": "deduplicating", "label": "Deduplicating"}
    task_runtime = SimpleNamespace(
        task_rows=[{"id": "task-1"}],
        recent_events=[{"event": "source_progress"}],
    )

    def write_progress_report(*, force: bool = False) -> None:
        calls["progress_reports"].append({"force": force, "phase": dict(progress_phase)})

    def write_task_state(**kwargs: Any) -> None:
        calls["task_states"].append(kwargs)

    report = pipeline_finalize.finalize_pipeline_run(
        paths=paths,
        source_reports=source_reports
        if source_reports is not None
        else [
            {
                "name": "source_1",
                "keptCount": 1,
                "loss": {"canonicalKept": 1},
            }
        ],
        canonical_rows=[],
        using_default_loaders=True,
        selected_loaders=[("source_1", object())],
        effective_seed_from_existing_output=False,
        preserve_previous_on_empty=False,
        source_state_rows=source_state_rows or {},
        lifecycle_rows={},
        runtime_payload={"existing": "runtime"},
        redirect_resolver=calls["redirect_resolver"],
        task_runtime=task_runtime,
        progress_phase=progress_phase,
        write_progress_report=write_progress_report,
        write_task_state=write_task_state,
        started_at="2026-06-21T09:00:00+00:00",
        run_started_mono=0.0,
        run_id="fetch_1",
        circuit_breaker_failures=2,
        circuit_breaker_cooldown_minutes=30,
        circuit_breaker_zero_kept=3,
        static_suppression_policy={"existingPolicy": True},
    )
    return report, calls, paths, progress_phase


def test_finalize_pipeline_run_writes_terminal_outputs_and_report_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, calls, paths, progress_phase = _run_finalize(tmp_path, monkeypatch)

    assert progress_phase == {"key": "writing_outputs", "label": "Writing outputs"}
    assert [row["phase"]["key"] for row in calls["progress_reports"]] == [
        "deduplicating",
        "reconciling_identities",
        "applying_lifecycle",
        "running_quality_audits",
        "writing_outputs",
    ]
    assert calls["task_states"][:-1] == [{"finished_at": "", "force": True}] * 5
    assert calls["task_states"][-1] == {
        "finished_at": "2026-06-21T10:00:00+00:00",
        "force": True,
    }

    assert report["outputs"]["json"] == str(paths.json_path)
    assert report["outputs"]["lightJson"] == str(paths.light_json_path)
    assert json.loads(paths.report_path.read_text(encoding="utf-8")) == report

    assert report["schemaVersion"] == pipeline_finalize.SCHEMA_VERSION
    assert report["runId"] == "fetch_1"
    assert report["finishedAt"] == "2026-06-21T10:00:00+00:00"
    assert report["runtime"]["existing"] == "runtime"
    assert set(report["runtime"]["finalizationTiming"]) == {
        "deduplicatingMs",
        "reconciling_identitiesMs",
        "applying_lifecycleMs",
        "running_quality_auditsMs",
        "writing_outputsMs",
    }
    assert report["runtime"]["lifecycle"]["owner"] == "fetch_report"
    assert report["summary"]["outputCount"] == 1
    assert set(report["summary"]) >= {"jsonBytes", "lightJsonBytes"}
    assert "csvBytes" not in report["summary"]
    assert "csv" not in report["outputs"]
    assert report["taskProgress"]["phaseKey"] == "completed"
    assert report["taskProgress"]["counts"]["outputCount"] == 1
    assert report["workItems"] == [{"id": "task-1"}]
    assert report["recentEvents"] == [{"event": "source_progress"}]
    assert report["sources"][0]["name"] == "source_1"
    sidecar = json.loads(
        paths.report_path.with_name("jobs-fetch-report-summary.json").read_text(encoding="utf-8")
    )
    assert sidecar["taskProgress"]["phaseKey"] == "completed"
    assert sidecar["sources"][0]["name"] == "source_1"
    assert report["sourceFamilies"][0]["loss"]["finalOutput"] == 1
    assert report["sourceFamilies"][0]["loss"]["dedupMerged"] == 0
    assert report["outputs"]["changed"] == {
        "json": True,
        "lightJson": True,
        "availabilityHistory": True,
        "availabilitySweepPlan": True,
    }
    assert report["dedupReviewStateExport"] == {
        "status": "ok",
        "artifactPath": str(paths.dedup_review_state_path),
        "reviewedPairCount": 3,
    }
    assert report["healthSummary"]["parserRegressionQueueCount"] == 1
    assert report["staticSuppressionPolicy"]["existingPolicy"] is True
    assert report["staticSuppressionPolicy"]["refreshed"] is True

    assert calls["success_cache"] == [(paths.success_cache_path, report["sourceFamilies"])]
    assert calls["source_state"][0][0] == paths.source_state_path
    assert calls["lifecycle_state"] == [
        (paths.lifecycle_state_path, {"job-1": {"state": "active"}})
    ]
    assert calls["lifecycle_archive"][0]["archive_rows_by_year"] == {"2026": [{"id": "job-1"}]}
    assert calls["source_policy_exports"][0]["finished_at"] == report["finishedAt"]


def test_finalize_pipeline_run_records_preserved_previous_and_sector_gate_drop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, _calls, _paths, _progress_phase = _run_finalize(
        tmp_path,
        monkeypatch,
        preserved_previous=True,
        sector_gate_dropped=2,
    )

    assert report["summary"]["preservedPrevious"] is True
    assert report["summary"]["outputCount"] == 1
    assert report["taskProgress"]["counts"]["outputCount"] == 1


def test_finalize_pipeline_run_exports_dedup_review_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    report, _calls, paths, _progress_phase = _run_finalize(
        tmp_path,
        monkeypatch,
        dedup_warning="dedup_review_state_unreadable: invalid json",
    )

    assert report["dedupReviewStateExport"] == {
        "status": "warning",
        "artifactPath": str(paths.dedup_review_state_path),
        "warning": "dedup_review_state_unreadable: invalid json",
    }


def test_finalize_pipeline_run_persists_google_sheets_redirect_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _state_rows(**_kwargs: Any) -> dict[str, Any]:
        return {
            "google_sheets_source": {
                "name": "google_sheets_source",
                "lastKeptCount": 1,
            }
        }

    _install_terminal_harness(
        monkeypatch,
        redirect_cache={"https://docs.google.com/sheet": "https://studio.example/jobs"},
    )
    monkeypatch.setattr(pipeline_finalize, "update_source_state_rows", _state_rows)
    paths = build_pipeline_paths(tmp_path)
    progress_phase = {"key": "deduplicating", "label": "Deduplicating"}
    calls: dict[str, Any] = {
        "progress_reports": [],
        "task_states": [],
        "source_state": [],
    }

    monkeypatch.setattr(
        pipeline_finalize,
        "write_source_state",
        lambda path, rows_arg: calls["source_state"].append((path, rows_arg)),
    )

    report = pipeline_finalize.finalize_pipeline_run(
        paths=paths,
        source_reports=[{"name": "source_1", "keptCount": 1, "loss": {"canonicalKept": 1}}],
        canonical_rows=[],
        using_default_loaders=True,
        selected_loaders=[("source_1", object())],
        effective_seed_from_existing_output=False,
        preserve_previous_on_empty=False,
        source_state_rows={},
        lifecycle_rows={},
        runtime_payload={},
        redirect_resolver=SimpleNamespace(
            snapshot_cache=lambda: {"https://docs.google.com/sheet": "https://studio.example/jobs"}
        ),
        task_runtime=SimpleNamespace(task_rows=[], recent_events=[]),
        progress_phase=progress_phase,
        write_progress_report=lambda **kwargs: calls["progress_reports"].append(kwargs),
        write_task_state=lambda **kwargs: calls["task_states"].append(kwargs),
        started_at="2026-06-21T09:00:00+00:00",
        run_started_mono=0.0,
        run_id="fetch_1",
        circuit_breaker_failures=2,
        circuit_breaker_cooldown_minutes=30,
        circuit_breaker_zero_kept=3,
    )

    persisted_rows = calls["source_state"][0][1]
    assert persisted_rows["google_sheets_source"]["googleSheetsRedirectCache"] == {
        "https://docs.google.com/sheet": "https://studio.example/jobs"
    }
    assert report["providerCoverage"] == {"providerCount": 1}


def test_finalize_pipeline_run_restores_prior_provider_overlap_state_for_dynamic_redundant(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    def overlap_spy(**kwargs: Any) -> dict[str, Any]:
        captured["source_rows"] = kwargs["source_rows"]
        captured["source_state_rows"] = kwargs["source_state_rows"]
        return {"overlapCount": 1}

    report, _calls, _paths, _progress_phase = _run_finalize(
        tmp_path,
        monkeypatch,
        source_reports=[
            {
                "name": "dynamic_provider",
                "status": "excluded",
                "exclusionReason": "dynamic_redundant_provider",
                "keptCount": 0,
                "loss": {"canonicalKept": 0},
            }
        ],
        source_state_rows={
            "dynamic_provider": {
                "name": "dynamic_provider",
                "provider": "greenhouse",
                "lastKeptCount": 12,
                "prior": True,
            }
        },
        detailed_source_rows=[
            {
                "name": "dynamic_provider",
                "status": "excluded",
                "exclusionReason": "dynamic_redundant_provider",
                "keptCount": 0,
            }
        ],
        updated_source_state_rows={
            "dynamic_provider": {
                "name": "dynamic_provider",
                "provider": "greenhouse",
                "lastKeptCount": 0,
                "prior": False,
            }
        },
        provider_static_overlap_summary=overlap_spy,
    )

    assert report["providerStaticOverlap"] == {"overlapCount": 1}
    assert captured["source_rows"][0]["name"] == "dynamic_provider"
    assert captured["source_state_rows"]["dynamic_provider"] == {
        "name": "dynamic_provider",
        "provider": "greenhouse",
        "lastKeptCount": 12,
        "prior": True,
    }
