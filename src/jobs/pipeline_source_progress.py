from __future__ import annotations

import sys
import time
from typing import Any, Protocol

from src.jobs.common.taxonomy import (
    ClassificationContext,
    FailureBucket,
    classify_zero_kept,
    map_error_to_failure_bucket,
)
from src.jobs.pipeline_runtime_summary import (
    PipelineTaskRuntime,
    record_completed_source_report,
    update_fetch_work_item_progress,
)
from src.jobs.text_utils import clean_text
from src.jobs_fetcher_registry import SOURCE_REPORT_META
from src.shared.utils import now_iso

from .reporting_summary import format_source_error


class _PipelineSourceProgressRoot(Protocol):
    def _failure_bucket_from_zero_extract_context(
        self,
        cls_context: ClassificationContext,
        zero_kept_classification: str,
    ) -> FailureBucket: ...


root: _PipelineSourceProgressRoot | None = None


def _require_root() -> _PipelineSourceProgressRoot:
    if root is None:
        raise RuntimeError("jobs.pipeline_source_progress root is not bound")
    return root


def console_safe_text(value: Any) -> str:
    text = str(value or "")
    stream = getattr(sys, "stdout", None)
    encoding = str(getattr(stream, "encoding", "") or "").strip() or "utf-8"
    try:
        return text.encode(encoding, errors="backslashreplace").decode(encoding)
    except (LookupError, UnicodeError, ValueError):
        return text.encode("ascii", errors="backslashreplace").decode("ascii")


def emit_progress_line(message: str) -> None:
    print(console_safe_text(message), flush=True)


def fallback_error_report(source_name: str, exc: Exception) -> dict[str, Any]:
    report: dict[str, Any] = {
        "name": source_name,
        "status": "error",
        "adapter": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("adapter")) or "custom",
        "fetchStrategy": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("fetchStrategy"))
        or "auto",
        "studio": clean_text(SOURCE_REPORT_META.get(source_name, {}).get("studio")) or "",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": format_source_error(source_name, exc),
        "durationMs": 0,
        "loss": {
            "rawFetched": 0,
            "canonicalDropped": 0,
            "canonicalKept": 0,
            "dedupMerged": 0,
            "finalOutput": 0,
            "canonicalDropReasons": {
                "missing_title": 0,
                "missing_company": 0,
                "missing_job_link": 0,
                "invalid_url": 0,
                "invalid_payload": 0,
            },
        },
    }
    cls_context = ClassificationContext(
        status="error",
        error=report["error"],
        classification="",
        http_status=None,
        fetched_count=0,
    )
    zero_kept_classification = classify_zero_kept(cls_context)
    failure_bucket = map_error_to_failure_bucket(cls_context)
    if failure_bucket == FailureBucket.UNKNOWN:
        failure_bucket = _require_root()._failure_bucket_from_zero_extract_context(
            cls_context,
            zero_kept_classification.value,
        )
    report["failureBucket"] = failure_bucket.value
    report["zeroKeptClassification"] = zero_kept_classification.value
    return report


def mark_task_started(
    *,
    source_name: str,
    task_runtime: PipelineTaskRuntime,
    task_rows: dict[str, dict[str, Any]],
    task_lock,
    write_task_state,
    show_progress: bool,
) -> None:
    start_time = now_iso()
    with task_lock:
        task_rows[source_name]["status"] = "running"
        task_rows[source_name]["startedAt"] = start_time
        task_rows[source_name]["heartbeatAt"] = start_time
        task_rows[source_name]["_startedMonotonic"] = time.perf_counter()
        task_rows[source_name]["_slowWarned"] = False
        task_rows[source_name]["_staticDomainGateWaitMs"] = 0
        task_rows[source_name]["_staticDomainGateWaitCount"] = 0
    update_fetch_work_item_progress(
        task_runtime,
        source_name,
        phase_key="starting_source",
        phase_label="Starting source",
        emit_event=True,
        event_level="info",
        event_message=f"Started source {source_name}.",
    )
    write_task_state()
    if show_progress:
        emit_progress_line(f"[jobs_fetcher] START source={source_name}")


def mark_task_finished(
    *,
    source_name: str,
    report: dict[str, Any],
    task_runtime: PipelineTaskRuntime,
    task_rows: dict[str, dict[str, Any]],
    task_lock,
    write_progress_report,
    write_task_state,
    show_progress: bool,
) -> None:
    end_time = now_iso()
    report_status = str(report.get("status") or "").strip().lower()
    with task_lock:
        task_rows[source_name]["status"] = (
            "excluded"
            if report_status == "excluded"
            else "ok"
            if report_status == "ok"
            else "error"
        )
        task_rows[source_name]["finishedAt"] = end_time
        task_rows[source_name]["durationMs"] = int(report.get("durationMs") or 0)
        task_rows[source_name]["heartbeatAt"] = end_time
        task_rows[source_name]["error"] = clean_text(report.get("error"))
        task_rows[source_name]["_slowWarned"] = False
    record_completed_source_report(task_runtime, source_name=source_name, report=report)
    update_fetch_work_item_progress(
        task_runtime,
        source_name,
        phase_key="completed_source" if report_status in {"ok", "excluded"} else "failed_source",
        phase_label="Completed"
        if report_status == "ok"
        else "Excluded"
        if report_status == "excluded"
        else "Failed",
        counts={
            "fetchedCount": int(report.get("fetchedCount") or 0),
            "keptCount": int(report.get("keptCount") or 0),
            "durationMs": int(report.get("durationMs") or 0),
        },
        emit_event=True,
        event_level="success"
        if report_status == "ok"
        else "warn"
        if report_status == "excluded"
        else "error",
        event_message=(
            f"Finished source {source_name}: status={report_status or 'ok'}, "
            f"fetched={int(report.get('fetchedCount') or 0)}, "
            f"kept={int(report.get('keptCount') or 0)}."
        ),
    )
    write_progress_report()
    write_task_state()
    if show_progress:
        error_text = clean_text(report.get("error"))
        if report.get("status") == "error" and error_text:
            emit_progress_line(f"[jobs_fetcher] ERROR source={source_name} error={error_text}")
        elif error_text:
            emit_progress_line(f"[jobs_fetcher] WARN source={source_name} error={error_text}")
        emit_progress_line(
            f"[jobs_fetcher] DONE source={source_name} status={report['status']} "
            f"fetched={int(report.get('fetchedCount') or 0)} "
            f"kept={int(report.get('keptCount') or 0)} "
            f"durationMs={int(report.get('durationMs') or 0)}"
        )
