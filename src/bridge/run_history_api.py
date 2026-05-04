"""Run-history projection helpers for admin task lifecycle state."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.bridge.task_history import TaskHistoryManager
from src.shared.json_shapes import as_json_object

_ORPHANED_RUN_ERROR = "owner_inactive_without_terminal_report"


def load_run_history(manager: TaskHistoryManager) -> list[dict[str, Any]]:
    return manager.load_run_history()


def save_run_history(manager: TaskHistoryManager, rows: list[dict[str, Any]]) -> None:
    manager.save_run_history(rows)


def append_run_history(manager: TaskHistoryManager, row: dict[str, Any]) -> dict[str, Any]:
    return manager.append_run_history(row)


def upsert_run_history(
    manager: TaskHistoryManager,
    entry: dict[str, Any],
    *,
    dedupe_fields: tuple[str, ...],
) -> dict[str, Any]:
    return manager.upsert_run_history(entry, dedupe_fields=dedupe_fields)


def prune_started_rows_for_type(
    manager: TaskHistoryManager,
    run_type: str,
    *,
    keep_started_at: str = "",
    finished_at: str = "",
) -> None:
    manager.prune_started_rows_for_type(
        run_type, keep_started_at=keep_started_at, finished_at=finished_at
    )


def clear_task_state(manager: TaskHistoryManager, task_type: str) -> None:
    manager.clear_task_state(task_type)


def _load_task_state_entry(
    task_type: str,
    load_json_object: Callable[[Any, dict[str, Any]], dict[str, Any]],
    task_state_path: Any,
) -> dict[str, Any]:
    state = load_json_object(task_state_path, {})
    if not isinstance(state, dict):
        return {}
    entry = state.get(str(task_type))
    return dict(entry) if isinstance(entry, dict) else {}


def task_running_from_state(
    task_type: str,
    load_json_object: Callable[[Any, dict[str, Any]], dict[str, Any]],
    task_state_path: Any,
    pid_is_running: Callable[[int], bool],
) -> bool:
    entry = _load_task_state_entry(task_type, load_json_object, task_state_path)
    pid = int(entry.get("pid") or 0)
    return pid_is_running(pid)


def _safe_parse_iso(
    parse_iso: Callable[[Any], datetime | None],
    value: Any,
) -> datetime | None:
    try:
        return parse_iso(value)
    except Exception:  # noqa: BLE001
        return None


def _path_is_recent(
    path: Path | None, now_utc: Callable[[], datetime], *, max_idle_minutes: float
) -> bool:
    if path is None:
        return False
    try:
        mtime_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return False
    idle_minutes = (now_utc() - mtime_dt).total_seconds() / 60.0
    return idle_minutes <= float(max_idle_minutes)


def _signal_is_recent(
    text: str,
    *,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    max_idle_minutes: float,
) -> bool:
    signal_dt = _safe_parse_iso(parse_iso, text)
    if not signal_dt:
        return False
    idle_minutes = (now_utc() - signal_dt).total_seconds() / 60.0
    return idle_minutes <= float(max_idle_minutes)


def report_is_stale_in_progress(
    task_type: str,
    path: Path,
    report: dict[str, Any],
    *,
    load_json_object: Callable[[Any, dict[str, Any]], dict[str, Any]],
    task_state_path: Any,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    pid_is_running: Callable[[int], bool],
    max_age_minutes: int = 5,
    max_mtime_idle_minutes: float = 2.0,
) -> bool:
    started_raw = str(report.get("startedAt") or "")
    finished_raw = str(report.get("finishedAt") or "")
    run_id = str(report.get("runId") or "").strip()
    if not started_raw or finished_raw:
        return False
    started_dt = _safe_parse_iso(parse_iso, started_raw)
    if not started_dt:
        return False

    state_entry = _load_task_state_entry(task_type, load_json_object, task_state_path)
    state_run_id = str(state_entry.get("runId") or "").strip()
    if state_run_id and run_id and state_run_id == run_id:
        pid = int(state_entry.get("pid") or 0)
        if pid_is_running(pid):
            return False

    lifecycle = (
        report.get("runtime", {}).get("lifecycle")
        if isinstance(report.get("runtime"), dict)
        and isinstance((report.get("runtime") or {}).get("lifecycle"), dict)
        else {}
    )
    heartbeat_at = str(lifecycle.get("heartbeatAt") or "").strip()
    if _signal_is_recent(
        heartbeat_at,
        parse_iso=parse_iso,
        now_utc=now_utc,
        max_idle_minutes=max_mtime_idle_minutes,
    ):
        return False
    if _path_is_recent(path, now_utc, max_idle_minutes=max_mtime_idle_minutes):
        return False

    age_minutes = (now_utc() - started_dt).total_seconds() / 60.0
    return age_minutes >= float(max_age_minutes)


@dataclass(frozen=True)
class ChildTaskSnapshot:
    task_type: str
    run_id: str
    started_at: str
    finished_at: str
    active: bool
    terminal_status: str
    summary: dict[str, Any]
    outputs: dict[str, Any]
    task_progress: dict[str, Any]
    explicit_dead: bool
    diagnostics: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class LifecycleProjection:
    rows: list[dict[str, Any]]
    child_tasks: dict[str, ChildTaskSnapshot]
    diagnostics: list[dict[str, Any]]


@dataclass
class SyncHistoryDeps:
    ops_state_lock: Any
    load_run_history: Callable[[], list[dict[str, Any]]]
    save_run_history: Callable[[list[dict[str, Any]]], None]
    save_json_atomic: Callable[[Path, Any], None]
    prune_started_rows_for_type: Callable[..., None]
    clear_task_state: Callable[[str], None]
    clear_task_state_locked: Callable[[str], None]
    upsert_run_history: Callable[..., dict[str, Any]]
    task_running_from_state: Callable[[str], bool]
    report_is_stale_in_progress: Callable[..., bool]
    load_json_object: Callable[[Any, dict[str, Any]], dict[str, Any]]
    normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    normalize_discovery_report_contract: Callable[[dict[str, Any]], dict[str, Any]]
    summarize_fetch_report: Callable[[dict[str, Any]], dict[str, Any]]
    summarize_discovery_report: Callable[[dict[str, Any]], tuple[dict[str, Any], str]]
    jobs_fetch_report_path: Path
    jobs_fetch_tasks_path: Path
    discovery_report_path: Path
    task_state_path: Path
    get_active_sync_runs: Callable[[], set[str]]
    parse_iso: Callable[[Any], datetime | None]
    now_iso: Callable[[], str]
    now_utc: Callable[[], datetime]


def _row_score(row: dict[str, Any]) -> tuple[int, int, int]:
    finished = 1 if str(row.get("finishedAt") or "").strip() else 0
    has_summary = 1 if isinstance(row.get("summary"), dict) and row.get("summary") else 0
    duration = int(row.get("durationMs") or 0)
    return (finished, has_summary, duration)


def _collapse_duplicate_history_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    run_id_rows: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_type = str(row.get("type") or "").strip().lower()
        if not row_type:
            continue
        run_id = str(row.get("runId") or "").strip()
        if not run_id:
            continue
        key = (row_type, run_id)
        existing = run_id_rows.get(key)
        if existing is None or _row_score(row) >= _row_score(existing):
            merged = {**dict(existing or {}), **dict(row)}
            merged["type"] = row_type
            merged["runId"] = run_id
            merged["id"] = str(merged.get("id") or run_id)
            run_id_rows[key] = merged
    combined = list(run_id_rows.values())
    combined.sort(key=lambda item: str(item.get("startedAt") or item.get("finishedAt") or ""))
    return combined


def _snapshot_duration_ms(
    started_at: str,
    finished_at: str,
    *,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
) -> int:
    started_dt = _safe_parse_iso(parse_iso, started_at)
    finished_dt = _safe_parse_iso(parse_iso, finished_at) or now_utc()
    if not started_dt or not finished_dt:
        return 0
    return int(max(0.0, (finished_dt - started_dt).total_seconds() * 1000))


def _lifecycle_heartbeat(payload: dict[str, Any]) -> str:
    if not isinstance(payload, dict):
        return ""
    lifecycle = (
        payload.get("runtime", {}).get("lifecycle")
        if isinstance(payload.get("runtime"), dict)
        and isinstance((payload.get("runtime") or {}).get("lifecycle"), dict)
        else {}
    )
    if lifecycle:
        return str(lifecycle.get("heartbeatAt") or "").strip()
    return str(payload.get("heartbeatAt") or "").strip()


def _task_progress_active(payload: dict[str, Any]) -> bool:
    progress = payload.get("taskProgress")
    return bool(progress.get("active")) if isinstance(progress, dict) else False


def _payload_has_live_run_identity(payload: dict[str, Any], run_id: str = "") -> bool:
    if not isinstance(payload, dict):
        return False
    payload_run_id = str(payload.get("runId") or "").strip()
    if run_id and payload_run_id and payload_run_id != run_id:
        return False
    if str(payload.get("finishedAt") or "").strip():
        return False
    return bool(
        payload.get("active")
        or _task_progress_active(payload)
        or payload_run_id
        or str(payload.get("startedAt") or "").strip()
        or bool(payload.get("workItems"))
        or bool(payload.get("recentEvents"))
    )


def _build_child_task_snapshot(
    *,
    task_type: str,
    report: dict[str, Any],
    report_path: Path,
    task_state_path: Path,
    load_json_object: Callable[[Any, dict[str, Any]], dict[str, Any]],
    task_running_from_state: Callable[[str], bool],
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    summary_builder: Callable[[dict[str, Any]], dict[str, Any]]
    | Callable[[dict[str, Any]], tuple[dict[str, Any], str]],
    task_artifact: dict[str, Any] | None = None,
    task_artifact_path: Path | None = None,
    terminal_status_builder: Callable[[dict[str, Any]], str] | None = None,
    max_idle_minutes: float = 2.0,
    dead_age_minutes: float = 5.0,
) -> ChildTaskSnapshot:
    diagnostics: list[dict[str, Any]] = []
    run_id = str(report.get("runId") or "").strip()
    started_at = str(report.get("startedAt") or "").strip()
    finished_at = str(report.get("finishedAt") or "").strip()
    outputs = dict(report.get("outputs") or {})
    task_progress = dict(report.get("taskProgress") or {})
    state_entry = _load_task_state_entry(task_type, load_json_object, task_state_path)
    state_run_id = str(state_entry.get("runId") or "").strip()
    state_matches = bool(run_id and state_run_id and state_run_id == run_id)
    state_heartbeat_at = str(state_entry.get("heartbeatAt") or "").strip()
    state_recent = bool(
        state_matches
        and _signal_is_recent(
            state_heartbeat_at,
            parse_iso=parse_iso,
            now_utc=now_utc,
            max_idle_minutes=max_idle_minutes,
        )
    )
    state_active = bool(state_matches and (task_running_from_state(task_type) or state_recent))

    artifact = task_artifact if isinstance(task_artifact, dict) else {}
    artifact_run_id = str(artifact.get("runId") or "").strip()
    artifact_matches = bool(run_id and artifact_run_id and artifact_run_id == run_id)
    artifact_active = bool(
        artifact_matches
        and _payload_has_live_run_identity(artifact, run_id)
        and (
            _signal_is_recent(
                _lifecycle_heartbeat(artifact),
                parse_iso=parse_iso,
                now_utc=now_utc,
                max_idle_minutes=max_idle_minutes,
            )
            or _path_is_recent(
                task_artifact_path,
                now_utc,
                max_idle_minutes=max_idle_minutes,
            )
        )
    )
    report_active = bool(
        run_id
        and _payload_has_live_run_identity(report, run_id)
        and (
            _signal_is_recent(
                _lifecycle_heartbeat(report),
                parse_iso=parse_iso,
                now_utc=now_utc,
                max_idle_minutes=max_idle_minutes,
            )
            or _path_is_recent(report_path, now_utc, max_idle_minutes=max_idle_minutes)
        )
    )
    progress_active = bool(
        run_id
        and _task_progress_active(report)
        and _signal_is_recent(
            str(as_json_object(report.get("taskProgress")).get("updatedAt") or "").strip(),
            parse_iso=parse_iso,
            now_utc=now_utc,
            max_idle_minutes=max_idle_minutes,
        )
    )

    if state_run_id and run_id and state_run_id != run_id:
        diagnostics.append(
            {
                "code": "task_state_run_id_mismatch",
                "taskType": task_type,
                "stateRunId": state_run_id,
                "reportRunId": run_id,
            }
        )
    if artifact_run_id and run_id and artifact_run_id != run_id:
        diagnostics.append(
            {
                "code": "task_artifact_run_id_mismatch",
                "taskType": task_type,
                "artifactRunId": artifact_run_id,
                "reportRunId": run_id,
            }
        )

    owner_active = bool(
        run_id and (state_active or artifact_active or report_active or progress_active)
    )
    if finished_at and owner_active:
        diagnostics.append(
            {
                "code": "report_finished_while_owner_active",
                "taskType": task_type,
                "runId": run_id,
            }
        )
        finished_at = ""

    active = bool(owner_active)
    explicit_dead = False
    if run_id and not finished_at and not active:
        started_dt = _safe_parse_iso(parse_iso, started_at)
        age_minutes = (
            (now_utc() - started_dt).total_seconds() / 60.0
            if started_dt
            else float(dead_age_minutes)
        )
        explicit_dead = age_minutes >= float(dead_age_minutes)

    summary: dict[str, Any]
    terminal_status: str
    if terminal_status_builder is not None:
        summary_result = summary_builder(report)
        summary = dict(summary_result) if isinstance(summary_result, dict) else {}
        terminal_status = terminal_status_builder(summary)
    else:
        summary_result = summary_builder(report)
        if isinstance(summary_result, tuple):
            summary, terminal_status = summary_result
        else:
            summary = dict(summary_result)
            terminal_status = ""

    return ChildTaskSnapshot(
        task_type=task_type,
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        active=active,
        terminal_status=str(terminal_status or "").strip().lower(),
        summary=dict(summary or {}),
        outputs=outputs,
        task_progress=task_progress,
        explicit_dead=bool(explicit_dead),
        diagnostics=tuple(diagnostics),
    )


def _fetch_terminal_status(summary: dict[str, Any]) -> str:
    return str(summary.get("status") or "").strip().lower() or "ok"


def _build_history_row_from_snapshot(
    snapshot: ChildTaskSnapshot,
    *,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    now_iso: Callable[[], str],
) -> dict[str, Any]:
    if snapshot.finished_at:
        return {
            "id": snapshot.run_id,
            "runId": snapshot.run_id,
            "type": snapshot.task_type,
            "status": snapshot.terminal_status or "ok",
            "startedAt": snapshot.started_at,
            "finishedAt": snapshot.finished_at,
            "durationMs": _snapshot_duration_ms(
                snapshot.started_at,
                snapshot.finished_at,
                parse_iso=parse_iso,
                now_utc=now_utc,
            ),
            "summary": dict(snapshot.summary),
        }
    if snapshot.active:
        return {
            "id": snapshot.run_id,
            "runId": snapshot.run_id,
            "type": snapshot.task_type,
            "status": "started",
            "startedAt": snapshot.started_at,
            "finishedAt": "",
            "durationMs": _snapshot_duration_ms(
                snapshot.started_at,
                "",
                parse_iso=parse_iso,
                now_utc=now_utc,
            ),
            "summary": dict(snapshot.summary),
        }
    return {
        "id": snapshot.run_id,
        "runId": snapshot.run_id,
        "type": snapshot.task_type,
        "status": "error",
        "startedAt": snapshot.started_at,
        "finishedAt": now_iso(),
        "durationMs": _snapshot_duration_ms(
            snapshot.started_at,
            "",
            parse_iso=parse_iso,
            now_utc=now_utc,
        ),
        "summary": {
            **dict(snapshot.summary),
            "error": _ORPHANED_RUN_ERROR,
        },
    }


def _replace_run_row(
    rows: list[dict[str, Any]],
    snapshot: ChildTaskSnapshot,
    *,
    parse_iso: Callable[[Any], datetime | None],
    now_utc: Callable[[], datetime],
    now_iso: Callable[[], str],
    diagnostics: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not snapshot.run_id:
        return rows
    projected_row = _build_history_row_from_snapshot(
        snapshot,
        parse_iso=parse_iso,
        now_utc=now_utc,
        now_iso=now_iso,
    )
    next_rows: list[dict[str, Any]] = []
    for row in rows:
        same_type = str(row.get("type") or "").strip().lower() == snapshot.task_type
        same_run_id = str(row.get("runId") or "").strip() == snapshot.run_id
        if same_type and same_run_id:
            if snapshot.active and str(row.get("finishedAt") or "").strip():
                diagnostics.append(
                    {
                        "code": "history_finished_while_owner_active",
                        "taskType": snapshot.task_type,
                        "runId": snapshot.run_id,
                    }
                )
            continue
        next_rows.append(row)
    next_rows.append(projected_row)
    return next_rows


def reconcile_sync_history_locked(deps: SyncHistoryDeps) -> None:
    history = _collapse_duplicate_history_rows(deps.load_run_history())
    deps.save_run_history(history)


def project_run_history(deps: SyncHistoryDeps) -> LifecycleProjection:
    history = _collapse_duplicate_history_rows(deps.load_run_history())
    diagnostics: list[dict[str, Any]] = []

    fetch_report = deps.normalize_fetch_report_contract(
        deps.load_json_object(deps.jobs_fetch_report_path, {})
    )
    fetch_task_artifact = deps.load_json_object(deps.jobs_fetch_tasks_path, {})
    fetch_snapshot = _build_child_task_snapshot(
        task_type="fetch",
        report=fetch_report,
        report_path=deps.jobs_fetch_report_path,
        task_state_path=deps.task_state_path,
        load_json_object=deps.load_json_object,
        task_running_from_state=deps.task_running_from_state,
        parse_iso=deps.parse_iso,
        now_utc=deps.now_utc,
        summary_builder=deps.summarize_fetch_report,
        terminal_status_builder=_fetch_terminal_status,
        task_artifact=fetch_task_artifact if isinstance(fetch_task_artifact, dict) else None,
        task_artifact_path=deps.jobs_fetch_tasks_path,
    )
    diagnostics.extend(fetch_snapshot.diagnostics)
    if fetch_snapshot.run_id and (
        fetch_snapshot.active or fetch_snapshot.finished_at or fetch_snapshot.explicit_dead
    ):
        history = _replace_run_row(
            history,
            fetch_snapshot,
            parse_iso=deps.parse_iso,
            now_utc=deps.now_utc,
            now_iso=deps.now_iso,
            diagnostics=diagnostics,
        )

    discovery_report = deps.normalize_discovery_report_contract(
        deps.load_json_object(deps.discovery_report_path, {})
    )
    discovery_snapshot = _build_child_task_snapshot(
        task_type="discovery",
        report=discovery_report,
        report_path=deps.discovery_report_path,
        task_state_path=deps.task_state_path,
        load_json_object=deps.load_json_object,
        task_running_from_state=deps.task_running_from_state,
        parse_iso=deps.parse_iso,
        now_utc=deps.now_utc,
        summary_builder=deps.summarize_discovery_report,
    )
    diagnostics.extend(discovery_snapshot.diagnostics)
    if discovery_snapshot.run_id and (
        discovery_snapshot.active
        or discovery_snapshot.finished_at
        or discovery_snapshot.explicit_dead
    ):
        history = _replace_run_row(
            history,
            discovery_snapshot,
            parse_iso=deps.parse_iso,
            now_utc=deps.now_utc,
            now_iso=deps.now_iso,
            diagnostics=diagnostics,
        )

    final_rows = _collapse_duplicate_history_rows(history)
    return LifecycleProjection(
        rows=final_rows,
        child_tasks={
            "fetch": fetch_snapshot,
            "discovery": discovery_snapshot,
        },
        diagnostics=diagnostics,
    )


def sync_history_from_reports(deps: SyncHistoryDeps) -> list[dict[str, Any]]:
    with deps.ops_state_lock:
        projection = project_run_history(deps)
        deps.save_run_history(projection.rows)
        return projection.rows
