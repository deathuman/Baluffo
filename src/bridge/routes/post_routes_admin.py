from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol

from src.bridge.registry_conflict_adjudication import start_registry_conflict_adjudication
from src.bridge.registry_conflicts import (
    SAFE_AUTO_DEMOTE_ACTION,
    SAFE_AUTO_DEMOTE_ACTIONS,
    SAFE_AUTO_DEMOTE_REASON,
    apply_registry_conflict_safe_demotions,
)
from src.bridge.registry_tombstones import (
    add_tombstone,
    remove_tombstone,
    tombstone_source_row,
)
from src.bridge.routes.error_boundary import (
    run_route_boundary,
    safe_bridge_log,
    send_json_boundary,
)
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.source_policy_migration_links import apply_source_policy_migration_link_action
from src.jobs.common.contracts_dedup_review_state import (
    apply_dedup_review_action,
    read_dedup_review_state_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    apply_source_policy_review_action,
    read_source_policy_review_state_artifact,
)
from src.shared.json_shapes import (
    as_json_list,
    as_json_object,
    copy_json_object,
    json_object_rows,
)
from src.source_registry import (
    REGISTRY_REASON_APPROVE,
    REGISTRY_REASON_DELETE,
    REGISTRY_REASON_FETCH_EMPTY_DEMOTE,
    REGISTRY_REASON_FETCH_FAILURE_DEMOTE,
    REGISTRY_REASON_REJECT,
    REGISTRY_REASON_RESTORE_DELETED,
    REGISTRY_REASON_RESTORE_REJECTED,
    REGISTRY_REASON_ROLLBACK,
    transition_registry_to_active,
    transition_registry_to_pending,
    transition_registry_to_rejected,
)


class _AdminPostRouteApi(Protocol):
    APPROVAL_STATE_PATH: Path
    DEDUP_REVIEW_STATE_PATH: Path
    JOBS_FETCH_REPORT_PATH: Path
    SOURCE_POLICY_REVIEW_STATE_PATH: Path

    def abort_task(self, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]: ...

    def add_manual_source(self, url: str) -> dict[str, Any]: ...

    def bridge_log(self, level: str, event: str, **fields: Any) -> None: ...

    def compute_ops_health(self) -> dict[str, Any]: ...

    def get_discovery_config_payload(self) -> dict[str, Any]: ...

    def get_jobs_pipeline_schedule_payload(self) -> dict[str, Any]: ...

    def get_sync_status_payload(self) -> dict[str, Any]: ...

    def load_alert_state(self) -> dict[str, Any]: ...

    def load_json_object(self, path: Path, default: Any = None) -> dict[str, Any]: ...

    def load_state(self) -> dict[str, list[dict[str, Any]]]: ...

    def load_tombstones(self) -> dict[str, Any]: ...

    def move_entries(
        self, rows: list[dict[str, Any]], ids: list[str]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]: ...

    def normalize_source_url(self, url: str) -> str: ...

    def now_iso(self) -> str: ...

    def persist_state_and_auto_sync(
        self, state: dict[str, list[dict[str, Any]]], *, reason: str
    ) -> dict[str, list[dict[str, Any]]]: ...

    def save_alert_state(self, state: dict[str, Any]) -> None: ...

    def save_json_atomic(self, path: Path, payload: dict[str, Any]) -> None: ...

    def save_tombstones(self, tombstones: dict[str, Any]) -> None: ...

    def set_sync_status(self, **fields: Any) -> None: ...

    def source_identity(self, row: dict[str, Any]) -> str: ...

    def source_url_fingerprint(self, row: dict[str, Any]) -> str: ...

    def start_fetcher_task(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def start_jobs_bootstrap_task(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def start_jobs_pipeline_task(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def start_sync_task(self, action: str, *, reason: str, automatic: bool) -> dict[str, Any]: ...

    def summarize_state(self, state: dict[str, list[dict[str, Any]]]) -> dict[str, Any]: ...

    def sync_config_status(self) -> dict[str, Any]: ...

    def sync_pull_sources(self) -> dict[str, Any]: ...

    def sync_push_sources(self) -> dict[str, Any]: ...

    def test_sync_config(self) -> dict[str, Any]: ...

    def trigger_discovery_task(
        self, *, payload: dict[str, Any], route_name: str
    ) -> tuple[int, dict[str, Any]]: ...

    def trigger_source_check(self, source_id: str) -> dict[str, Any]: ...

    def unique_sources(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]: ...

    def update_jobs_pipeline_schedule(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def update_saved_discovery_settings(self, payload: dict[str, Any]) -> None: ...

    def update_saved_sync_settings(self, payload: dict[str, Any]) -> None: ...


def _transition_registry_row(
    api: _AdminPostRouteApi,
    row: dict[str, Any],
    *,
    candidate_state: str,
    reason: str = "",
    approved_by: str = "",
    quarantine_reason: str = "",
) -> dict[str, Any]:
    if candidate_state == "live":
        return transition_registry_to_active(
            row,
            reason=reason or REGISTRY_REASON_APPROVE,
            actor=str(approved_by or reason or "registry_manual_approve"),
            at=str(getattr(api, "now_iso", lambda: "")() or ""),
        )
    if candidate_state == "validated":
        return transition_registry_to_pending(
            row,
            reason=reason or REGISTRY_REASON_ROLLBACK,
            actor=str(approved_by or reason or "registry_manual_restore"),
            at=str(getattr(api, "now_iso", lambda: "")() or ""),
        )
    if candidate_state == "quarantined":
        return transition_registry_to_rejected(
            row,
            reason=quarantine_reason or reason or REGISTRY_REASON_REJECT,
            actor=str(approved_by or reason or "registry_manual_reject"),
            at=str(getattr(api, "now_iso", lambda: "")() or ""),
        )
    return dict(row)


def _json_error(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc)}


def _handle_source_policy_post(
    handler: BridgeResponseWriter,
    *,
    api: _AdminPostRouteApi,
    path: str,
    data: dict[str, Any],
) -> bool:
    if path == "/dedup/review-action":

        def _payload() -> dict[str, Any]:
            prior_review_state, warning = read_dedup_review_state_artifact(
                api.DEDUP_REVIEW_STATE_PATH
            )
            review_state, pair = apply_dedup_review_action(
                prior_artifact=prior_review_state,
                action_payload=data,
                updated_at=str(getattr(api, "now_iso", lambda: "")() or ""),
                default_updated_by="admin",
            )
            api.save_json_atomic(api.DEDUP_REVIEW_STATE_PATH, review_state)
            return {
                "ok": True,
                "pair": pair,
                "summary": review_state.get("summary", {}),
                "artifactPath": str(api.DEDUP_REVIEW_STATE_PATH),
                **({"warning": warning} if warning else {}),
            }

        send_json_boundary(
            handler,
            _payload,
            error_status=400,
            error_payload=lambda exc: {"ok": False, "error": str(exc)},
        )
        return True

    if path == "/source-policy/review-action":

        def _payload() -> dict[str, Any]:
            prior_review_state, warning = read_source_policy_review_state_artifact(
                api.SOURCE_POLICY_REVIEW_STATE_PATH
            )
            review_state, pair = apply_source_policy_review_action(
                prior_artifact=prior_review_state,
                action_payload=data,
                updated_at=str(getattr(api, "now_iso", lambda: "")() or ""),
                default_updated_by="admin",
            )
            api.save_json_atomic(api.SOURCE_POLICY_REVIEW_STATE_PATH, review_state)
            return {
                "ok": True,
                "pair": pair,
                "summary": review_state.get("summary", {}),
                "artifactPath": str(api.SOURCE_POLICY_REVIEW_STATE_PATH),
                **({"warning": warning} if warning else {}),
            }

        send_json_boundary(
            handler,
            _payload,
            error_status=400,
            error_payload=lambda exc: {"ok": False, "error": str(exc)},
        )
        return True

    if path == "/source-policy/migration-link-action":

        def _payload() -> dict[str, Any]:
            return apply_source_policy_migration_link_action(api, data)

        send_json_boundary(
            handler,
            _payload,
            error_status=400,
            error_payload=lambda exc: {"ok": False, "error": str(exc)},
        )
        return True

    return False


def handle_post(
    handler: BridgeResponseWriter, *, api: _AdminPostRouteApi, path: str, payload: Any
) -> bool:
    state = api.load_state()
    data = as_json_object(payload)

    if _handle_source_policy_post(handler, api=api, path=path, data=data):
        return True

    if path == "/sources/manual":
        result = api.add_manual_source(str(data.get("url") or ""))
        handler.send_json(result)
        return True

    if path == "/discovery/check-source":
        result = api.trigger_source_check(str(data.get("sourceId") or ""))
        status = 200 if bool(result.get("started")) else 400
        handler.send_json(result, status=status)
        return True

    if path == "/registry/conflicts/check-sources":
        result = start_registry_conflict_adjudication(api, data)
        status = 200 if bool(result.get("ok")) else 409
        handler.send_json(result, status=status)
        return True

    if path == "/registry/approve":
        ids = as_json_list(data.get("ids"))
        moved, remaining = api.move_entries(state["pending"], [str(item) for item in ids])
        moved = [
            _transition_registry_row(
                api,
                row,
                candidate_state="live",
                reason=REGISTRY_REASON_APPROVE,
                approved_by="registry_manual_approve",
            )
            for row in moved
        ]
        state["pending"] = remaining
        state["active"] = api.unique_sources([*state["active"], *moved])
        state = api.persist_state_and_auto_sync(state, reason="registry_approve")
        approval = api.load_json_object(api.APPROVAL_STATE_PATH, {"approvedSinceLastRun": 0})
        approval["approvedSinceLastRun"] = int(approval.get("approvedSinceLastRun") or 0) + len(
            moved
        )
        api.save_json_atomic(api.APPROVAL_STATE_PATH, approval)
        handler.send_json({"approved": len(moved), "summary": api.summarize_state(state)})
        return True

    if path == "/registry/reject":
        ids = as_json_list(data.get("ids"))
        moved, remaining = api.move_entries(state["pending"], [str(item) for item in ids])
        state["pending"] = remaining
        moved = [
            _transition_registry_row(
                api,
                row,
                candidate_state="quarantined",
                reason=REGISTRY_REASON_REJECT,
                approved_by="registry_manual_reject",
                quarantine_reason=REGISTRY_REASON_REJECT,
            )
            for row in moved
        ]
        state["rejected"] = api.unique_sources([*state["rejected"], *moved])
        state = api.persist_state_and_auto_sync(state, reason=REGISTRY_REASON_REJECT)
        handler.send_json({"rejected": len(moved), "summary": api.summarize_state(state)})
        return True

    if path == "/registry/rollback":
        ids = as_json_list(data.get("ids"))
        selected = set(str(item) for item in ids)
        moved = []
        active_remaining = []
        for row in state["active"]:
            if api.source_identity(row) in selected:
                moved.append(
                    _transition_registry_row(
                        api,
                        row,
                        candidate_state="validated",
                        reason=REGISTRY_REASON_ROLLBACK,
                        approved_by="registry_manual_rollback",
                    )
                )
            else:
                active_remaining.append(row)
        state["active"] = active_remaining
        state["pending"] = api.unique_sources([*state["pending"], *moved])
        state = api.persist_state_and_auto_sync(state, reason=REGISTRY_REASON_ROLLBACK)
        handler.send_json({"rolledBack": len(moved), "summary": api.summarize_state(state)})
        return True

    if path == "/registry/demote-active":
        ids = as_json_list(data.get("ids"))
        selected = set(str(item) for item in ids)
        moved = []
        active_remaining = []
        demote_reason = (
            REGISTRY_REASON_FETCH_FAILURE_DEMOTE if selected else REGISTRY_REASON_FETCH_EMPTY_DEMOTE
        )
        for row in state["active"]:
            row_id = api.source_identity(row)
            jobs_found = max(0, int(row.get("jobsFound") or 0))
            if selected:
                if row_id in selected:
                    moved.append(
                        _transition_registry_row(
                            api,
                            row,
                            candidate_state="validated",
                            reason=demote_reason,
                            approved_by=demote_reason,
                        )
                    )
                else:
                    active_remaining.append(row)
            else:
                if jobs_found == 0:
                    moved.append(
                        _transition_registry_row(
                            api,
                            row,
                            candidate_state="validated",
                            reason=demote_reason,
                            approved_by=demote_reason,
                        )
                    )
                else:
                    active_remaining.append(row)
        state["active"] = active_remaining
        state["pending"] = api.unique_sources([*state["pending"], *moved])
        state = api.persist_state_and_auto_sync(state, reason=demote_reason)
        handler.send_json({"demoted": len(moved), "summary": api.summarize_state(state)})
        return True

    if path == "/registry/conflicts/auto-demote-safe":
        action = str(data.get("action") or SAFE_AUTO_DEMOTE_ACTION).strip()
        if action not in SAFE_AUTO_DEMOTE_ACTIONS:
            handler.send_json(
                {"ok": False, "error": "Unsupported safe automation action."}, status=400
            )
            return True

        requested_ids = {
            str(item).strip() for item in as_json_list(data.get("ids")) if str(item).strip()
        }
        source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
        source_state_payload = api.load_json_object(source_state_path, {})
        result = apply_registry_conflict_safe_demotions(
            state,
            source_state_payload,
            action=action,
            ids=sorted(requested_ids),
            now=str(getattr(api, "now_iso", lambda: "")() or ""),
        )
        state = result["state"]
        if result["demoted"]:
            state = api.persist_state_and_auto_sync(state, reason=SAFE_AUTO_DEMOTE_REASON)
        handler.send_json(
            {
                "ok": True,
                "demoted": result["demoted"],
                "skipped": result["skipped"],
                "applied": result["applied"],
                "skippedRows": result["skippedRows"],
                "summary": api.summarize_state(state),
            }
        )
        return True

    if path == "/registry/restore-rejected":
        ids = as_json_list(data.get("ids"))
        moved, remaining = api.move_entries(state["rejected"], [str(item) for item in ids])
        state["rejected"] = remaining
        moved = [
            _transition_registry_row(
                api,
                row,
                candidate_state="validated",
                reason=REGISTRY_REASON_RESTORE_REJECTED,
                approved_by="registry_restore_rejected",
            )
            for row in moved
        ]
        state["pending"] = api.unique_sources([*state["pending"], *moved])
        state = api.persist_state_and_auto_sync(state, reason=REGISTRY_REASON_RESTORE_REJECTED)
        handler.send_json({"restored": len(moved), "summary": api.summarize_state(state)})
        return True

    if path == "/registry/restore-deleted":
        ids = as_json_list(data.get("ids"))
        urls = as_json_list(data.get("urls"))
        selected = {str(item).strip().lower() for item in ids if str(item).strip()}
        selected_urls = {
            api.normalize_source_url(str(item))
            for item in urls
            if api.normalize_source_url(str(item))
        }
        tombstones = api.load_tombstones()
        matched = []
        for source_id, record in list(tombstones.items()):
            if selected and source_id in selected:
                matched.append((source_id, record))
                continue
            if selected_urls and str(record.get("sourceUrlFingerprint") or "") in selected_urls:
                matched.append((source_id, record))
        if not matched:
            handler.send_json({"restored": 0, "summary": api.summarize_state(state)})
            return True
        for source_id, _record in matched:
            row = tombstone_source_row(tombstones.get(source_id))
            if not row:
                continue
            bucket = str(tombstones.get(source_id, {}).get("bucket") or "pending").strip().lower()
            if bucket == "active":
                restored = _transition_registry_row(
                    api,
                    row,
                    candidate_state="live",
                    reason=REGISTRY_REASON_RESTORE_DELETED,
                    approved_by="registry_restore_deleted",
                )
                state["active"] = api.unique_sources([*state["active"], restored])
            elif bucket == "rejected":
                restored = _transition_registry_row(
                    api,
                    row,
                    candidate_state="quarantined",
                    reason=REGISTRY_REASON_RESTORE_DELETED,
                    approved_by="registry_restore_deleted",
                    quarantine_reason=REGISTRY_REASON_RESTORE_DELETED,
                )
                state["rejected"] = api.unique_sources([*state["rejected"], restored])
            else:
                restored = _transition_registry_row(
                    api,
                    row,
                    candidate_state="validated",
                    reason=REGISTRY_REASON_RESTORE_DELETED,
                    approved_by="registry_restore_deleted",
                )
                state["pending"] = api.unique_sources([*state["pending"], restored])
            tombstones, _removed = remove_tombstone(source_id, tombstones)
        api.save_tombstones(tombstones)
        state = api.persist_state_and_auto_sync(state, reason=REGISTRY_REASON_RESTORE_DELETED)
        handler.send_json({"restored": len(matched), "summary": api.summarize_state(state)})
        return True

    if path == "/registry/delete":
        ids = as_json_list(data.get("ids"))
        if not ids:
            ids = as_json_list(data.get("selected"))
        urls = as_json_list(data.get("urls"))
        if not urls:
            urls = as_json_list(data.get("selectedUrls"))
        selected = {str(item).strip().lower() for item in ids if str(item).strip()}
        selected_urls = {
            api.normalize_source_url(str(item))
            for item in urls
            if api.normalize_source_url(str(item))
        }
        if not selected and not selected_urls:
            handler.send_json({"deleted": 0, "summary": api.summarize_state(state)})
            return True
        tombstones = api.load_tombstones()
        deleted_count = 0

        def _is_selected_row(row: dict[str, Any]) -> bool:
            row_id = api.source_identity(row)
            row_url = api.source_url_fingerprint(row)
            return row_id in selected or bool(row_url and row_url in selected_urls)

        next_state: dict[str, list[dict[str, Any]]] = {
            "active": [],
            "pending": [],
            "rejected": [],
        }
        for bucket in ("active", "pending", "rejected"):
            for row in list(state.get(bucket, [])):
                if not isinstance(row, dict):
                    continue
                if _is_selected_row(row):
                    deleted_count += 1
                    tombstones = add_tombstone(
                        row,
                        deleted_at=str(getattr(api, "now_iso", lambda: "")() or ""),
                        deleted_by="registry_manual_delete",
                        reason=REGISTRY_REASON_DELETE,
                        bucket=bucket,
                        tombstones=tombstones,
                    )
                    continue
                next_state[bucket].append(row)
        api.save_tombstones(tombstones)
        state = api.persist_state_and_auto_sync(next_state, reason=REGISTRY_REASON_DELETE)
        handler.send_json({"deleted": deleted_count, "summary": api.summarize_state(state)})
        return True

    if path == "/tasks/run-discovery":

        def _send_discovery() -> None:
            status_code, result = api.trigger_discovery_task(
                payload=data,
                route_name=path,
            )
            safe_bridge_log(
                api,
                "info",
                "discovery_launch_response_sent",
                route=path,
                status=int(status_code),
                started=bool(result.get("started")),
                runId=str(result.get("runId") or ""),
            )
            handler.send_json(result, status=status_code)

        def _error(exc: Exception) -> dict[str, Any]:
            safe_bridge_log(
                api,
                "error",
                "discovery_launch_response_write_failed",
                route=path,
                error=str(exc),
            )
            return {"started": False, "task": "discovery", "error": str(exc)}

        run_route_boundary(handler, _send_discovery, error_status=500, error_payload=_error)
        return True

    if path == "/tasks/run-jobs-pipeline":
        result = api.start_jobs_pipeline_task(data)
        status_code = 200 if bool(result.get("started")) else 409
        handler.send_json(result, status=status_code)
        return True

    if path == "/tasks/jobs-pipeline-schedule":
        send_json_boundary(
            handler,
            lambda: api.update_jobs_pipeline_schedule(data),
            error_status=400,
            error_payload=lambda exc: {
                "ok": False,
                "error": str(exc),
                "savedConfig": api.get_jobs_pipeline_schedule_payload().get("savedConfig", {}),
            },
        )
        return True

    if path == "/tasks/abort":
        status_code, result = api.abort_task(data)
        handler.send_json(result, status=int(status_code or 500))
        return True

    if path in {"/tasks/run-sync-pull", "/tasks/run-sync-push"}:
        sync_action = "pull" if path.endswith("-pull") else "push"

        def _payload() -> dict[str, Any]:
            result = api.start_sync_task(
                sync_action, reason=f"manual_{sync_action}", automatic=False
            )
            status_code = 200 if bool(result.get("started")) else 409
            return {"__status": status_code, "__payload": result}

        def _send() -> None:
            result = _payload()
            handler.send_json(result["__payload"], status=int(result["__status"]))

        run_route_boundary(
            handler,
            _send,
            error_status=500,
            error_payload=lambda exc: {
                "started": False,
                "task": "source_sync",
                "action": sync_action,
                "error": str(exc),
            },
        )
        return True

    if path == "/tasks/run-jobs-bootstrap":

        def _send() -> None:
            result = api.start_jobs_bootstrap_task(data)
            status_code = (
                409
                if bool(result.get("alreadyRunning")) or bool(result.get("alreadyCompleted"))
                else 200
            )
            handler.send_json(result, status=status_code)

        run_route_boundary(
            handler,
            _send,
            error_status=500,
            error_payload=lambda exc: {
                "started": False,
                "task": "jobs_bootstrap",
                "taskType": "fetch",
                "preset": "bootstrap_sheets",
                "coverageScope": "bootstrap_sheets",
                "error": str(exc),
            },
        )
        return True

    if path == "/tasks/run-fetcher":

        def _send() -> None:
            result = api.start_fetcher_task(data)
            status_code = 409 if bool(result.get("alreadyRunning")) else 200
            handler.send_json(result, status=status_code)

        run_route_boundary(
            handler,
            _send,
            error_status=500,
            error_payload=lambda exc: {
                "started": False,
                "task": "jobs_fetcher",
                "error": str(exc),
            },
        )
        return True

    if path == "/discovery/config":

        def _payload() -> dict[str, Any]:
            api.update_saved_discovery_settings(data)
            return api.get_discovery_config_payload()

        send_json_boundary(
            handler,
            _payload,
            error_status=400,
            error_payload=lambda exc: {
                "ok": False,
                "error": str(exc),
                "savedConfig": api.get_discovery_config_payload().get("savedConfig", {}),
            },
        )
        return True

    if path == "/ops/alerts/ack":
        alert_id = str(data.get("id") or "").strip()
        if not alert_id:
            handler.send_json({"error": "Missing alert id"}, status=400)
            return True
        dashboard_health_fn = getattr(api, "compute_ops_dashboard_health", None)
        health = (
            dashboard_health_fn() if callable(dashboard_health_fn) else api.compute_ops_health()
        )
        active_alert = next(
            (
                row
                for row in json_object_rows(health.get("alerts"))
                if str(row.get("id") or "").strip() == alert_id
            ),
            None,
        )
        if active_alert is not None and not bool(active_alert.get("dismissible", True)):
            handler.send_json({"acked": alert_id, "ignored": True, "ok": True})
            return True
        state_alert = api.load_alert_state()
        acked = copy_json_object(state_alert.get("acked"))
        acked[alert_id] = api.now_iso()
        api.save_alert_state({"acked": acked})
        handler.send_json({"acked": alert_id, "ok": True})
        return True

    if path == "/sync/config":

        def _payload() -> dict[str, Any]:
            api.update_saved_sync_settings(data)
            return api.get_sync_status_payload()

        send_json_boundary(
            handler,
            _payload,
            error_status=400,
            error_payload=lambda exc: {
                "ok": False,
                "error": str(exc),
                "config": api.sync_config_status(),
            },
        )
        return True

    if path == "/sync/test":

        def _error(exc: Exception) -> dict[str, Any]:
            api.set_sync_status(
                action="test", result="error", error=str(exc), pulled=False, pushed=False
            )
            return {"ok": False, "error": str(exc), "config": api.sync_config_status()}

        send_json_boundary(
            handler,
            api.test_sync_config,
            error_status=500,
            error_payload=_error,
        )
        return True

    if path in {"/sync/pull", "/sync/push"}:
        sync_action = path.removeprefix("/sync/")

        def _error(exc: Exception) -> dict[str, Any]:
            if sync_action == "pull":
                api.set_sync_status(
                    action=sync_action, result="error", error=str(exc), pulled=False
                )
            else:
                api.set_sync_status(
                    action=sync_action, result="error", error=str(exc), pushed=False
                )
            return {"ok": False, "error": str(exc), "config": api.sync_config_status()}

        send_json_boundary(
            handler,
            api.sync_pull_sources if sync_action == "pull" else api.sync_push_sources,
            error_status=500,
            error_payload=_error,
        )
        return True

    return False
