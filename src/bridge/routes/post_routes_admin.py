from __future__ import annotations

from typing import Any

from src.bridge.api import BridgeApi
from src.bridge.registry_tombstones import (
    add_tombstone,
    load_tombstones,
    remove_tombstone,
    save_tombstones,
    tombstone_source_row,
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


def _transition_registry_row(
    api: BridgeApi,
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


def handle_post(handler: Any, *, api: BridgeApi, path: str, payload: Any) -> bool:
    state = api.load_state()

    if path == "/sources/manual":
        result = api.add_manual_source(str((payload or {}).get("url") or ""))
        handler._send_json(result)  # noqa: SLF001
        return True

    if path == "/discovery/check-source":
        result = api.trigger_source_check(str((payload or {}).get("sourceId") or ""))
        status = 200 if bool(result.get("started")) else 400
        handler._send_json(result, status=status)  # noqa: SLF001
        return True

    if path == "/registry/approve":
        ids = (payload or {}).get("ids") if isinstance((payload or {}).get("ids"), list) else []
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
        handler._send_json({"approved": len(moved), "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/registry/reject":
        ids = (payload or {}).get("ids") if isinstance((payload or {}).get("ids"), list) else []
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
        handler._send_json({"rejected": len(moved), "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/registry/rollback":
        ids = (payload or {}).get("ids") if isinstance((payload or {}).get("ids"), list) else []
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
        handler._send_json({"rolledBack": len(moved), "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/registry/demote-active":
        ids = (payload or {}).get("ids") if isinstance((payload or {}).get("ids"), list) else []
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
        handler._send_json({"demoted": len(moved), "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/registry/restore-rejected":
        ids = (payload or {}).get("ids") if isinstance((payload or {}).get("ids"), list) else []
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
        handler._send_json({"restored": len(moved), "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/registry/restore-deleted":
        ids = (payload or {}).get("ids") if isinstance((payload or {}).get("ids"), list) else []
        urls = (payload or {}).get("urls") if isinstance((payload or {}).get("urls"), list) else []
        selected = {str(item).strip().lower() for item in ids if str(item).strip()}
        selected_urls = {
            api.normalize_source_url(str(item))
            for item in urls
            if api.normalize_source_url(str(item))
        }
        tombstones = load_tombstones()
        matched = []
        for source_id, record in list(tombstones.items()):
            if selected and source_id in selected:
                matched.append((source_id, record))
                continue
            if selected_urls and str(record.get("sourceUrlFingerprint") or "") in selected_urls:
                matched.append((source_id, record))
        if not matched:
            handler._send_json({"restored": 0, "summary": api.summarize_state(state)})  # noqa: SLF001
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
        save_tombstones(tombstones)
        state = api.persist_state_and_auto_sync(state, reason=REGISTRY_REASON_RESTORE_DELETED)
        handler._send_json({"restored": len(matched), "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/registry/delete":
        ids = (payload or {}).get("ids") if isinstance((payload or {}).get("ids"), list) else []
        if not ids and isinstance((payload or {}).get("selected"), list):
            ids = list((payload or {}).get("selected") or [])
        urls = (payload or {}).get("urls") if isinstance((payload or {}).get("urls"), list) else []
        if not urls and isinstance((payload or {}).get("selectedUrls"), list):
            urls = list((payload or {}).get("selectedUrls") or [])
        selected = {str(item).strip().lower() for item in ids if str(item).strip()}
        selected_urls = {
            api.normalize_source_url(str(item))
            for item in urls
            if api.normalize_source_url(str(item))
        }
        if not selected and not selected_urls:
            handler._send_json({"deleted": 0, "summary": api.summarize_state(state)})  # noqa: SLF001
            return True
        tombstones = load_tombstones()
        deleted_count = 0

        def _is_selected_row(row: dict[str, Any]) -> bool:
            row_id = api.source_identity(row)
            row_url = api.source_url_fingerprint(row)
            return row_id in selected or bool(row_url and row_url in selected_urls)

        next_state = {"active": [], "pending": [], "rejected": []}
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
        save_tombstones(tombstones)
        state = api.persist_state_and_auto_sync(next_state, reason=REGISTRY_REASON_DELETE)
        handler._send_json({"deleted": deleted_count, "summary": api.summarize_state(state)})  # noqa: SLF001
        return True

    if path == "/tasks/run-discovery":
        try:
            status_code, result = api.trigger_discovery_task(
                payload=payload if isinstance(payload, dict) else {},
                route_name=path,
            )
            try:
                api.bridge_log(
                    "info",
                    "discovery_launch_response_sent",
                    route=path,
                    status=int(status_code),
                    started=bool((result or {}).get("started")),
                    runId=str((result or {}).get("runId") or ""),
                )
            except Exception:  # noqa: BLE001
                pass
            handler._send_json(result, status=status_code)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            try:
                api.bridge_log(
                    "error",
                    "discovery_launch_response_write_failed",
                    route=path,
                    error=str(exc),
                )
            except Exception:  # noqa: BLE001
                pass
            handler._send_json(
                {"started": False, "task": "discovery", "error": str(exc)}, status=500
            )  # noqa: SLF001,E501
        return True

    if path == "/tasks/run-jobs-pipeline":
        result = api.start_jobs_pipeline_task(payload if isinstance(payload, dict) else {})
        status_code = 200 if bool(result.get("started")) else 409
        handler._send_json(result, status=status_code)  # noqa: SLF001
        return True

    if path == "/tasks/run-sync-pull":
        try:
            result = api.start_sync_task("pull", reason="manual_pull", automatic=False)
            status_code = 200 if bool(result.get("started")) else 409
            handler._send_json(result, status=status_code)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json(
                {"started": False, "task": "source_sync", "action": "pull", "error": str(exc)},
                status=500,
            )  # noqa: SLF001,E501
        return True

    if path == "/tasks/run-sync-push":
        try:
            result = api.start_sync_task("push", reason="manual_push", automatic=False)
            status_code = 200 if bool(result.get("started")) else 409
            handler._send_json(result, status=status_code)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json(
                {"started": False, "task": "source_sync", "action": "push", "error": str(exc)},
                status=500,
            )  # noqa: SLF001,E501
        return True

    if path == "/tasks/run-fetcher":
        try:
            result = api.start_fetcher_task(payload if isinstance(payload, dict) else {})
            status_code = 409 if bool((result or {}).get("alreadyRunning")) else 200
            handler._send_json(result, status=status_code)  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json(
                {"started": False, "task": "jobs_fetcher", "error": str(exc)}, status=500
            )  # noqa: SLF001,E501
        return True

    if path == "/discovery/config":
        try:
            api.update_saved_discovery_settings(payload if isinstance(payload, dict) else {})
            handler._send_json(api.get_discovery_config_payload())  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json(
                {
                    "ok": False,
                    "error": str(exc),
                    "savedConfig": api.get_discovery_config_payload().get("savedConfig", {}),
                },
                status=400,
            )  # noqa: SLF001,E501
        return True

    if path == "/ops/alerts/ack":
        alert_id = str((payload or {}).get("id") or "").strip()
        if not alert_id:
            handler._send_json({"error": "Missing alert id"}, status=400)  # noqa: SLF001
            return True
        health = api.compute_ops_health()
        active_alert = next(
            (
                row
                for row in (health.get("alerts") or [])
                if str((row or {}).get("id") or "").strip() == alert_id
            ),
            None,
        )
        if active_alert is not None and not bool(active_alert.get("dismissible", True)):
            handler._send_json({"acked": alert_id, "ignored": True, "ok": True})  # noqa: SLF001
            return True
        state_alert = api.load_alert_state()
        acked = dict(state_alert.get("acked") or {})
        acked[alert_id] = api.now_iso()
        api.save_alert_state({"acked": acked})
        handler._send_json({"acked": alert_id, "ok": True})  # noqa: SLF001
        return True

    if path == "/sync/config":
        try:
            api.update_saved_sync_settings(payload if isinstance(payload, dict) else {})
            handler._send_json(api.get_sync_status_payload())  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            handler._send_json(
                {"ok": False, "error": str(exc), "config": api.sync_config_status()}, status=400
            )  # noqa: SLF001
        return True

    if path == "/sync/test":
        try:
            handler._send_json(api.test_sync_config())  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            api.set_sync_status(
                action="test", result="error", error=str(exc), pulled=False, pushed=False
            )
            handler._send_json(
                {"ok": False, "error": str(exc), "config": api.sync_config_status()}, status=500
            )  # noqa: SLF001
        return True

    if path == "/sync/pull":
        try:
            handler._send_json(api.sync_pull_sources())  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            api.set_sync_status(action="pull", result="error", error=str(exc), pulled=False)
            handler._send_json(
                {"ok": False, "error": str(exc), "config": api.sync_config_status()}, status=500
            )  # noqa: SLF001
        return True

    if path == "/sync/push":
        try:
            handler._send_json(api.sync_push_sources())  # noqa: SLF001
        except Exception as exc:  # noqa: BLE001
            api.set_sync_status(action="push", result="error", error=str(exc), pushed=False)
            handler._send_json(
                {"ok": False, "error": str(exc), "config": api.sync_config_status()}, status=500
            )  # noqa: SLF001
        return True

    return False
