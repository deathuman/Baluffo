"""Background direct-link availability checks for bridge routes."""

from __future__ import annotations

import json
import threading
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.bridge.task_launch_jobs_feed import (
    JobsFeedContext,
    JobsFeedReconciliationSnapshot,
    jobs_feed_reconciliation_transaction,
    reconcile_jobs_feed_availability,
    rollback_jobs_feed_reconciliation,
)
from src.jobs.availability_validator import DirectLinkValidator
from src.jobs.common.datetime_utils import parse_datetime
from src.jobs.state_lifecycle import (
    apply_direct_availability_evidence,
    build_availability_history_payload,
    read_job_lifecycle_state,
    write_job_lifecycle_state,
)
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import read_json
from src.shared.utils import now_iso


class JobAvailabilityService:
    def __init__(
        self,
        *,
        data_dir: Path,
        local_store_factory: Callable[[], Any],
        validator: DirectLinkValidator | None = None,
        enforce_direct: bool = False,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.local_store_factory = local_store_factory
        self.validator = validator or DirectLinkValidator()
        self.enforce_direct = bool(enforce_direct)
        self._lock = threading.RLock()
        # File-backed evidence application is serialized independently from
        # in-memory run bookkeeping so status/start remain responsive while a
        # worker waits on a reconciliation transaction.
        self._apply_lock = threading.Lock()
        self._runs: dict[str, dict[str, Any]] = {}
        self._active_by_availability_id: dict[str, str] = {}
        self._sweep_lock = threading.Lock()
        self._sweep_thread: threading.Thread | None = None
        self._sweep_wakeup = threading.Event()

    @property
    def lifecycle_path(self) -> Path:
        return self.data_dir / "jobs-lifecycle-state.json"

    @property
    def history_path(self) -> Path:
        return self.data_dir / "jobs-availability-history.json"

    @property
    def custom_lifecycle_path(self) -> Path:
        return self.data_dir / "local-user-data" / "jobs-custom-availability-state.json"

    @property
    def priority_manifest_path(self) -> Path:
        return self.data_dir / "jobs-availability-priority.json"

    def _bounded(self, row: dict[str, Any]) -> dict[str, Any]:
        result = row.get("result") if isinstance(row.get("result"), dict) else {}
        return {
            "runId": str(row.get("runId") or ""),
            "availabilityId": str(row.get("availabilityId") or ""),
            "status": str(row.get("status") or ""),
            "startedAt": str(row.get("startedAt") or ""),
            "completedAt": str(row.get("completedAt") or ""),
            "result": {
                key: value
                for key, value in result.items()
                if key
                in {
                    "availabilityStatus",
                    "availabilityCheckedAt",
                    "availabilityEvidence",
                    "classification",
                    "enforced",
                    "applied",
                }
            },
        }

    def status(self, run_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._runs.get(str(run_id or ""))
            if not row:
                return {"ok": False, "error": "availability_run_not_found"}
            return {"ok": True, **self._bounded(row)}

    def prepare_priority_manifest(self) -> dict[str, Any]:
        store = self.local_store_factory()
        builder = getattr(store, "build_availability_priority_manifest", None)
        if callable(builder):
            payload = builder()
        else:
            existing = read_json(self.priority_manifest_path, {})
            payload = existing if isinstance(existing, dict) else {"schemaVersion": 2, "rows": []}
        with jobs_feed_reconciliation_transaction(self.data_dir):
            write_atomic_if_changed(
                self.priority_manifest_path,
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            )
            custom_ids = {
                str(row.get("availabilityId") or "")
                for row in payload.get("rows") or []
                if isinstance(row, dict) and str(row.get("scope") or "") == "custom_saved"
            }
            existing_custom = read_job_lifecycle_state(self.custom_lifecycle_path)
            retained_custom = {
                key: entry
                for key, entry in existing_custom.items()
                if str(entry.get("availabilityId") or key) in custom_ids
            }
            if retained_custom != existing_custom:
                write_job_lifecycle_state(self.custom_lifecycle_path, retained_custom)
        return payload

    @staticmethod
    def _custom_manifest_entry(
        manifest: dict[str, Any], availability_id: str
    ) -> dict[str, Any] | None:
        rows = manifest.get("rows") if isinstance(manifest, dict) else []
        return next(
            (
                dict(row)
                for row in (rows if isinstance(rows, list) else [])
                if isinstance(row, dict)
                and str(row.get("availabilityId") or "") == availability_id
                and str(row.get("scope") or "") == "custom_saved"
            ),
            None,
        )

    def start_overdue_catchup(self, *, limit: int = 10) -> dict[str, Any]:
        lifecycle = read_job_lifecycle_state(self.lifecycle_path)
        candidates = sorted(
            (
                entry
                for entry in lifecycle.values()
                if str(entry.get("availabilityStatus") or "") == "verification_overdue"
                and str(entry.get("availabilityId") or "")
                and str(entry.get("jobLink") or "")
            ),
            key=lambda entry: str(
                entry.get("availabilityCheckedAt") or entry.get("availabilityVerifiedAt") or ""
            ),
        )
        started = 0
        for entry in candidates[: max(0, min(50, int(limit)))]:
            if self.start({"availabilityId": entry.get("availabilityId")}).get("started"):
                started += 1
        return {"eligible": len(candidates), "started": started, "limit": int(limit)}

    def _drain_sweep(
        self, rows: list[dict[str, Any]], active_run_ids: set[str], *, max_concurrent: int
    ) -> None:
        pending = list(rows)
        try:
            while pending or active_run_ids:
                while pending and len(active_run_ids) < max_concurrent:
                    row = pending.pop(0)
                    result = self.start({"availabilityId": row.get("availabilityId")})
                    run_id = str(result.get("runId") or "")
                    if result.get("started") and run_id:
                        active_run_ids.add(run_id)
                with self._lock:
                    active_run_ids = {
                        run_id
                        for run_id in active_run_ids
                        if str((self._runs.get(run_id) or {}).get("status") or "") == "running"
                    }
                if pending or active_run_ids:
                    self._sweep_wakeup.wait(0.1)
                    self._sweep_wakeup.clear()
        finally:
            with self._sweep_lock:
                self._sweep_thread = None

    def start_sweep_from_plan(
        self, *, limit: int | None = None, max_concurrent: int = 4
    ) -> dict[str, Any]:
        plan = read_json(self.data_dir / "jobs-availability-sweep-plan.json", {})
        rows = plan.get("rows") if isinstance(plan, dict) else []
        safe_rows = [dict(row) for row in rows if isinstance(row, dict)]
        safe_limit = len(safe_rows) if limit is None else max(0, min(1000, int(limit)))
        selected = safe_rows[:safe_limit]
        concurrency = max(1, min(8, int(max_concurrent)))
        with self._sweep_lock:
            if self._sweep_thread is not None and self._sweep_thread.is_alive():
                return {
                    "planned": len(safe_rows),
                    "started": 0,
                    "queued": 0,
                    "limit": safe_limit,
                    "reused": True,
                }
            active_run_ids: set[str] = set()
            remaining = list(selected)
            started = 0
            while remaining and len(active_run_ids) < concurrency:
                row = remaining.pop(0)
                result = self.start({"availabilityId": row.get("availabilityId")})
                run_id = str(result.get("runId") or "")
                if result.get("started") and run_id:
                    active_run_ids.add(run_id)
                    started += 1
            queued = len(remaining)
            if remaining or active_run_ids:
                self._sweep_thread = threading.Thread(
                    target=self._drain_sweep,
                    args=(remaining, active_run_ids),
                    kwargs={"max_concurrent": concurrency},
                    daemon=True,
                    name="baluffo-availability-sweep",
                )
                self._sweep_thread.start()
        return {
            "planned": len(safe_rows),
            "started": started,
            "queued": queued,
            "limit": safe_limit,
        }

    def project_published_transitions(self) -> int:
        lifecycle = read_job_lifecycle_state(self.lifecycle_path)
        entries = [dict(entry) for entry in lifecycle.values() if isinstance(entry, dict)]
        store = self.local_store_factory()
        batch_projector = getattr(store, "project_availability_transitions", None)
        if callable(batch_projector):
            return int(batch_projector(entries) or 0)
        projector = getattr(store, "project_availability_transition", None)
        if not callable(projector):
            return 0
        return sum(int(projector(entry) or 0) for entry in entries)

    def post_pipeline_publication(self, completion: dict[str, Any] | None = None) -> dict[str, Any]:
        projected = 0
        identity_migration_error = ""
        projection_error = ""
        identity_migration: dict[str, int] = {"rebound": 0, "unmonitored": 0}
        try:
            store = self.local_store_factory()
            migrator = getattr(store, "reconcile_repaired_availability_identities", None)
            if callable(migrator):
                result = migrator()
                if isinstance(result, dict):
                    identity_migration = {
                        "rebound": int(result.get("rebound") or 0),
                        "unmonitored": int(result.get("unmonitored") or 0),
                    }
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            identity_migration_error = type(exc).__name__
        try:
            projected = self.project_published_transitions()
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            projection_error = type(exc).__name__
        sweep = self.start_sweep_from_plan()
        return {
            "runId": str((completion or {}).get("runId") or ""),
            "projected": projected,
            "identityMigration": identity_migration,
            **(
                {"identityMigrationError": identity_migration_error}
                if identity_migration_error
                else {}
            ),
            **({"projectionError": projection_error} if projection_error else {}),
            "sweep": sweep,
        }

    def _record_shadow_result(
        self, availability_id: str, evidence: dict[str, Any], completed_at: str
    ) -> None:
        path = self.data_dir / "jobs-availability-shadow-results.json"
        payload = read_json(path, {})
        rows = (payload.get("rows") or []) if isinstance(payload, dict) else []
        safe_rows = [dict(row) for row in rows if isinstance(row, dict)][-499:]
        safe_rows.append(
            {
                "availabilityId": availability_id,
                "checkedAt": completed_at,
                "kind": str(evidence.get("kind") or ""),
                "confidence": str(evidence.get("confidence") or ""),
                **(
                    {"httpStatus": int(evidence["httpStatus"])}
                    if isinstance(evidence.get("httpStatus"), int)
                    else {}
                ),
            }
        )
        write_atomic_if_changed(
            path,
            json.dumps(
                {"schemaVersion": 1, "updatedAt": completed_at, "rows": safe_rows},
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )

    def _record_direct_checkpoint(
        self, availability_id: str, evidence: dict[str, Any], completed_at: str
    ) -> None:
        path = self.data_dir / "jobs-availability-direct-checkpoints.json"
        checked_at = str(evidence.get("checkedAt") or completed_at)
        with jobs_feed_reconciliation_transaction(self.data_dir):
            payload = read_json(path, {})
            rows = (payload.get("rows") or []) if isinstance(payload, dict) else []
            by_id = {
                str(row.get("availabilityId") or ""): dict(row)
                for row in rows
                if isinstance(row, dict) and str(row.get("availabilityId") or "")
            }
            previous = by_id.get(availability_id) or {}
            previous_at = str(previous.get("checkedAt") or "")
            checked_dt = parse_datetime(checked_at)
            previous_dt = parse_datetime(previous_at)
            if (checked_dt and previous_dt and checked_dt < previous_dt) or (
                not checked_dt and not previous_dt and checked_at < previous_at
            ):
                return
            by_id[availability_id] = {
                "availabilityId": availability_id,
                "checkedAt": checked_at,
                "kind": str(evidence.get("kind") or ""),
                "confidence": str(evidence.get("confidence") or ""),
            }
            safe_rows = sorted(
                by_id.values(), key=lambda row: str(row.get("checkedAt") or ""), reverse=True
            )[:20_000]
            write_atomic_if_changed(
                path,
                json.dumps(
                    {"schemaVersion": 1, "updatedAt": completed_at, "rows": safe_rows},
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            )

    def _prepare_target(self, availability_id: str) -> tuple[str, str, Path, dict[str, Any]]:
        """Resolve the lifecycle target inside the worker thread.

        Lifecycle artifacts can be large (the canonical state is routinely tens
        of megabytes), so this lookup must not block the bridge POST route.
        """
        lifecycle = read_job_lifecycle_state(self.lifecycle_path)
        match = next(
            (
                (key, entry)
                for key, entry in lifecycle.items()
                if str(entry.get("availabilityId") or "") == availability_id
            ),
            None,
        )
        scope = "canonical"
        state_path = self.lifecycle_path
        if match:
            lifecycle_key, entry = match
        else:
            try:
                custom = self._custom_manifest_entry(
                    self.prepare_priority_manifest(), availability_id
                )
            except (OSError, RuntimeError, TypeError, ValueError):
                custom = None
            if not custom:
                raise ValueError("availability_id_not_found")
            scope = "custom_saved"
            state_path = self.custom_lifecycle_path
            lifecycle_key = availability_id
            private_lifecycle = read_job_lifecycle_state(state_path)
            entry = dict(private_lifecycle.get(lifecycle_key) or {})
            entry.update(
                {
                    "availabilityId": availability_id,
                    "availabilityStatus": str(entry.get("availabilityStatus") or "available"),
                    "status": str(entry.get("status") or "active"),
                    "jobLink": str(custom.get("jobLink") or ""),
                    "source": "custom_saved",
                }
            )
        if not str(entry.get("jobLink") or "").strip():
            raise ValueError("public_job_link_required")
        return lifecycle_key, scope, state_path, entry

    def start(self, payload: dict[str, Any] | None) -> dict[str, Any]:
        availability_id = str((payload or {}).get("availabilityId") or "").strip()
        if not availability_id:
            return {"started": False, "error": "availability_id_required"}
        with self._lock:
            active_run_id = self._active_by_availability_id.get(availability_id, "")
            active_row = self._runs.get(active_run_id) if active_run_id else None
            if active_row and str(active_row.get("status") or "") == "running":
                return {
                    "started": True,
                    "runId": active_run_id,
                    "availabilityId": availability_id,
                    "reused": True,
                }
            run_id = f"availability_{uuid.uuid4().hex[:16]}"
            row = {
                "runId": run_id,
                "availabilityId": availability_id,
                "status": "running",
                "startedAt": now_iso(),
                "completedAt": "",
                "result": {},
            }
            self._runs[run_id] = row
            self._active_by_availability_id[availability_id] = run_id
            if len(self._runs) > 200:
                for stale_id in list(self._runs)[:-200]:
                    self._runs.pop(stale_id, None)
        threading.Thread(
            target=self._run,
            args=(run_id, availability_id),
            daemon=True,
            name=f"baluffo-availability-{run_id[-8:]}",
        ).start()
        return {"started": True, "runId": run_id, "availabilityId": availability_id}

    @staticmethod
    def _evidence_is_stale(entry: dict[str, Any], evidence: dict[str, Any]) -> bool:
        current_at = parse_datetime(entry.get("availabilityCheckedAt"))
        evidence_at = parse_datetime(evidence.get("checkedAt"))
        return bool(current_at and evidence_at and evidence_at < current_at)

    def _rewrite_feeds(
        self, availability_id: str, entry: dict[str, Any]
    ) -> tuple[JobsFeedContext, JobsFeedReconciliationSnapshot]:
        context = JobsFeedContext(
            data_dir=self.data_dir,
            jobs_fetch_report=self.data_dir / "jobs-fetch-report.json",
            now_iso=now_iso,
            bridge_log=lambda *_args, **_kwargs: None,
            save_json_atomic=lambda path, payload: write_atomic_if_changed(
                path, json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            ),
        )
        snapshot = reconcile_jobs_feed_availability(
            context, availability_id=availability_id, entry=entry
        )
        if not isinstance(snapshot, JobsFeedReconciliationSnapshot):
            raise RuntimeError("jobs feed availability reconciliation failed")
        return context, snapshot

    def _project_saved(self, entry: dict[str, Any]) -> None:
        try:
            store = self.local_store_factory()
        except (OSError, RuntimeError, TypeError, ValueError):
            return
        projector = getattr(store, "project_availability_transition", None)
        if callable(projector):
            projector(entry)

    def _restore_profile_reports_for_live(
        self, availability_id: str, evidence: dict[str, Any]
    ) -> None:
        if not (
            str(evidence.get("kind") or "") == "direct_live"
            and str(evidence.get("confidence") or "") == "definitive"
        ):
            return
        try:
            store = self.local_store_factory()
        except (OSError, RuntimeError, TypeError, ValueError):
            return
        restore = getattr(store, "restore_reported_jobs_for_live", None)
        if callable(restore):
            restore(
                availability_id,
                checked_at=str(evidence.get("checkedAt") or now_iso()),
            )

    def _commit_direct_evidence(
        self,
        *,
        lifecycle_key: str,
        scope: str,
        state_path: Path,
        fallback_entry: dict[str, Any],
        evidence: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        completed_at = now_iso()
        with jobs_feed_reconciliation_transaction(self.data_dir):
            if scope == "custom_saved" and not self._custom_manifest_entry(
                read_json(self.priority_manifest_path, {}),
                str(fallback_entry.get("availabilityId") or ""),
            ):
                return fallback_entry, False
            latest = read_job_lifecycle_state(state_path)
            current_entry = dict(latest.get(lifecycle_key) or fallback_entry)
            if self._evidence_is_stale(current_entry, evidence):
                return current_entry, False
            next_entry = apply_direct_availability_evidence(current_entry, evidence)
            previous_lifecycle = dict(latest)
            previous_history = (
                self.history_path.read_text(encoding="utf-8")
                if scope == "canonical" and self.history_path.exists()
                else None
            )
            feed_reconciliation = None
            try:
                if scope == "canonical":
                    feed_reconciliation = self._rewrite_feeds(
                        str(next_entry.get("availabilityId") or ""), next_entry
                    )
                latest[lifecycle_key] = next_entry
                write_job_lifecycle_state(state_path, latest)
                if scope == "canonical":
                    history = build_availability_history_payload(latest, finished_at=completed_at)
                    write_atomic_if_changed(
                        self.history_path,
                        json.dumps(history, ensure_ascii=False, separators=(",", ":")),
                    )
            except (OSError, RuntimeError, TypeError, ValueError):
                try:
                    if feed_reconciliation is not None:
                        rollback_jobs_feed_reconciliation(*feed_reconciliation)
                finally:
                    write_job_lifecycle_state(state_path, previous_lifecycle)
                    if scope == "canonical":
                        if previous_history is None:
                            self.history_path.unlink(missing_ok=True)
                        else:
                            write_atomic_if_changed(self.history_path, previous_history)
                raise
        return next_entry, True

    def _apply_checked_evidence(
        self,
        *,
        lifecycle_key: str,
        scope: str,
        state_path: Path,
        current_entry: dict[str, Any],
        evidence: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        stale_evidence = self._evidence_is_stale(current_entry, evidence)
        self._record_direct_checkpoint(
            str(current_entry.get("availabilityId") or ""), evidence, now_iso()
        )
        if not self.enforce_direct:
            self._record_shadow_result(
                str(current_entry.get("availabilityId") or ""), evidence, now_iso()
            )
            return current_entry, False
        if stale_evidence:
            return current_entry, False
        return self._commit_direct_evidence(
            lifecycle_key=lifecycle_key,
            scope=scope,
            state_path=state_path,
            fallback_entry=current_entry,
            evidence=evidence,
        )

    def _run(
        self,
        run_id: str,
        availability_id: str,
    ) -> None:
        lifecycle_key = ""
        scope = "canonical"
        state_path = self.lifecycle_path
        initial_entry: dict[str, Any] = {}
        try:
            lifecycle_key, scope, state_path, initial_entry = self._prepare_target(availability_id)
            # `_prepare_target` already captured the authoritative target. Do not
            # re-read the large lifecycle artifact while holding the service lock;
            # duplicate admission and status reads must stay prompt.
            entry = dict(initial_entry)
            availability_id = str(entry.get("availabilityId") or availability_id)
            evidence = self.validator.check(str(entry.get("jobLink") or ""))
            # Refresh outside the lock before the minimal synchronized mutation.
            latest = read_job_lifecycle_state(state_path)
            current_entry = dict(latest.get(lifecycle_key) or entry)
            with self._apply_lock:
                next_entry, applied = self._apply_checked_evidence(
                    lifecycle_key=lifecycle_key,
                    scope=scope,
                    state_path=state_path,
                    current_entry=current_entry,
                    evidence=evidence,
                )
            if applied:
                self._project_saved(next_entry)
                self._restore_profile_reports_for_live(availability_id, evidence)
            completed_at = now_iso()
            result = {
                "availabilityStatus": str(next_entry.get("availabilityStatus") or ""),
                "availabilityCheckedAt": str(evidence.get("checkedAt") or completed_at),
                "availabilityEvidence": dict(evidence),
                "classification": str(evidence.get("kind") or ""),
                "enforced": self.enforce_direct,
                "applied": applied,
            }
            status = "succeeded"
        except Exception:  # Keep the run terminal and release duplicate-admission state.
            completed_at = now_iso()
            result = {
                "availabilityStatus": "",
                "availabilityCheckedAt": completed_at,
                "availabilityEvidence": {
                    "kind": "check_failed",
                    "confidence": "unknown",
                    "checkedAt": completed_at,
                    "source": "direct_link",
                },
                "applied": False,
            }
            status = "failed"
        with self._lock:
            row = self._runs.get(run_id, {})
            row.update({"status": status, "completedAt": completed_at, "result": result})
            self._runs[run_id] = row
            if self._active_by_availability_id.get(availability_id) == run_id:
                self._active_by_availability_id.pop(availability_id, None)
        self._sweep_wakeup.set()
