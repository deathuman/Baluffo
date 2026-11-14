"""SQLite per-source fetch run storage APIs.

AI boundary owns: per-source fetch run rows, source summaries, and persisted fetch-run evidence.
AI boundary implement in: this file for source run storage operations; fetch execution and route payloads stay outside storage.
AI boundary search before contracts: fetch report normalization, source run routes, migrations, and source runtime tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source runtime storage tests.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from src.shared.coerce import as_dict as _coerce_as_dict
from src.shared.json_io import json_dumps, loads_object
from src.shared.text_utils import clean_text as _shared_clean_text
from src.shared.utils import coerce_non_negative_int as _coerce_int
from src.shared.utils import now_iso as _shared_now_iso
from src.shared.utils import parse_iso as _parse_iso
from src.storage.baluffo_store import DEFAULT_BATCH_SIZE, BaluffoStore
from src.storage.evidence_archive import EvidenceArchiveStore
from src.storage.retention_cancel import (
    CancelRequested,
    raise_if_cancelled,
)

SOURCE_RUN_SCHEMA_VERSION = 1
DEFAULT_SOURCE_RUN_LIMIT = 500
DEFAULT_RUNTIME_RETENTION_DAYS = 30
DEFAULT_RUNTIME_RETENTION_KEEP_RUNS = 30
DEFAULT_RUNTIME_RETENTION_MAX_RUNS = 200
DEFAULT_RUNTIME_RETENTION_BATCH_RUNS = 10
DEFAULT_RUNTIME_RETENTION_MAX_EVENT_ROWS = 5_000
DEFAULT_RUNTIME_RETENTION_MAX_SYNC_RUNS = 500
DEFAULT_ARCHIVE_COMPACT_RUNS = 30
DEFAULT_ARCHIVE_COMPACT_ROWS = 200
DEFAULT_ARCHIVE_COMPACT_BYTES = 32 * 1024 * 1024


def _now_iso() -> str:
    return _shared_now_iso()


_clean_text = _shared_clean_text


def _norm_token(value: Any) -> str:
    return _clean_text(value).lower()


_json_object = _coerce_as_dict


def _json_dumps(value: Any) -> str:
    return json_dumps(value)


def _json_loads_object(value: Any) -> dict[str, Any]:
    return loads_object(value)


def _source_key(row: Mapping[str, Any], ordinal: int) -> str:
    raw = (
        _clean_text(row.get("sourceKey"))
        or _clean_text(row.get("sourceId"))
        or _clean_text(row.get("id"))
        or _clean_text(row.get("name"))
        or f"source_{ordinal + 1}"
    )
    key = re.sub(r"\s+", "_", raw.lower())
    return key[:240] or f"source_{ordinal + 1}"


def _source_id(source_key: str) -> str:
    return f"fetch:{source_key}"


def _emit_progress(
    on_progress: Callable[[dict[str, Any]], None] | None,
    payload: dict[str, Any],
) -> None:
    if on_progress is not None:
        on_progress(payload)


def _delete_source_run_batches(
    store: BaluffoStore,
    selected: Sequence[tuple[str, datetime]],
    *,
    candidate_run_count: int,
    on_progress: Callable[[dict[str, Any]], None] | None,
    cancel_requested: CancelRequested | None = None,
) -> tuple[int, int]:
    deleted_run_count = 0
    deleted_row_count = 0
    for start in range(0, len(selected), DEFAULT_RUNTIME_RETENTION_BATCH_RUNS):
        raise_if_cancelled(cancel_requested)
        batch = selected[start : start + DEFAULT_RUNTIME_RETENTION_BATCH_RUNS]
        batch_ids = [run_id for run_id, _ended_at in batch]

        def delete_batch(conn: Any, run_ids: list[str] = batch_ids) -> int:
            deleted = 0
            for run_id in run_ids:
                cursor = conn.execute("DELETE FROM source_runs WHERE run_id = ?", (run_id,))
                deleted += max(0, int(cursor.rowcount or 0))
            return deleted

        deleted_row_count += int(store.write(delete_batch) or 0)
        deleted_run_count += len(batch)
        _emit_progress(
            on_progress,
            {
                "phase": "prune-source-runs",
                "stage": "deleting",
                "candidateRunCount": candidate_run_count,
                "processedRunCount": deleted_run_count,
                "totalRunCount": len(selected),
                "deletedRunCount": deleted_run_count,
                "deletedRowCount": deleted_row_count,
            },
        )
    return deleted_run_count, deleted_row_count


def _source_run_record(
    run_id: str,
    row: Mapping[str, Any],
    *,
    ordinal: int,
    evidence_ref: Mapping[str, Any],
    now_iso: Callable[[], str],
) -> dict[str, Any]:
    source_key = _source_key(row, ordinal)
    source_name = _clean_text(row.get("name")) or source_key
    status = _norm_token(row.get("status")) or "error"
    fetched_count = _coerce_int(row.get("fetchedCount"))
    kept_count = _coerce_int(row.get("keptCount"))
    failed_count = _coerce_int(row.get("failedCount"))
    if failed_count == 0 and status == "error":
        failed_count = 1
    payload = dict(row)
    payload.setdefault("sourceKey", source_key)
    payload.setdefault("name", source_name)
    return {
        "schema_version": SOURCE_RUN_SCHEMA_VERSION,
        "run_id": _clean_text(run_id),
        "source_id": _source_id(source_key),
        "source_key": source_key,
        "source_name": source_name,
        "ordinal": max(0, int(ordinal)),
        "status": status,
        "started_at": _clean_text(row.get("startedAt")),
        "finished_at": _clean_text(row.get("finishedAt")),
        "duration_ms": _coerce_int(row.get("durationMs")),
        "fetched_count": fetched_count,
        "kept_count": kept_count,
        "failed_count": failed_count,
        "low_confidence_dropped": _coerce_int(row.get("lowConfidenceDropped")),
        "adapter": _clean_text(row.get("adapter")),
        "fetch_strategy": _clean_text(row.get("fetchStrategy")),
        "studio": _clean_text(row.get("studio")),
        "error": _clean_text(row.get("error")),
        "payload": payload,
        "evidence_ref": dict(evidence_ref),
        "updated_at": now_iso(),
    }


def _source_run_from_sql(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = _json_loads_object(row.get("payload_json"))
    payload.update(
        {
            "sourceKey": _clean_text(row.get("source_key")),
            "name": _clean_text(row.get("source_name")),
            "status": _clean_text(row.get("status")),
            "adapter": _clean_text(row.get("adapter")),
            "fetchStrategy": _clean_text(row.get("fetch_strategy")),
            "studio": _clean_text(row.get("studio")),
            "startedAt": _clean_text(row.get("started_at")),
            "finishedAt": _clean_text(row.get("finished_at")),
            "durationMs": _coerce_int(row.get("duration_ms")),
            "fetchedCount": _coerce_int(row.get("fetched_count")),
            "keptCount": _coerce_int(row.get("kept_count")),
            "failedCount": _coerce_int(row.get("failed_count")),
            "lowConfidenceDropped": _coerce_int(row.get("low_confidence_dropped")),
            "error": _clean_text(row.get("error")),
            "evidenceRefs": _json_loads_object(row.get("evidence_ref_json")),
        }
    )
    return payload


class SourceRuntimeStore:
    """Persists terminal per-source fetch details in SQLite."""

    def __init__(
        self,
        store: BaluffoStore,
        *,
        now_iso: Callable[[], str] = _now_iso,
        row_limit: int = DEFAULT_SOURCE_RUN_LIMIT,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.store = store
        self._now_iso = now_iso
        self.row_limit = max(1, int(row_limit or DEFAULT_SOURCE_RUN_LIMIT))
        self.batch_size = max(1, int(batch_size or DEFAULT_BATCH_SIZE))

    def upsert_source_runs(
        self,
        *,
        run_id: str,
        rows: Sequence[Mapping[str, Any]],
        evidence_ref: Mapping[str, Any] | None = None,
    ) -> int:
        normalized_run_id = _clean_text(run_id)
        if not normalized_run_id:
            raise ValueError("source runs require runId")
        records = [
            _source_run_record(
                normalized_run_id,
                row,
                ordinal=index,
                evidence_ref=evidence_ref or {},
                now_iso=self._now_iso,
            )
            for index, row in enumerate(rows)
            if isinstance(row, Mapping)
        ]
        if not records:
            return 0

        total = 0
        for start in range(0, len(records), self.batch_size):
            batch = records[start : start + self.batch_size]

            def write_batch(conn: Any, batch_rows: list[dict[str, Any]] = batch) -> None:
                conn.executemany(
                    """
                    INSERT INTO sources(
                        id, canonical_key, status, name, adapter, url, payload_json,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        status = excluded.status,
                        name = excluded.name,
                        adapter = excluded.adapter,
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at
                    """,
                    [
                        (
                            record["source_id"],
                            record["source_id"],
                            "fetch-report",
                            record["source_name"],
                            record["adapter"],
                            "",
                            _json_dumps(
                                {
                                    "sourceKey": record["source_key"],
                                    "name": record["source_name"],
                                    "adapter": record["adapter"],
                                }
                            ),
                            record["updated_at"],
                            record["updated_at"],
                        )
                        for record in batch_rows
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO source_runs(
                        run_id, source_id, status, started_at, finished_at, duration_ms,
                        fetched_count, kept_count, failed_count, payload_json,
                        schema_version, ordinal, source_key, source_name, adapter,
                        fetch_strategy, studio, error, low_confidence_dropped,
                        evidence_ref_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, source_id) DO UPDATE SET
                        status = excluded.status,
                        started_at = excluded.started_at,
                        finished_at = excluded.finished_at,
                        duration_ms = excluded.duration_ms,
                        fetched_count = excluded.fetched_count,
                        kept_count = excluded.kept_count,
                        failed_count = excluded.failed_count,
                        payload_json = excluded.payload_json,
                        schema_version = excluded.schema_version,
                        ordinal = excluded.ordinal,
                        source_key = excluded.source_key,
                        source_name = excluded.source_name,
                        adapter = excluded.adapter,
                        fetch_strategy = excluded.fetch_strategy,
                        studio = excluded.studio,
                        error = excluded.error,
                        low_confidence_dropped = excluded.low_confidence_dropped,
                        evidence_ref_json = excluded.evidence_ref_json,
                        updated_at = excluded.updated_at
                    """,
                    [
                        (
                            record["run_id"],
                            record["source_id"],
                            record["status"],
                            record["started_at"],
                            record["finished_at"],
                            record["duration_ms"],
                            record["fetched_count"],
                            record["kept_count"],
                            record["failed_count"],
                            _json_dumps(record["payload"]),
                            record["schema_version"],
                            record["ordinal"],
                            record["source_key"],
                            record["source_name"],
                            record["adapter"],
                            record["fetch_strategy"],
                            record["studio"],
                            record["error"],
                            record["low_confidence_dropped"],
                            _json_dumps(record["evidence_ref"]),
                            record["updated_at"],
                        )
                        for record in batch_rows
                    ],
                )

            self.store.write(write_batch)
            total += len(batch)
        return total

    def source_runs(
        self,
        *,
        run_id: str,
        status: str = "",
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        normalized_limit = max(1, min(self.row_limit, int(limit or self.row_limit)))
        normalized_offset = max(0, int(offset or 0))
        clauses = ["run_id = ?"]
        params: list[Any] = [_clean_text(run_id)]
        status_token = _norm_token(status)
        if status_token:
            clauses.append("status = ?")
            params.append(status_token)
        rows = self.store.execute_read(
            f"""
            SELECT * FROM source_runs
            WHERE {" AND ".join(clauses)}
            ORDER BY ordinal ASC, source_name ASC
            LIMIT ? OFFSET ?
            """,
            (*params, normalized_limit, normalized_offset),
        )
        return [_source_run_from_sql(row) for row in rows]

    def source_runs_for_parity(self, *, run_id: str, expected_count: int) -> list[dict[str, Any]]:
        """Read the full terminal report population for mirror parity checks.

        Ordinary source-run reads remain bounded by ``row_limit``. Terminal
        parity needs to detect rows beyond that live/read cap, so this method
        reads at most ``expected_count + 1`` rows and lets the caller compare
        the complete expected population.
        """
        normalized_run_id = _clean_text(run_id)
        normalized_expected_count = max(1, _coerce_int(expected_count, 1))
        rows = self.store.execute_read(
            """
            SELECT * FROM source_runs
            WHERE run_id = ?
            ORDER BY ordinal ASC, source_name ASC
            LIMIT ? OFFSET 0
            """,
            (normalized_run_id, normalized_expected_count + 1),
        )
        return [_source_run_from_sql(row) for row in rows]

    def source_run_summary(self, *, run_id: str) -> dict[str, Any]:
        rows = self.store.execute_read(
            """
            SELECT status, COUNT(*) AS row_count
            FROM source_runs
            WHERE run_id = ?
            GROUP BY status
            """,
            (_clean_text(run_id),),
        )
        counts = Counter({str(row["status"]): _coerce_int(row["row_count"]) for row in rows})
        total = sum(counts.values())
        return {
            "runId": _clean_text(run_id),
            "rowCount": total,
            "statusCounts": dict(counts),
            "successfulSources": counts.get("ok", 0),
            "failedSources": counts.get("error", 0),
            "excludedSources": counts.get("excluded", 0),
        }

    def prune_old_runs(
        self,
        *,
        retention_days: int = DEFAULT_RUNTIME_RETENTION_DAYS,
        keep_runs: int = DEFAULT_RUNTIME_RETENTION_KEEP_RUNS,
        max_runs: int = DEFAULT_RUNTIME_RETENTION_MAX_RUNS,
        max_event_rows: int = DEFAULT_RUNTIME_RETENTION_MAX_EVENT_ROWS,
        max_sync_runs: int = DEFAULT_RUNTIME_RETENTION_MAX_SYNC_RUNS,
        now: datetime | None = None,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
        cancel_requested: CancelRequested | None = None,
    ) -> dict[str, Any]:
        """Delete old terminal source-run payloads in bounded batches.

        ``source_runs`` keeps a compact per-source report payload for every
        fetch. Keep the newest ``keep_runs`` runs and every run that is still
        active; delete every older terminal run, including recent runs beyond the
        count cap, so growth is bounded even when fetches arrive faster than the
        age cutoff. Deletion is deliberately bounded by ``max_runs`` so one
        startup pass cannot monopolize the write lock; event and sync deletions
        are bounded separately so a huge history cannot become one giant
        transaction.
        """
        raise_if_cancelled(cancel_requested)
        normalized_days = max(0, int(retention_days or 0))
        normalized_keep = max(1, int(keep_runs or 1))
        normalized_max = max(0, int(max_runs or 0))
        if normalized_days <= 0 or normalized_max <= 0:
            return {
                "status": "disabled",
                "retentionDays": normalized_days,
                "keepRuns": normalized_keep,
                "deletedRunCount": 0,
                "deletedRowCount": 0,
            }

        now_dt = now or datetime.now(UTC)
        cutoff = now_dt - timedelta(days=normalized_days)
        _emit_progress(on_progress, {"phase": "prune-source-runs", "stage": "scanning"})
        active_rows = self.store.execute_read(
            "SELECT run_id FROM task_runs WHERE status IN ('queued', 'running')"
        )
        active_run_ids = {
            _clean_text(row.get("run_id")) for row in active_rows if _clean_text(row.get("run_id"))
        }
        run_rows = self.store.execute_read(
            """
            SELECT run_id,
                   MAX(COALESCE(NULLIF(updated_at, ''), NULLIF(finished_at, ''), started_at))
                       AS ended_at
            FROM source_runs
            GROUP BY run_id
            """
        )
        parsed_runs: list[tuple[str, datetime]] = []
        for row in run_rows:
            run_id = _clean_text(row.get("run_id"))
            ended_at = _parse_iso(row.get("ended_at"))
            if run_id and ended_at is not None:
                parsed_runs.append((run_id, ended_at))
        parsed_runs.sort(key=lambda item: item[1], reverse=True)
        keep_run_ids = {run_id for run_id, _ended_at in parsed_runs[:normalized_keep]}
        candidates = [
            (run_id, ended_at)
            for run_id, ended_at in parsed_runs
            if run_id not in keep_run_ids and run_id not in active_run_ids
        ]
        candidates.sort(key=lambda item: item[1])
        selected = candidates[:normalized_max]
        _emit_progress(
            on_progress,
            {
                "phase": "prune-source-runs",
                "stage": "deleting",
                "candidateRunCount": len(candidates),
                "processedRunCount": 0,
                "totalRunCount": len(selected),
                "deletedRunCount": 0,
                "deletedRowCount": 0,
            },
        )

        deleted_run_count, deleted_row_count = _delete_source_run_batches(
            self.store,
            selected,
            candidate_run_count=len(candidates),
            on_progress=on_progress,
            cancel_requested=cancel_requested,
        )
        raise_if_cancelled(cancel_requested)

        cutoff_text = cutoff.isoformat().replace("+00:00", "Z")
        event_limit = max(0, int(max_event_rows or 0))
        sync_limit = max(0, int(max_sync_runs or 0))

        def delete_bounded_rows(conn: Any) -> tuple[int, int]:
            event_count = 0
            sync_count = 0
            if event_limit > 0:
                event_count = max(
                    0,
                    int(
                        conn.execute(
                            """
                            DELETE FROM task_events
                            WHERE id IN (
                                SELECT id FROM task_events
                                WHERE created_at <> '' AND created_at < ?
                                  AND run_id NOT IN (
                                      SELECT run_id FROM task_runs
                                      WHERE status IN ('queued', 'running')
                                  )
                                ORDER BY created_at ASC, id ASC
                                LIMIT ?
                            )
                            """,
                            (cutoff_text, event_limit),
                        ).rowcount
                        or 0
                    ),
                )
            if sync_limit > 0:
                sync_count = max(
                    0,
                    int(
                        conn.execute(
                            """
                            DELETE FROM sync_runs
                            WHERE run_id IN (
                                SELECT run_id FROM sync_runs
                                WHERE started_at <> '' AND started_at < ?
                                  AND status NOT IN ('queued', 'running')
                                ORDER BY started_at ASC
                                LIMIT ?
                            )
                            """,
                            (cutoff_text, sync_limit),
                        ).rowcount
                        or 0
                    ),
                )
            return event_count, sync_count

        _emit_progress(
            on_progress,
            {
                "phase": "prune-event-history",
                "stage": "deleting",
                "candidateRunCount": len(candidates),
                "processedRunCount": deleted_run_count,
                "totalRunCount": len(selected),
                "deletedRunCount": deleted_run_count,
                "deletedRowCount": deleted_row_count,
            },
        )
        raise_if_cancelled(cancel_requested)
        event_row_count, sync_row_count = self.store.write(delete_bounded_rows)
        raise_if_cancelled(cancel_requested)
        event_truncated = event_limit > 0 and event_row_count >= event_limit
        sync_truncated = sync_limit > 0 and sync_row_count >= sync_limit
        _emit_progress(
            on_progress,
            {
                "phase": "prune-event-history",
                "stage": "complete",
                "candidateRunCount": len(candidates),
                "processedRunCount": deleted_run_count,
                "totalRunCount": len(selected),
                "deletedRunCount": deleted_run_count,
                "deletedRowCount": deleted_row_count,
                "deletedEventCount": event_row_count,
                "deletedSyncRunCount": sync_row_count,
            },
        )
        return {
            "status": "ok" if selected or event_row_count or sync_row_count else "nothing-to-prune",
            "retentionDays": normalized_days,
            "keepRuns": normalized_keep,
            "maxRuns": normalized_max,
            "maxEventRows": event_limit,
            "maxSyncRuns": sync_limit,
            "cutoff": cutoff_text,
            "candidateRunCount": len(candidates),
            "deletedRunCount": deleted_run_count,
            "deletedRowCount": deleted_row_count,
            "deletedEventCount": event_row_count,
            "deletedSyncRunCount": sync_row_count,
            "retainedRunCount": max(0, len(parsed_runs) - deleted_run_count),
            "truncated": len(candidates) > len(selected) or event_truncated or sync_truncated,
        }

    def compact_archived_payloads(
        self,
        *,
        max_runs: int = DEFAULT_ARCHIVE_COMPACT_RUNS,
        max_rows: int = DEFAULT_ARCHIVE_COMPACT_ROWS,
        max_bytes: int = DEFAULT_ARCHIVE_COMPACT_BYTES,
        now: datetime | None = None,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
        cancel_requested: CancelRequested | None = None,
    ) -> dict[str, Any]:
        """Remove full source details from SQLite when the gzip archive owns them.

        Older builds wrote the complete per-source `details` payload to
        `source_runs.payload_json` *and* to the evidence archive. That duplication
        is what turned the runtime database into hundreds of gigabytes. This pass
        only touches rows whose evidence reference names a source-details archive,
        so a row without a durable copy of its details is never stripped.
        """
        del now  # Kept in the signature for parity with prune_old_runs.
        raise_if_cancelled(cancel_requested)
        normalized_max_runs = max(1, int(max_runs or 1))
        normalized_max_rows = max(1, int(max_rows or 1))
        normalized_max_bytes = max(1, int(max_bytes or 1))
        run_rows = self.store.execute_read(
            """
            SELECT run_id
            FROM source_runs
            WHERE run_id <> ''
            GROUP BY run_id
            ORDER BY MAX(COALESCE(NULLIF(updated_at, ''), NULLIF(finished_at, ''), started_at)) DESC
            LIMIT ?
            """,
            (normalized_max_runs,),
        )
        run_ids = [
            _clean_text(row.get("run_id")) for row in run_rows if _clean_text(row.get("run_id"))
        ]
        _emit_progress(
            on_progress,
            {
                "phase": "compact-archived-payloads",
                "stage": "scanning",
                "candidateRunCount": len(run_ids),
            },
        )
        if not run_ids:
            return {
                "status": "nothing-to-compact",
                "candidateRowCount": 0,
                "compactedPayloadCount": 0,
                "compactedPayloadBytes": 0,
                "truncated": False,
            }
        placeholders = ",".join("?" for _ in run_ids)
        rows = self.store.execute_read(
            f"""
            SELECT run_id, source_id, payload_json, evidence_ref_json
            FROM source_runs
            WHERE run_id IN ({placeholders})
              AND payload_json LIKE '%\"details\"%'
              AND evidence_ref_json LIKE '%sourceDetailsArchive%'
            LIMIT ?
            """,
            (*run_ids, normalized_max_rows + 1),
        )
        raise_if_cancelled(cancel_requested)
        truncated = len(rows) > normalized_max_rows
        _emit_progress(
            on_progress,
            {
                "phase": "compact-archived-payloads",
                "stage": "updating",
                "candidateRunCount": len(run_ids),
                "candidateRowCount": len(rows),
                "compactedPayloadCount": 0,
                "compactedPayloadBytes": 0,
            },
        )
        archive_store = EvidenceArchiveStore(self.store.data_dir)
        updates: list[tuple[str, str, str]] = []
        compacted_bytes = 0
        processed_bytes = 0
        for row in rows[:normalized_max_rows]:
            raise_if_cancelled(cancel_requested)
            raw = str(row.get("payload_json") or "")
            raw_bytes = len(raw.encode("utf-8"))
            if updates and processed_bytes + raw_bytes > normalized_max_bytes:
                truncated = True
                break
            evidence = _json_loads_object(row.get("evidence_ref_json"))
            archive = _json_object(evidence.get("sourceDetailsArchive"))
            if not archive_store.archive_exists(archive):
                continue
            payload = _json_loads_object(raw)
            if not payload or "details" not in payload:
                continue
            compact = {key: value for key, value in payload.items() if key != "details"}
            compact_text = _json_dumps(compact)
            if len(compact_text) >= len(raw):
                continue
            updates.append(
                (
                    compact_text,
                    _clean_text(row.get("run_id")),
                    _clean_text(row.get("source_id")),
                )
            )
            processed_bytes += raw_bytes
            compacted_bytes += raw_bytes - len(compact_text.encode("utf-8"))
        raise_if_cancelled(cancel_requested)
        if updates:
            self.store.write(
                lambda conn: conn.executemany(
                    """
                    UPDATE source_runs
                    SET payload_json = ?
                    WHERE run_id = ? AND source_id = ?
                    """,
                    updates,
                )
            )
        raise_if_cancelled(cancel_requested)
        _emit_progress(
            on_progress,
            {
                "phase": "compact-archived-payloads",
                "stage": "complete",
                "candidateRunCount": len(run_ids),
                "candidateRowCount": len(rows),
                "compactedPayloadCount": len(updates),
                "compactedPayloadBytes": compacted_bytes,
            },
        )
        return {
            "status": "ok" if updates else "nothing-to-compact",
            "candidateRowCount": len(rows),
            "compactedPayloadCount": len(updates),
            "compactedPayloadBytes": compacted_bytes,
            "truncated": truncated,
        }
