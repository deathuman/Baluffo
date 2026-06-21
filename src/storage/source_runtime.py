"""SQLite per-source fetch run storage APIs.

AI boundary owns: per-source fetch run rows, source summaries, and persisted fetch-run evidence.
AI boundary implement in: this file for source run storage operations; fetch execution and route payloads stay outside storage.
AI boundary search before contracts: fetch report normalization, source run routes, migrations, and source runtime tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source runtime storage tests.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from src.storage.baluffo_store import DEFAULT_BATCH_SIZE, BaluffoStore

SOURCE_RUN_SCHEMA_VERSION = 1
DEFAULT_SOURCE_RUN_LIMIT = 500


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_token(value: Any) -> str:
    return _clean_text(value).lower()


def _coerce_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _json_object(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _json_dumps(value: Any) -> str:
    return json.dumps(
        value if isinstance(value, dict) else {}, sort_keys=True, separators=(",", ":")
    )


def _json_loads_object(value: Any) -> dict[str, Any]:
    try:
        loaded = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


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
