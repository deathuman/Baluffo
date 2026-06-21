"""SQLite source-registry runtime storage APIs.

AI boundary owns: persisted source registry snapshots, bucket rows, link identity, and registry generations.
AI boundary implement in: this file for registry storage operations; registry policy and route payloads stay outside storage.
AI boundary search before contracts: source registry identity, bridge registry routes, migrations, and registry storage tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused source registry storage tests.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.source_registry_identity import source_identity
from src.storage.baluffo_store import DEFAULT_BATCH_SIZE, BaluffoStore

SOURCE_REGISTRY_SCHEMA_VERSION = 1
DEFAULT_GENERATION_DELETE_CAP = 4
SOURCE_REGISTRY_BUCKETS = ("active", "pending", "rejected")


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _coerce_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_loads_object(value: Any) -> dict[str, Any]:
    try:
        loaded = json.loads(str(value or "{}"))
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()


def _generation_token(now_iso: str) -> str:
    safe_time = re.sub(r"[^0-9A-Za-z]+", "", now_iso)[:24] or "generation"
    return f"registry_{safe_time}_{uuid.uuid4().hex[:12]}"


def _normalize_state(state: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    normalized: dict[str, list[dict[str, Any]]] = {}
    for bucket in SOURCE_REGISTRY_BUCKETS:
        rows = state.get(bucket)
        normalized[bucket] = [dict(row) for row in (rows or []) if isinstance(row, Mapping)]
    return normalized


def _normalize_tombstones(tombstones: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    normalized: dict[str, dict[str, Any]] = {}
    for key, value in dict(tombstones or {}).items():
        text_key = _clean_text(key)
        if not text_key:
            continue
        normalized[text_key] = dict(value) if isinstance(value, Mapping) else {"value": value}
    return normalized


def _row_identity(row: Mapping[str, Any], bucket: str, ordinal: int) -> str:
    identity = _clean_text(source_identity(dict(row)))
    return identity or f"{bucket}:{ordinal}"


def _tombstone_key(value: Mapping[str, Any], fallback: str, ordinal: int) -> str:
    for key in ("key", "sourceIdentity", "sourceId", "id"):
        text = _clean_text(value.get(key))
        if text:
            return text
    return _clean_text(fallback) or f"tombstone:{ordinal}"


def source_registry_state_hash(state: Mapping[str, Any]) -> str:
    normalized = _normalize_state(state)
    return _sha256_json({bucket: normalized[bucket] for bucket in SOURCE_REGISTRY_BUCKETS})


def source_registry_tombstone_hash(tombstones: Mapping[str, Any] | None) -> str:
    normalized = _normalize_tombstones(tombstones)
    return _sha256_json(
        [
            {"key": key, "payload": normalized[key]}
            for key in sorted(normalized.keys(), key=lambda item: item.lower())
        ]
    )


@dataclass(frozen=True)
class SourceRegistryWriteSummary:
    generation: str
    reason: str
    active_count: int
    pending_count: int
    rejected_count: int
    tombstone_count: int
    state_hash: str
    tombstone_hash: str
    published: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "generation": self.generation,
            "reason": self.reason,
            "activeCount": self.active_count,
            "pendingCount": self.pending_count,
            "rejectedCount": self.rejected_count,
            "tombstoneCount": self.tombstone_count,
            "stateHash": self.state_hash,
            "tombstoneHash": self.tombstone_hash,
            "published": self.published,
        }


class SourceRegistryRuntimeStore:
    """Persists generation-scoped source-registry rows and tombstones in SQLite."""

    def __init__(
        self,
        store: BaluffoStore,
        *,
        now_iso: Callable[[], str] = _now_iso,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.store = store
        self._now_iso = now_iso
        self.batch_size = max(1, int(batch_size or DEFAULT_BATCH_SIZE))

    def stage_state(
        self,
        *,
        state: Mapping[str, Any],
        tombstones: Mapping[str, Any] | None = None,
        generation: str = "",
        reason: str = "",
    ) -> SourceRegistryWriteSummary:
        updated_at = self._now_iso()
        registry_generation = _clean_text(generation) or _generation_token(updated_at)
        normalized_state = _normalize_state(state)
        normalized_tombstones = _normalize_tombstones(tombstones)
        row_records = self._row_records_for_state(
            normalized_state,
            generation=registry_generation,
            updated_at=updated_at,
        )
        tombstone_records = self._records_for_tombstones(
            normalized_tombstones,
            generation=registry_generation,
            updated_at=updated_at,
        )

        def delete_existing(conn: Any) -> None:
            conn.execute(
                "DELETE FROM source_registry_tombstones WHERE registry_generation = ?",
                (registry_generation,),
            )
            conn.execute(
                "DELETE FROM source_registry_rows WHERE registry_generation = ?",
                (registry_generation,),
            )

        self.store.write(delete_existing)
        for start in range(0, len(row_records), self.batch_size):
            batch = row_records[start : start + self.batch_size]

            def write_rows(conn: Any, batch_rows: list[dict[str, Any]] = batch) -> None:
                conn.executemany(
                    """
                    INSERT INTO source_registry_rows(
                        registry_generation, bucket, source_identity, row_ordinal,
                        row_hash, payload_json, schema_version, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            record["registry_generation"],
                            record["bucket"],
                            record["source_identity"],
                            record["row_ordinal"],
                            record["row_hash"],
                            record["payload_json"],
                            record["schema_version"],
                            record["updated_at"],
                        )
                        for record in batch_rows
                    ],
                )

            self.store.write(write_rows)
        for start in range(0, len(tombstone_records), self.batch_size):
            batch = tombstone_records[start : start + self.batch_size]

            def write_tombstones(conn: Any, batch_rows: list[dict[str, Any]] = batch) -> None:
                conn.executemany(
                    """
                    INSERT INTO source_registry_tombstones(
                        registry_generation, tombstone_key, row_ordinal, row_hash,
                        payload_json, schema_version, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            record["registry_generation"],
                            record["tombstone_key"],
                            record["row_ordinal"],
                            record["row_hash"],
                            record["payload_json"],
                            record["schema_version"],
                            record["updated_at"],
                        )
                        for record in batch_rows
                    ],
                )

            self.store.write(write_tombstones)
        return SourceRegistryWriteSummary(
            generation=registry_generation,
            reason=_clean_text(reason),
            active_count=len(normalized_state["active"]),
            pending_count=len(normalized_state["pending"]),
            rejected_count=len(normalized_state["rejected"]),
            tombstone_count=len(normalized_tombstones),
            state_hash=source_registry_state_hash(normalized_state),
            tombstone_hash=source_registry_tombstone_hash(normalized_tombstones),
            published=False,
        )

    def replace_state(
        self,
        *,
        state: Mapping[str, Any],
        tombstones: Mapping[str, Any] | None = None,
        generation: str = "",
        reason: str = "",
    ) -> SourceRegistryWriteSummary:
        summary = self.stage_state(
            state=state,
            tombstones=tombstones,
            generation=generation,
            reason=reason,
        )
        self.publish_generation(
            summary.generation,
            expected_state_hash=summary.state_hash,
            expected_tombstone_hash=summary.tombstone_hash,
            reason=reason,
        )
        return SourceRegistryWriteSummary(
            generation=summary.generation,
            reason=summary.reason,
            active_count=summary.active_count,
            pending_count=summary.pending_count,
            rejected_count=summary.rejected_count,
            tombstone_count=summary.tombstone_count,
            state_hash=summary.state_hash,
            tombstone_hash=summary.tombstone_hash,
            published=True,
        )

    def publish_generation(
        self,
        generation: str,
        *,
        expected_state_hash: str = "",
        expected_tombstone_hash: str = "",
        reason: str = "",
    ) -> dict[str, Any]:
        registry_generation = _clean_text(generation)
        if not registry_generation:
            raise ValueError("source registry publish requires a generation")
        state = self.state_for_generation(registry_generation)
        tombstones = self.tombstones_for_generation(registry_generation)
        state_hash = source_registry_state_hash(state)
        tombstone_hash = source_registry_tombstone_hash(tombstones)
        if expected_state_hash and state_hash != expected_state_hash:
            raise ValueError("source registry generation state hash mismatch")
        if expected_tombstone_hash and tombstone_hash != expected_tombstone_hash:
            raise ValueError("source registry generation tombstone hash mismatch")
        now = self._now_iso()

        def publish(conn: Any) -> None:
            conn.execute(
                """
                UPDATE source_registry_state
                SET current_generation = ?, reason = ?, active_count = ?,
                    pending_count = ?, rejected_count = ?, tombstone_count = ?,
                    state_hash = ?, tombstone_hash = ?, schema_version = ?,
                    updated_at = ?, published_at = ?, payload_json = ?
                WHERE id = 1
                """,
                (
                    registry_generation,
                    _clean_text(reason),
                    len(state["active"]),
                    len(state["pending"]),
                    len(state["rejected"]),
                    len(tombstones),
                    state_hash,
                    tombstone_hash,
                    SOURCE_REGISTRY_SCHEMA_VERSION,
                    now,
                    now,
                    _json_dumps({"generation": registry_generation}),
                ),
            )

        self.store.write(publish)
        return self.current_summary()

    def current_generation(self) -> str:
        row = self.store.execute_read(
            "SELECT current_generation FROM source_registry_state WHERE id = 1"
        )
        if not row:
            return ""
        return _clean_text(row[0].get("current_generation"))

    def current_state(self) -> dict[str, list[dict[str, Any]]]:
        generation = self.current_generation()
        if not generation:
            return {bucket: [] for bucket in SOURCE_REGISTRY_BUCKETS}
        return self.state_for_generation(generation)

    def current_tombstones(self) -> dict[str, dict[str, Any]]:
        generation = self.current_generation()
        if not generation:
            return {}
        return self.tombstones_for_generation(generation)

    def state_for_generation(self, generation: str) -> dict[str, list[dict[str, Any]]]:
        registry_generation = _clean_text(generation)
        state = {bucket: [] for bucket in SOURCE_REGISTRY_BUCKETS}
        if not registry_generation:
            return state
        rows = self.store.execute_read(
            """
            SELECT bucket, payload_json
            FROM source_registry_rows
            WHERE registry_generation = ?
            ORDER BY
                CASE bucket
                    WHEN 'active' THEN 0
                    WHEN 'pending' THEN 1
                    ELSE 2
                END,
                row_ordinal ASC
            """,
            (registry_generation,),
        )
        for row in rows:
            bucket = _clean_text(row.get("bucket"))
            if bucket in state:
                state[bucket].append(_json_loads_object(row.get("payload_json")))
        return state

    def tombstones_for_generation(self, generation: str) -> dict[str, dict[str, Any]]:
        registry_generation = _clean_text(generation)
        if not registry_generation:
            return {}
        rows = self.store.execute_read(
            """
            SELECT tombstone_key, payload_json
            FROM source_registry_tombstones
            WHERE registry_generation = ?
            ORDER BY row_ordinal ASC
            """,
            (registry_generation,),
        )
        return {
            _clean_text(row.get("tombstone_key")): _json_loads_object(row.get("payload_json"))
            for row in rows
            if _clean_text(row.get("tombstone_key"))
        }

    def current_summary(self) -> dict[str, Any]:
        rows = self.store.execute_read("SELECT * FROM source_registry_state WHERE id = 1")
        if not rows:
            return {
                "generation": "",
                "reason": "",
                "activeCount": 0,
                "pendingCount": 0,
                "rejectedCount": 0,
                "tombstoneCount": 0,
                "stateHash": "",
                "tombstoneHash": "",
                "publishedAt": "",
                "updatedAt": "",
            }
        row = rows[0]
        return {
            "generation": _clean_text(row.get("current_generation")),
            "reason": _clean_text(row.get("reason")),
            "activeCount": _coerce_int(row.get("active_count")),
            "pendingCount": _coerce_int(row.get("pending_count")),
            "rejectedCount": _coerce_int(row.get("rejected_count")),
            "tombstoneCount": _coerce_int(row.get("tombstone_count")),
            "stateHash": _clean_text(row.get("state_hash")),
            "tombstoneHash": _clean_text(row.get("tombstone_hash")),
            "publishedAt": _clean_text(row.get("published_at")),
            "updatedAt": _clean_text(row.get("updated_at")),
        }

    def parity_hash(
        self,
        *,
        state: Mapping[str, Any] | None = None,
        tombstones: Mapping[str, Any] | None = None,
    ) -> dict[str, str]:
        return {
            "stateHash": source_registry_state_hash(
                self.current_state() if state is None else state
            ),
            "tombstoneHash": source_registry_tombstone_hash(
                self.current_tombstones() if tombstones is None else tombstones
            ),
        }

    def cleanup_old_generations(self, *, delete_cap: int = DEFAULT_GENERATION_DELETE_CAP) -> int:
        current = self.current_generation()
        rows = self.store.execute_read(
            """
            SELECT registry_generation
            FROM source_registry_rows
            WHERE registry_generation != ?
            GROUP BY registry_generation
            ORDER BY MIN(updated_at) ASC
            LIMIT ?
            """,
            (current, max(0, int(delete_cap or 0))),
        )
        generations = [_clean_text(row.get("registry_generation")) for row in rows]
        generations = [generation for generation in generations if generation]
        if not generations:
            return 0

        def delete_old(conn: Any) -> None:
            for generation in generations:
                conn.execute(
                    "DELETE FROM source_registry_tombstones WHERE registry_generation = ?",
                    (generation,),
                )
                conn.execute(
                    "DELETE FROM source_registry_rows WHERE registry_generation = ?",
                    (generation,),
                )

        self.store.write(delete_old)
        return len(generations)

    def _row_records_for_state(
        self,
        state: Mapping[str, Sequence[Mapping[str, Any]]],
        *,
        generation: str,
        updated_at: str,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for bucket in SOURCE_REGISTRY_BUCKETS:
            identity_counts: dict[str, int] = {}
            for ordinal, row in enumerate(state.get(bucket, [])):
                row_copy = dict(row)
                base_identity = _row_identity(row_copy, bucket, ordinal)
                count = identity_counts.get(base_identity, 0)
                identity_counts[base_identity] = count + 1
                identity = base_identity if count == 0 else f"{base_identity}::dup::{ordinal}"
                records.append(
                    {
                        "registry_generation": generation,
                        "bucket": bucket,
                        "source_identity": identity,
                        "row_ordinal": ordinal,
                        "row_hash": _sha256_json(row_copy),
                        "payload_json": _json_dumps(row_copy),
                        "schema_version": SOURCE_REGISTRY_SCHEMA_VERSION,
                        "updated_at": updated_at,
                    }
                )
        return records

    def _records_for_tombstones(
        self,
        tombstones: Mapping[str, Mapping[str, Any]],
        *,
        generation: str,
        updated_at: str,
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        for ordinal, key in enumerate(sorted(tombstones.keys(), key=lambda item: item.lower())):
            payload = dict(tombstones[key])
            tombstone_key = _tombstone_key(payload, key, ordinal)
            records.append(
                {
                    "registry_generation": generation,
                    "tombstone_key": tombstone_key,
                    "row_ordinal": ordinal,
                    "row_hash": _sha256_json(payload),
                    "payload_json": _json_dumps(payload),
                    "schema_version": SOURCE_REGISTRY_SCHEMA_VERSION,
                    "updated_at": updated_at,
                }
            )
        return records


__all__ = [
    "DEFAULT_GENERATION_DELETE_CAP",
    "SOURCE_REGISTRY_SCHEMA_VERSION",
    "SOURCE_REGISTRY_BUCKETS",
    "SourceRegistryRuntimeStore",
    "SourceRegistryWriteSummary",
    "source_registry_state_hash",
    "source_registry_tombstone_hash",
]
