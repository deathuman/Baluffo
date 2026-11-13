"""SQLite jobs feed storage APIs.

AI boundary owns: jobs feed generation rows, job snapshots, and feed storage schema adapters.
AI boundary implement in: this file for persisted jobs feed operations; job normalization stays in src.jobs.
AI boundary search before contracts: jobs contracts, feed consumers, migrations, and storage tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused jobs feed storage tests.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from src.shared.json_io import json_dumps, loads_object
from src.shared.text_utils import clean_text as _shared_clean_text
from src.shared.utils import int_or_default
from src.shared.utils import now_iso as _shared_now_iso
from src.storage.baluffo_store import (
    BULK_DELETE_SECURE_DELETE,
    DEFAULT_BATCH_SIZE,
    BaluffoStore,
    BaluffoStoreError,
)

LOGGER = logging.getLogger(__name__)

JOB_FEED_SCHEMA_VERSION = 1
DEFAULT_GENERATION_DELETE_CAP = 4
DEFAULT_GENERATION_DELETE_BATCH_SIZE = 1000
_SOURCE_ALIAS_QUERY_CHUNK = 200

# Keep the batch's selection generation-scoped and name the feed_generation-leading
# index. The outer delete is by rowid, so a batch is bounded by rows even when one
# job has a large source bundle; selecting job keys would make the transaction size
# depend on the bundle shape.
_JOB_SOURCES_BATCH_DELETE_SQL = """
DELETE FROM job_sources
WHERE rowid IN (
      SELECT rowid
      FROM job_sources INDEXED BY idx_job_sources_generation_job
      WHERE feed_generation = ?
      ORDER BY job_key
      LIMIT ?
  )
"""

_JOBS_BATCH_DELETE_SQL = """
DELETE FROM jobs INDEXED BY idx_jobs_generation_ordinal
WHERE feed_generation = ?
  AND row_ordinal IN (
      SELECT row_ordinal
      FROM jobs INDEXED BY idx_jobs_generation_ordinal
      WHERE feed_generation = ?
      ORDER BY row_ordinal
      LIMIT ?
  )
"""


def _now_iso() -> str:
    return _shared_now_iso()


_clean_text = _shared_clean_text


def _json_dumps(value: Any) -> str:
    return json_dumps(value)


def _json_loads_object(value: Any) -> dict[str, Any]:
    return loads_object(value)


def _json_loads_list(value: Any) -> list[Any]:
    try:
        loaded = json.loads(str(value or "[]"))
    except (TypeError, json.JSONDecodeError):
        return []
    return list(loaded) if isinstance(loaded, list) else []


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()


def jobs_feed_rows_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    return _sha256_json([dict(row) for row in rows if isinstance(row, Mapping)])


def _location_label(row: Mapping[str, Any]) -> str:
    city = _clean_text(row.get("city"))
    country = _clean_text(row.get("country"))
    return ", ".join(part for part in (city, country) if part)


def _source_bundle(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    bundle = row.get("sourceBundle")
    if isinstance(bundle, str):
        bundle = _json_loads_list(bundle)
    return [dict(item) for item in (bundle or []) if isinstance(item, Mapping)]


def _source_identity(row: Mapping[str, Any], ordinal: int) -> str:
    source_name = _clean_text(row.get("sourceName")) or _clean_text(row.get("source"))
    source_id = _clean_text(row.get("sourceId"))
    source_job_id = _clean_text(row.get("sourceJobId"))
    token = "::".join(part for part in (source_id, source_name, source_job_id) if part)
    return token or f"source:{ordinal}"


def _base_job_key(row: Mapping[str, Any], ordinal: int) -> str:
    source = _clean_text(row.get("source"))
    source_job_id = _clean_text(row.get("sourceJobId"))
    token = (
        _clean_text(row.get("dedupKey"))
        or _clean_text(row.get("jobLink"))
        or ("::".join(part for part in (source, source_job_id) if part))
        or _clean_text(row.get("id"))
        or f"row:{ordinal}:{_sha256_json(dict(row))}"
    )
    return "job_" + hashlib.sha256(token.encode("utf-8")).hexdigest()


def _generation_token(now_iso: str) -> str:
    safe_time = re.sub(r"[^0-9A-Za-z]+", "", now_iso)[:24] or "generation"
    return f"jobs_{safe_time}_{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True)
class JobFeedWriteSummary:
    generation: str
    run_id: str
    row_count: int
    row_hash: str
    source_count: int
    source_hash: str
    published: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "generation": self.generation,
            "runId": self.run_id,
            "rowCount": self.row_count,
            "rowHash": self.row_hash,
            "sourceCount": self.source_count,
            "sourceHash": self.source_hash,
            "published": self.published,
        }


class JobRuntimeStore:
    """Persists generation-scoped canonical jobs feed rows in SQLite."""

    def __init__(
        self,
        store: BaluffoStore,
        *,
        now_iso: Callable[[], str] = _now_iso,
        batch_size: int = DEFAULT_BATCH_SIZE,
        delete_batch_size: int | None = None,
    ) -> None:
        self.store = store
        self._now_iso = now_iso
        self.batch_size = max(1, int(batch_size or DEFAULT_BATCH_SIZE))
        self.delete_batch_size = max(
            1,
            int(delete_batch_size or DEFAULT_GENERATION_DELETE_BATCH_SIZE),
        )

    def stage_feed(
        self,
        *,
        run_id: str,
        rows: Sequence[Mapping[str, Any]],
        generation: str = "",
    ) -> JobFeedWriteSummary:
        generated_at = self._now_iso()
        feed_generation = _clean_text(generation) or _generation_token(generated_at)
        records, source_records, row_hash, source_hash = self._records_for_rows(
            run_id=run_id,
            rows=rows,
            generation=feed_generation,
            updated_at=generated_at,
        )
        self.store.write(
            lambda conn: (
                conn.execute(
                    "DELETE FROM job_sources WHERE feed_generation = ?", (feed_generation,)
                ),
                conn.execute("DELETE FROM jobs WHERE feed_generation = ?", (feed_generation,)),
            )
        )
        for start in range(0, len(records), self.batch_size):
            batch = records[start : start + self.batch_size]

            def write_jobs(conn: Any, batch_rows: list[dict[str, Any]] = batch) -> None:
                conn.executemany(
                    """
                    INSERT INTO jobs(
                        feed_generation, job_key, row_ordinal, run_id, row_hash,
                        title, company, location, status, first_seen_at, last_seen_at,
                        updated_at, payload_json, schema_version
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            record["feed_generation"],
                            record["job_key"],
                            record["row_ordinal"],
                            record["run_id"],
                            record["row_hash"],
                            record["title"],
                            record["company"],
                            record["location"],
                            record["status"],
                            record["first_seen_at"],
                            record["last_seen_at"],
                            record["updated_at"],
                            record["payload_json"],
                            record["schema_version"],
                        )
                        for record in batch_rows
                    ],
                )

            self.store.write(write_jobs)
        for start in range(0, len(source_records), self.batch_size):
            batch = source_records[start : start + self.batch_size]

            def write_sources(conn: Any, batch_rows: list[dict[str, Any]] = batch) -> None:
                conn.executemany(
                    """
                    INSERT INTO job_sources(
                        feed_generation, job_key, source_ordinal, source_id,
                        source_job_id, source_name, source_key, row_hash,
                        payload_json, schema_version, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            record["feed_generation"],
                            record["job_key"],
                            record["source_ordinal"],
                            record["source_id"],
                            record["source_job_id"],
                            record["source_name"],
                            record["source_key"],
                            record["row_hash"],
                            record["payload_json"],
                            record["schema_version"],
                            record["updated_at"],
                        )
                        for record in batch_rows
                    ],
                )

            self.store.write(write_sources)
        self._save_generation_stats(
            {feed_generation: (len(records), len(source_records))},
            updated_at=generated_at,
        )
        return JobFeedWriteSummary(
            generation=feed_generation,
            run_id=_clean_text(run_id),
            row_count=len(records),
            row_hash=row_hash,
            source_count=len(source_records),
            source_hash=source_hash,
            published=False,
        )

    def replace_feed(
        self,
        *,
        run_id: str,
        rows: Sequence[Mapping[str, Any]],
        generation: str = "",
    ) -> JobFeedWriteSummary:
        summary = self.stage_feed(run_id=run_id, rows=rows, generation=generation)
        self.publish_generation(
            summary.generation,
            expected_row_count=summary.row_count,
            expected_row_hash=summary.row_hash,
        )
        return JobFeedWriteSummary(
            generation=summary.generation,
            run_id=summary.run_id,
            row_count=summary.row_count,
            row_hash=summary.row_hash,
            source_count=summary.source_count,
            source_hash=summary.source_hash,
            published=True,
        )

    def publish_generation(
        self,
        generation: str,
        *,
        expected_row_count: int | None = None,
        expected_row_hash: str = "",
    ) -> dict[str, Any]:
        feed_generation = _clean_text(generation)
        if not feed_generation:
            raise ValueError("jobs feed publish requires a generation")
        rows = self.rows_for_generation(feed_generation)
        row_hash = jobs_feed_rows_hash(rows)
        if expected_row_count is not None and len(rows) != int(expected_row_count):
            raise ValueError(
                f"jobs feed generation row count mismatch: expected {expected_row_count}, got {len(rows)}"
            )
        if expected_row_hash and row_hash != expected_row_hash:
            raise ValueError("jobs feed generation hash mismatch")
        source_count = self._source_count(feed_generation)
        source_hash = self._source_hash(feed_generation)
        run_id = self._run_id_for_generation(feed_generation)
        now = self._now_iso()

        def publish(conn: Any) -> None:
            conn.execute(
                """
                UPDATE job_feed_state
                SET current_generation = ?, run_id = ?, row_count = ?, row_hash = ?,
                    source_count = ?, source_hash = ?, schema_version = ?,
                    updated_at = ?, published_at = ?, payload_json = ?
                WHERE id = 1
                """,
                (
                    feed_generation,
                    run_id,
                    len(rows),
                    row_hash,
                    source_count,
                    source_hash,
                    JOB_FEED_SCHEMA_VERSION,
                    now,
                    now,
                    _json_dumps({"generation": feed_generation}),
                ),
            )
            conn.execute(
                """
                INSERT INTO job_feed_generation_stats(
                    feed_generation, row_count, source_count, updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(feed_generation) DO UPDATE SET
                    row_count = excluded.row_count,
                    source_count = excluded.source_count,
                    updated_at = excluded.updated_at
                """,
                (feed_generation, len(rows), source_count, now),
            )

        self.store.write(publish)
        return self.current_summary()

    def current_generation(self) -> str:
        row = self.store.execute_read("SELECT current_generation FROM job_feed_state WHERE id = 1")
        if not row:
            return ""
        return _clean_text(row[0].get("current_generation"))

    def current_rows(self) -> list[dict[str, Any]]:
        generation = self.current_generation()
        if not generation:
            return []
        return self.rows_for_generation(generation)

    def current_feed_published_at(self) -> str:
        """When the current feed generation was published, or the last update."""
        row = self.store.execute_read(
            "SELECT published_at, updated_at FROM job_feed_state WHERE id = 1"
        )
        if not row:
            return ""
        return _clean_text(row[0].get("published_at")) or _clean_text(row[0].get("updated_at"))

    def source_identity_rows(self, source_aliases: set[str]) -> list[dict[str, Any]] | None:
        """Source bundle entries for the given registry aliases in the current feed.

        ``None`` means no generation is published, so the caller should fall
        back to the JSON artifact. An empty list is authoritative: the indexed
        ``job_sources`` rows for this generation contain none of those sources.
        """
        generation = self.current_generation()
        if not generation:
            return None
        aliases = sorted(
            {_clean_text(alias).lower() for alias in source_aliases if _clean_text(alias)}
        )
        if not aliases:
            return []
        payloads: list[dict[str, Any]] = []
        for start in range(0, len(aliases), _SOURCE_ALIAS_QUERY_CHUNK):
            chunk = aliases[start : start + _SOURCE_ALIAS_QUERY_CHUNK]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.store.execute_read(
                f"""
                SELECT payload_json
                FROM job_sources
                WHERE feed_generation = ?
                  AND (source_id IN ({placeholders}) OR lower(source_name) IN ({placeholders}))
                ORDER BY job_key ASC, source_ordinal ASC
                """,
                [generation, *chunk, *chunk],
            )
            payloads.extend(_json_loads_object(row.get("payload_json")) for row in rows)
        unique: list[dict[str, Any]] = []
        seen: set[str] = set()
        for payload in payloads:
            key = "::".join(
                part
                for part in (
                    _clean_text(payload.get("source")),
                    _clean_text(payload.get("sourceJobId")),
                    _clean_text(payload.get("jobLink")),
                )
                if part
            ) or _json_dumps(payload)
            if key in seen:
                continue
            seen.add(key)
            unique.append(payload)
        return unique

    def rows_for_generation(self, generation: str) -> list[dict[str, Any]]:
        feed_generation = _clean_text(generation)
        if not feed_generation:
            return []
        rows = self.store.execute_read(
            """
            SELECT job_key, payload_json
            FROM jobs
            WHERE feed_generation = ?
            ORDER BY row_ordinal ASC
            """,
            (feed_generation,),
        )
        source_rows = self.store.execute_read(
            """
            SELECT job_key, payload_json
            FROM job_sources
            WHERE feed_generation = ?
            ORDER BY job_key ASC, source_ordinal ASC
            """,
            (feed_generation,),
        )
        sources_by_job: dict[str, list[dict[str, Any]]] = {}
        for source_row in source_rows:
            sources_by_job.setdefault(_clean_text(source_row.get("job_key")), []).append(
                _json_loads_object(source_row.get("payload_json"))
            )
        result: list[dict[str, Any]] = []
        for row in rows:
            payload = _json_loads_object(row.get("payload_json"))
            payload["sourceBundle"] = sources_by_job.get(_clean_text(row.get("job_key")), [])
            result.append(payload)
        return result

    def current_summary(self) -> dict[str, Any]:
        rows = self.store.execute_read("SELECT * FROM job_feed_state WHERE id = 1")
        if not rows:
            return {
                "generation": "",
                "runId": "",
                "rowCount": 0,
                "rowHash": "",
                "sourceCount": 0,
                "sourceHash": "",
                "publishedAt": "",
                "updatedAt": "",
            }
        row = rows[0]
        return {
            "generation": _clean_text(row.get("current_generation")),
            "runId": _clean_text(row.get("run_id")),
            "rowCount": int_or_default(row.get("row_count")),
            "rowHash": _clean_text(row.get("row_hash")),
            "sourceCount": int_or_default(row.get("source_count")),
            "sourceHash": _clean_text(row.get("source_hash")),
            "publishedAt": _clean_text(row.get("published_at")),
            "updatedAt": _clean_text(row.get("updated_at")),
        }

    def parity_hash(self, rows: Sequence[Mapping[str, Any]] | None = None) -> str:
        return jobs_feed_rows_hash(self.current_rows() if rows is None else rows)

    def cleanup_old_generations(
        self,
        *,
        delete_cap: int | None = DEFAULT_GENERATION_DELETE_CAP,
        keep_generations: int = 1,
        on_progress: Callable[[dict[str, Any]], None] | None = None,
    ) -> int:
        """Delete old jobs-feed generations without holding the write lock.

        A generation can contain enough rows to hold the store write lock for
        hours. Delete ``job_sources`` and ``jobs`` in bounded row batches, one
        committed transaction per batch, and report progress after each batch
        so health reads and other writers can interleave with the cleanup.
        """
        if delete_cap is not None and int(delete_cap) <= 0:
            return 0
        generations = [
            generation
            for generation in self._deletable_generations(
                current=self.current_generation(),
                keep_count=max(1, int(keep_generations or 1)),
                delete_cap=delete_cap,
            )
            if generation
        ]
        if not generations:
            return 0

        total_generations, row_counts_known, total_rows = self._generation_cleanup_metadata(
            generations,
            on_progress=on_progress,
        )
        deleted_generations = 0
        deleted_rows = 0

        def report_progress(generation: str, *, stage: str = "deleting") -> None:
            if on_progress is not None:
                on_progress(
                    {
                        "stage": stage,
                        "deletedGenerationCount": deleted_generations,
                        "deletedRowCount": deleted_rows,
                        "totalGenerationCount": total_generations,
                        "processedGenerationCount": deleted_generations,
                        "totalGenerationRowCount": total_rows,
                        "generationRowCountKnown": row_counts_known,
                        "generation": generation,
                    }
                )

        def delete_generation_rows(
            sql: str,
            generation: str,
            parameters: tuple[Any, ...],
        ) -> None:
            def on_deleted(deleted: int) -> None:
                nonlocal deleted_rows
                deleted_rows += deleted
                report_progress(generation)

            self._delete_generation_batches(sql, parameters, on_deleted=on_deleted)

        report_progress(generations[0])
        with self.store.bulk_delete_mode():
            for generation in generations:
                # Sources must go before their parent job rows; each batch is a
                # separate transaction so the write lock is released between them.
                delete_generation_rows(
                    _JOB_SOURCES_BATCH_DELETE_SQL,
                    generation,
                    (generation, self.delete_batch_size),
                )
                delete_generation_rows(
                    _JOBS_BATCH_DELETE_SQL,
                    generation,
                    (generation, generation, self.delete_batch_size),
                )
                deleted_generations += 1
                report_progress(generation)
        self._delete_generation_stats(generations)
        return deleted_generations

    def _generation_cleanup_metadata(
        self,
        generations: Sequence[str],
        *,
        on_progress: Callable[[dict[str, Any]], None] | None,
    ) -> tuple[int, bool, int]:
        total_generations = len(generations)
        stats = self._generation_stats(generations)
        missing_stats = [generation for generation in generations if generation not in stats]
        if missing_stats:
            self._report_generation_counting(
                generations[0], total_generations, on_progress=on_progress
            )
            counted = self._count_and_save_generation_stats(missing_stats)
            stats.update(counted)
        row_counts_known = all(generation in stats for generation in generations)
        total_rows = (
            sum(row_count + source_count for row_count, source_count in stats.values())
            if row_counts_known
            else 0
        )
        return total_generations, row_counts_known, total_rows

    def _report_generation_counting(
        self,
        generation: str,
        total_generations: int,
        *,
        on_progress: Callable[[dict[str, Any]], None] | None,
    ) -> None:
        if on_progress is None:
            return
        on_progress(
            {
                "stage": "counting",
                "deletedGenerationCount": 0,
                "deletedRowCount": 0,
                "totalGenerationCount": total_generations,
                "processedGenerationCount": 0,
                "totalGenerationRowCount": 0,
                "generationRowCountKnown": False,
                "generation": generation,
            }
        )

    def _count_and_save_generation_stats(
        self, generations: Sequence[str]
    ) -> dict[str, tuple[int, int]]:
        try:
            counted = self._count_generation_stats(generations)
        except (
            BaluffoStoreError,
            OSError,
            RuntimeError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as exc:
            LOGGER.warning("Could not count jobs-feed generation rows: %s", exc)
            return {}
        if not counted:
            return {}
        try:
            self._save_generation_stats(counted, updated_at=self._now_iso())
        except (
            BaluffoStoreError,
            OSError,
            RuntimeError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as exc:
            LOGGER.warning("Could not persist jobs-feed generation row counts: %s", exc)
        return counted

    def _generation_stats(self, generations: Sequence[str]) -> dict[str, tuple[int, int]]:
        if not generations:
            return {}
        placeholders = ", ".join("?" for _ in generations)
        rows = self.store.execute_read(
            f"""
            SELECT feed_generation, row_count, source_count
            FROM job_feed_generation_stats
            WHERE feed_generation IN ({placeholders})
            """,
            tuple(generations),
        )
        return {
            _clean_text(row.get("feed_generation")): (
                int_or_default(row.get("row_count")),
                int_or_default(row.get("source_count")),
            )
            for row in rows
            if _clean_text(row.get("feed_generation"))
        }

    def _count_generation_stats(self, generations: Sequence[str]) -> dict[str, tuple[int, int]]:
        """Count rows for legacy generations that predate the stats table."""
        if not generations:
            return {}
        placeholders = ", ".join("?" for _ in generations)
        parameters = tuple(generations)
        job_rows = self.store.execute_read(
            f"""
            SELECT feed_generation, COUNT(*) AS row_count
            FROM jobs
            WHERE feed_generation IN ({placeholders})
            GROUP BY feed_generation
            """,
            parameters,
        )
        source_rows = self.store.execute_read(
            f"""
            SELECT feed_generation, COUNT(*) AS source_count
            FROM job_sources
            WHERE feed_generation IN ({placeholders})
            GROUP BY feed_generation
            """,
            parameters,
        )
        counts = {generation: [0, 0] for generation in generations}
        for row in job_rows:
            generation = _clean_text(row.get("feed_generation"))
            if generation in counts:
                counts[generation][0] = int_or_default(row.get("row_count"))
        for row in source_rows:
            generation = _clean_text(row.get("feed_generation"))
            if generation in counts:
                counts[generation][1] = int_or_default(row.get("source_count"))
        return {
            generation: (int(values[0]), int(values[1])) for generation, values in counts.items()
        }

    def _save_generation_stats(
        self,
        stats: Mapping[str, tuple[int, int]],
        *,
        updated_at: str,
    ) -> None:
        if not stats:
            return
        rows = [
            (generation, max(0, int(row_count)), max(0, int(source_count)), str(updated_at))
            for generation, (row_count, source_count) in stats.items()
        ]
        self.store.write(
            lambda conn: conn.executemany(
                """
                INSERT INTO job_feed_generation_stats(
                    feed_generation, row_count, source_count, updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(feed_generation) DO UPDATE SET
                    row_count = excluded.row_count,
                    source_count = excluded.source_count,
                    updated_at = excluded.updated_at
                """,
                rows,
            )
        )

    def _delete_generation_stats(self, generations: Sequence[str]) -> None:
        if not generations:
            return
        try:
            self.store.write(
                lambda conn: conn.executemany(
                    "DELETE FROM job_feed_generation_stats WHERE feed_generation = ?",
                    [(generation,) for generation in generations],
                )
            )
        except (
            BaluffoStoreError,
            OSError,
            RuntimeError,
            sqlite3.Error,
            TypeError,
            ValueError,
        ) as exc:
            LOGGER.warning("Could not delete jobs-feed generation row stats: %s", exc)

    def _delete_generation_batches(
        self,
        sql: str,
        parameters: tuple[Any, ...],
        *,
        on_deleted: Callable[[int], None],
    ) -> None:
        while True:
            deleted = self._delete_generation_batch(sql, parameters)
            if deleted <= 0:
                break
            on_deleted(deleted)

    def _delete_generation_batch(self, sql: str, parameters: tuple[Any, ...]) -> int:
        def delete_rows(conn: Any) -> int:
            cursor = conn.execute(sql, parameters)
            return max(0, int(cursor.rowcount or 0))

        return int(
            self.store.write(
                delete_rows,
                secure_delete=BULK_DELETE_SECURE_DELETE,
            )
        )

    def _deletable_generations(
        self, *, current: str, keep_count: int, delete_cap: int | None
    ) -> list[str]:
        """Generations the retention policy may delete, in deletion order."""
        if keep_count <= 1:
            return self._generations_before_current(current, delete_cap)
        return self._generations_beyond_keep(current, keep_count, delete_cap)

    def _generations_before_current(self, current: str, delete_cap: int | None) -> list[str]:
        """Everything except the current generation, oldest ``feed_generation`` first."""
        if delete_cap is None:
            rows = self.store.execute_read(
                """
                SELECT feed_generation
                FROM jobs
                WHERE feed_generation != ?
                GROUP BY feed_generation
                ORDER BY feed_generation ASC
                """,
                (current,),
            )
        else:
            rows = self.store.execute_read(
                """
                SELECT feed_generation
                FROM jobs
                WHERE feed_generation != ?
                GROUP BY feed_generation
                ORDER BY feed_generation ASC
                LIMIT ?
                """,
                (current, int(delete_cap)),
            )
        return [_clean_text(row.get("feed_generation")) for row in rows]

    def _generations_beyond_keep(
        self, current: str, keep_count: int, delete_cap: int | None
    ) -> list[str]:
        """Everything outside the newest ``keep_count`` generations.

        The current generation is always kept even when it is older than the
        newest ones, and candidates are reversed so the least recently written go
        first -- the same order the single-generation path deletes in.
        """
        all_gens = self.store.execute_read(
            """
            SELECT feed_generation, MAX(updated_at) as latest_updated, MAX(rowid) as latest_rowid
            FROM jobs
            GROUP BY feed_generation
            ORDER BY latest_updated DESC, latest_rowid DESC
            """
        )
        to_keep = {current}
        for row in all_gens:
            gen = _clean_text(row.get("feed_generation"))
            if gen:
                to_keep.add(gen)
            if len(to_keep) >= keep_count:
                break
        candidates = [
            _clean_text(row.get("feed_generation"))
            for row in all_gens
            if _clean_text(row.get("feed_generation")) not in to_keep
        ]
        candidates.reverse()
        if delete_cap is None:
            return candidates
        return candidates[: int(delete_cap)]

    def _records_for_rows(
        self,
        *,
        run_id: str,
        rows: Sequence[Mapping[str, Any]],
        generation: str,
        updated_at: str,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str, str]:
        job_records: list[dict[str, Any]] = []
        source_records: list[dict[str, Any]] = []
        key_counts: dict[str, int] = {}
        normalized_rows = [dict(row) for row in rows if isinstance(row, Mapping)]
        for ordinal, row in enumerate(normalized_rows):
            base_key = _base_job_key(row, ordinal)
            count = key_counts.get(base_key, 0)
            key_counts[base_key] = count + 1
            job_key = base_key if count == 0 else f"{base_key}_dup_{ordinal}"
            source_bundle = _source_bundle(row)
            payload = dict(row)
            payload["sourceBundle"] = []
            row_hash = _sha256_json(row)
            job_records.append(
                {
                    "feed_generation": generation,
                    "job_key": job_key,
                    "row_ordinal": ordinal,
                    "run_id": _clean_text(run_id),
                    "row_hash": row_hash,
                    "title": _clean_text(row.get("title")),
                    "company": _clean_text(row.get("company")),
                    "location": _location_label(row),
                    "status": _clean_text(row.get("status")),
                    "first_seen_at": _clean_text(row.get("firstSeenAt")),
                    "last_seen_at": _clean_text(row.get("lastSeenAt")),
                    "updated_at": updated_at,
                    "payload_json": _json_dumps(payload),
                    "schema_version": JOB_FEED_SCHEMA_VERSION,
                }
            )
            for source_ordinal, source_row in enumerate(source_bundle):
                source_records.append(
                    {
                        "feed_generation": generation,
                        "job_key": job_key,
                        "source_ordinal": source_ordinal,
                        "source_id": _clean_text(source_row.get("sourceId")),
                        "source_job_id": _clean_text(source_row.get("sourceJobId")),
                        "source_name": _clean_text(source_row.get("sourceName"))
                        or _clean_text(source_row.get("source")),
                        "source_key": _source_identity(source_row, source_ordinal),
                        "row_hash": _sha256_json(source_row),
                        "payload_json": _json_dumps(source_row),
                        "schema_version": JOB_FEED_SCHEMA_VERSION,
                        "updated_at": updated_at,
                    }
                )
        return (
            job_records,
            source_records,
            jobs_feed_rows_hash(normalized_rows),
            _sha256_json([_json_loads_object(record["payload_json"]) for record in source_records]),
        )

    def _source_count(self, generation: str) -> int:
        return int_or_default(
            self.store.execute_scalar(
                "SELECT COUNT(*) FROM job_sources WHERE feed_generation = ?",
                (_clean_text(generation),),
            )
        )

    def _source_hash(self, generation: str) -> str:
        rows = self.store.execute_read(
            """
            SELECT payload_json
            FROM job_sources
            WHERE feed_generation = ?
            ORDER BY job_key ASC, source_ordinal ASC
            """,
            (_clean_text(generation),),
        )
        return _sha256_json([_json_loads_object(row.get("payload_json")) for row in rows])

    def _run_id_for_generation(self, generation: str) -> str:
        rows = self.store.execute_read(
            """
            SELECT run_id
            FROM jobs
            WHERE feed_generation = ?
            ORDER BY row_ordinal ASC
            LIMIT 1
            """,
            (_clean_text(generation),),
        )
        return _clean_text(rows[0].get("run_id")) if rows else ""
