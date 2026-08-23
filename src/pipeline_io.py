#!/usr/bin/env python3
"""IO helpers for jobs pipeline outputs."""

from __future__ import annotations

import gzip
import json
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Sequence
from pathlib import Path
from typing import Any

from src.shared.json_io import gzip_backed_json_storage_path
from src.storage_json_metrics import record_json_text_write

RawJob = dict[str, Any]

_STALE_TMP_AGE_SECONDS = 60 * 60


def _trusted_local_path(path: Path | str) -> Path:
    return Path(path).expanduser()


def _storage_target_path(path: Path | str) -> Path:
    return gzip_backed_json_storage_path(_trusted_local_path(path))


def _read_text_path(path: Path) -> str:
    path = _trusted_local_path(path)
    if path.suffix == ".gz":
        # codeql[py/path-injection] Pipeline IO only reads trusted local runtime artifacts.
        with gzip.open(path, mode="rt", encoding="utf-8") as handle:
            return handle.read()
    # codeql[py/path-injection] Pipeline IO only reads trusted local runtime artifacts.
    return path.read_text(encoding="utf-8")


def _restore_existing_rows(
    rows: Iterable[dict[str, Any]],
    *,
    fetched_at: str,
    canonicalize_job: Callable[..., Any],
    clean_text: Callable[[Any], str],
    canonical_job_cls: type[Any] | None = None,
) -> list[Any]:
    """Restore canonical jobs from raw/dict rows (shared by sidecar + blob paths)."""
    restored: list[Any] = []
    for row in rows:
        candidate = _existing_output_row_to_canonical(
            row,
            fetched_at=fetched_at,
            canonicalize_job=canonicalize_job,
            clean_text=clean_text,
            canonical_job_cls=canonical_job_cls,
        )
        if candidate is not None:
            restored.append(candidate)
    return restored


def read_existing_output(
    json_path: Path,
    fetched_at: str,
    *,
    canonicalize_job: Callable[..., Any],
    clean_text: Callable[[Any], str],
    canonical_job_cls: type[Any] | None = None,
    row_predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> list[Any]:
    # ponytail: sidecar-only read — the legacy json.loads fallback on the
    # 60+ MB feed blob (a ~3x parse peak) is gone; a missing sidecar cold-seeds
    # ([]) and the pipeline rebuilds from the lifecycle carry, the source of
    # truth. Deleting a sidecar file is safe but cold-seeds the next run.
    sidecar_rows = read_pipeline_rows_sidecar(Path(json_path))
    if sidecar_rows is None:
        return []
    return _restore_existing_rows(
        (row for row in sidecar_rows if row_predicate is None or row_predicate(row)),
        fetched_at=fetched_at,
        canonicalize_job=canonicalize_job,
        clean_text=clean_text,
        canonical_job_cls=canonical_job_cls,
    )


def _existing_output_row_to_canonical(
    row: dict[str, Any],
    *,
    fetched_at: str,
    canonicalize_job: Callable[..., Any],
    clean_text: Callable[[Any], str],
    canonical_job_cls: type[Any] | None = None,
) -> Any:
    dedup_key = clean_text(row.get("dedupKey"))
    if canonical_job_cls is not None and row.get("availabilityId") and row.get("jobLink"):
        candidate = canonical_job_cls.from_mapping(row)
        if dedup_key and not candidate.dedupKey:
            candidate = canonical_job_cls.from_mapping({**row, "dedupKey": dedup_key})
        return candidate
    normalized = canonicalize_job(
        row,
        source=clean_text(row.get("source")) or "previous_output",
        fetched_at=fetched_at,
    )
    if not normalized:
        return None
    if canonical_job_cls is not None and isinstance(normalized, canonical_job_cls):
        if dedup_key and not normalized.dedupKey:
            normalized = canonical_job_cls.from_mapping(
                {**normalized.to_dict(), "dedupKey": dedup_key}
            )
        return normalized
    if isinstance(normalized, dict):
        if dedup_key:
            normalized["dedupKey"] = dedup_key
        return normalized
    # ponytail: duck-typed fallback — accept non-dict objects (e.g. CanonicalJob
    # when canonical_job_cls was not provided) instead of silently dropping them.
    return normalized


def serialize_rows_for_json(rows: Sequence[RawJob], fields: Sequence[str]) -> str:
    payload = [{field: row.get(field, "") for field in fields} for row in rows]
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _read_streamed_text(tmp_path: Path, target: Path) -> str:
    if target.suffix == ".gz":
        with gzip.open(tmp_path, mode="rt", encoding="utf-8") as handle:
            return handle.read()
    return tmp_path.read_text(encoding="utf-8")


def _write_streamed_tmp(tmp_path: Path, target: Path, stream_fn: Callable[[Any], None]) -> int:
    write_count = 0

    class _Counting:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        def write(self, text: str) -> int:
            nonlocal write_count
            write_count += len(text)
            return int(self._inner.write(text))

        def flush(self) -> None:
            self._inner.flush()

    if target.suffix == ".gz":
        with gzip.open(tmp_path, mode="wt", encoding="utf-8", newline="") as handle:
            stream_fn(_Counting(handle))
    else:
        with open(tmp_path, "w", encoding="utf-8", newline="") as handle:
            stream_fn(_Counting(handle))
    return write_count


def _sweep_stale_tmp(target: Path) -> None:
    """Remove `target.*.tmp` siblings left by interrupted (e.g. SIGKILLed) writes.

    Age-gated so a live concurrent writer's temp is never touched; the pipeline
    lock serializes writers of the same artifact anyway. ponytail: directory
    scan per write is cheap next to the multi-MB payloads involved.
    """
    try:
        now = time.time()
        for leftover in target.parent.glob(f"{target.name}.*.tmp"):
            try:
                if now - leftover.stat().st_mtime > _STALE_TMP_AGE_SECONDS:
                    leftover.unlink()
            except OSError:
                pass
    except OSError:
        pass


def write_streamed_text_if_changed(path: Path, stream_fn: Callable[[Any], None]) -> bool:
    """Atomically write content produced by ``stream_fn(handle)`` to path.

    ``stream_fn`` writes text to the handle; the temp result is compared with
    the existing file (size gate first) and only replaced when different.
    Gzip-backed targets (``.gz`` storage mapping) are compressed like the
    regular text writers. Keeps peak memory flat for large artifacts (no
    full-text string, no serialized copy) — the equivalent
    ``write_text_if_changed`` path peaked at ~355 MiB for the 40k-row unified
    output.
    """
    target = _storage_target_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_name(f"{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    _sweep_stale_tmp(target)
    try:
        write_count = _write_streamed_tmp(tmp_path, target, stream_fn)
        existing_size: int = -1
        try:
            existing_size = target.stat().st_size
        except OSError:
            pass
        if tmp_path.stat().st_size == existing_size:
            try:
                existing_text = _read_text_path(target)
            except OSError:
                existing_text = None
            if existing_text is not None and existing_text == _read_streamed_text(tmp_path, target):
                return False
        write_started_at = time.perf_counter()
        os.replace(tmp_path, target)
        record_json_text_write(
            path=path,
            target=target,
            text="",
            write_started_at=write_started_at,
            uncompressed_size_bytes=write_count,
        )
        return True
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass


_PIPELINE_ROWS_SIDECAR_SUFFIX = ".rows.jsonl.gz"
_FETCHED_ROWS_SIDECAR_NAME = ".pipeline-fetched-rows.jsonl"


def _pipeline_rows_sidecar_path(path: Path) -> Path:
    path = _trusted_local_path(path)
    return path.with_name(path.name + _PIPELINE_ROWS_SIDECAR_SUFFIX)


def _fetched_rows_sidecar_path(output_dir: Path) -> Path:
    # ponytail: ephemeral sidecar for freshly fetched rows during the fetch stage
    # — streamed incrementally so the ThreadPoolExecutor window does not pin 25k
    # CanonicalJob objects while 2 317 sources run. Plain JSONL (no gzip) so
    # concurrent worker threads can append cheaply; gzipped at finalize time only.
    return _trusted_local_path(output_dir) / _FETCHED_ROWS_SIDECAR_NAME


def write_pipeline_rows_sidecar(path: Path, rows: Sequence[RawJob]) -> None:
    """Write ``<path>.rows.jsonl.gz`` — one row dict per line, gzip-wrapped.

    The sidecar lets ``read_existing_output`` stream rows one at a time instead
    of ``json.loads``-ing a 60+ MB blob and holding Python's parse tree in memory.
    """

    target = _pipeline_rows_sidecar_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_name(f"{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    _sweep_stale_tmp(target)
    try:
        # codeql[py/path-injection] Sidecar lives beside a trusted pipeline artifact.
        with gzip.open(tmp_path, mode="wt", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
                handle.write("\n")
        os.replace(tmp_path, target)
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass


def read_pipeline_rows_sidecar(path: Path) -> Iterator[dict[str, Any]] | None:
    """Yield row dicts from the sidecar if present, else ``None``.

    Caller consumes lazily; keeps peak RSS at max-single-row instead of file size.
    """

    target = _pipeline_rows_sidecar_path(path)
    if not target.exists():
        return None

    def _iter() -> Iterator[dict[str, Any]]:
        # codeql[py/path-injection] Sidecar lives beside a trusted pipeline artifact.
        with gzip.open(target, mode="rt", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                row = json.loads(line)
                if isinstance(row, dict):
                    yield row

    return _iter()


class IncrementalFetchedRowsWriter:
    """Thread-safe incremental JSONL writer for freshly fetched canonical rows.

    Keeps fetch-stage RSS flat: workers append compact JSONL lines under a lock
    instead of extending a 25k+ CanonicalJob list that lives through the whole
    ThreadPoolExecutor window. The file is plain text (no gzip) so appends are
    cheap and lock-hold is minimal. Caller later streams it back via
    ``read_fetched_rows_sidecar``.
    """

    def __init__(self, output_dir: Path) -> None:
        self.path = _fetched_rows_sidecar_path(output_dir)
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Truncate stale sidecar from a prior run / crash.
        try:
            self.path.write_text("", encoding="utf-8")
        except OSError:
            pass
        self._count = 0

    def append_canonical_jobs(self, rows: Sequence[Any]) -> None:
        if not rows:
            return
        # Serialize outside the lock to keep it brief.
        lines: list[str] = []
        for row in rows:
            try:
                payload = row.to_dict() if hasattr(row, "to_dict") else dict(row)
            except Exception:
                continue
            lines.append(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
        if not lines:
            return
        text = "\n".join(lines) + "\n"
        with self._lock:
            with open(self.path, "a", encoding="utf-8", newline="\n") as handle:
                handle.write(text)
            self._count += len(lines)

    @property
    def count(self) -> int:
        return int(self._count)


def read_fetched_rows_sidecar(
    output_dir: Path,
    *,
    canonical_job_cls: Any | None = None,
) -> Iterator[Any]:
    """Yield CanonicalJob objects from the incremental fetched sidecar."""

    path = _fetched_rows_sidecar_path(output_dir)
    if not path.exists():
        return
    # codeql[py/path-injection] Sidecar lives beside a trusted pipeline artifact.
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            if canonical_job_cls is not None:
                try:
                    yield canonical_job_cls.from_mapping(payload)
                except Exception:
                    continue
            else:
                yield payload


def cleanup_fetched_rows_sidecar(output_dir: Path) -> None:
    try:
        _fetched_rows_sidecar_path(output_dir).unlink(missing_ok=True)
    except OSError:
        pass


def write_text_if_changed(path: Path, text: str) -> bool:
    target = _storage_target_path(path)
    try:
        existing = _read_text_path(target)
        if existing == text:
            return False
    except OSError:
        pass
    write_started_at = time.perf_counter()
    _write_atomic_text(target, text, attempts=18, sleep_base_s=0.012)
    record_json_text_write(
        path=path,
        target=target,
        text=text,
        write_started_at=write_started_at,
    )
    return True


def _write_text_with_retry(path: Path, text: str, *, attempts: int, sleep_base_s: float) -> None:
    path = _trusted_local_path(path)
    last_error: OSError | None = None
    for attempt in range(max(1, int(attempts or 1))):
        try:
            # codeql[py/path-injection] Pipeline IO only writes trusted local runtime artifacts.
            path.write_text(text, encoding="utf-8")
            return
        except OSError as exc:
            last_error = exc
            if attempt >= max(1, int(attempts or 1)) - 1:
                break
            time.sleep(float(sleep_base_s) * float(attempt + 1))
    if last_error is not None:
        raise last_error


def _write_atomic_text(
    path: Path,
    text: str,
    *,
    attempts: int = 1,
    sleep_base_s: float = 0.0,
    fallback_to_in_place: bool = False,
) -> None:
    """Write text to path via temp file + replace, with optional retry/fallback."""
    path = _trusted_local_path(path)
    # codeql[py/path-injection] Pipeline IO only creates trusted local runtime artifact parents.
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    _sweep_stale_tmp(path)
    try:
        if path.suffix == ".gz":
            # codeql[py/path-injection] Pipeline IO temp files stay next to trusted artifacts.
            with gzip.open(tmp_path, mode="wt", encoding="utf-8") as handle:
                handle.write(text)
        else:
            # codeql[py/path-injection] Pipeline IO temp files stay next to trusted artifacts.
            tmp_path.write_text(text, encoding="utf-8")
        last_error: OSError | None = None
        for attempt in range(max(1, int(attempts or 1))):
            try:
                # codeql[py/path-injection] Atomic replace targets trusted local runtime artifacts.
                os.replace(tmp_path, path)
                last_error = None
                return
            except OSError as exc:
                last_error = exc
                if attempt >= max(1, int(attempts or 1)) - 1:
                    break
                time.sleep(float(sleep_base_s) * float(attempt + 1))
        if fallback_to_in_place and last_error is not None:
            _write_text_with_retry(
                path,
                text,
                attempts=max(2, min(6, max(1, int(attempts or 1)) // 2)),
                sleep_base_s=max(0.01, float(sleep_base_s)),
            )
            last_error = None
            return
        if last_error is not None:
            raise last_error
    finally:
        try:
            # codeql[py/path-injection] Cleanup only removes the trusted sibling temp file created above.
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass


def write_atomic_if_changed(path: Path, text: str) -> bool:
    """Write text to path atomically (via temp file + rename) so readers never see partial content."""
    return write_text_if_changed(path, text)


def write_hot_text_if_changed(path: Path, text: str) -> bool:
    """Write frequently polled task/report files with retry and in-place fallback on Windows locks."""
    path = _trusted_local_path(path)
    try:
        # codeql[py/path-injection] Hot writes only compare trusted local runtime artifacts.
        existing = path.read_text(encoding="utf-8")
        if existing == text:
            return False
    except OSError:
        pass
    write_started_at = time.perf_counter()
    _write_atomic_text(path, text, attempts=18, sleep_base_s=0.012, fallback_to_in_place=True)
    record_json_text_write(
        path=path,
        target=path,
        text=text,
        write_started_at=write_started_at,
    )
    return True
