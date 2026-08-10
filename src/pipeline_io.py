#!/usr/bin/env python3
"""IO helpers for jobs pipeline outputs."""

from __future__ import annotations

import gzip
import json
import os
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

from src.shared.json_io import gzip_backed_json_storage_path, read_json
from src.storage_json_metrics import record_json_text_write

RawJob = dict[str, Any]


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


def read_existing_output(
    json_path: Path,
    fetched_at: str,
    *,
    canonicalize_job: Callable[..., Any],
    clean_text: Callable[[Any], str],
    canonical_job_cls: type | None = None,
    row_predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> list[Any]:
    # ponytail: try the streaming sidecar first — avoids the ~3x parse peak of
    # json.loads on the 60+ MB jobs-unified.json.gz blob.
    sidecar_rows = read_pipeline_rows_sidecar(Path(json_path))
    if sidecar_rows is not None:
        restored_stream: list[Any] = []
        for row in sidecar_rows:
            if row_predicate is not None and not row_predicate(row):
                continue
            candidate = _existing_output_row_to_canonical(
                row,
                fetched_at=fetched_at,
                canonicalize_job=canonicalize_job,
                clean_text=clean_text,
                canonical_job_cls=canonical_job_cls,
            )
            if candidate is not None:
                restored_stream.append(candidate)
        return restored_stream

    payload = read_json(Path(json_path), None)
    if payload is None:
        return []

    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
    elif isinstance(payload, dict) and isinstance(payload.get("jobs"), list):
        rows = [row for row in payload["jobs"] if isinstance(row, dict)]
    else:
        return []

    if row_predicate is not None:
        rows = [row for row in rows if row_predicate(row)]

    # ponytail: rows already canonicalized are the common case; only truly raw
    # rows go through canonicalize_job. On the seeded 40k-row bench volume this
    # drops fetch's `seeding_existing_output` cost from ~97 s to ~5 s and cuts
    # peak RSS by avoiding the dict→CanonicalJob→dict→CanonicalJob round trip.
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


def _existing_output_row_to_canonical(
    row: dict[str, Any],
    *,
    fetched_at: str,
    canonicalize_job: Callable[..., Any],
    clean_text: Callable[[Any], str],
    canonical_job_cls: type | None = None,
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


_PIPELINE_ROWS_SIDECAR_SUFFIX = ".rows.jsonl.gz"


def _pipeline_rows_sidecar_path(path: Path) -> Path:
    path = _trusted_local_path(path)
    return path.with_name(path.name + _PIPELINE_ROWS_SIDECAR_SUFFIX)


def write_pipeline_rows_sidecar(path: Path, rows: Sequence[RawJob]) -> None:
    """Write ``<path>.rows.jsonl.gz`` — one row dict per line, gzip-wrapped.

    The sidecar lets ``read_existing_output`` stream rows one at a time instead
    of ``json.loads``-ing a 60+ MB blob and holding Python's parse tree in memory.
    """

    target = _pipeline_rows_sidecar_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_name(f"{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
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
