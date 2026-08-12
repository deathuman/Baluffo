"""Tests for the pipeline JSONL rows sidecar.

Phase 3 memory optimization: ``write_pipeline_rows_sidecar`` produces a
``<json>.rows.jsonl.gz`` file next to the main blob, and ``read_existing_output``
prefers it (streaming one row at a time) over ``json.loads`` of the whole blob.
The material win is on large seeds where the whole-blob parse peaks at ~3x the
final row list; the streaming path peaks at the largest single row plus the
accumulated CanonicalJob list.
"""

from __future__ import annotations

from pathlib import Path

from src.jobs.models import CanonicalJob
from src.jobs.pipeline_run_setup import canonicalize_existing_output_row
from src.jobs.text_utils import clean_text
from src.pipeline_io import (
    _pipeline_rows_sidecar_path,
    read_existing_output,
    write_atomic_if_changed,
    write_pipeline_rows_sidecar,
)


def _sample_rows() -> list[dict]:
    return [
        {
            "id": str(i),
            "title": f"Engineer {i}",
            "company": "Acme",
            "jobLink": f"https://example.com/{i}",
            "availabilityId": f"aid{i}",
            "dedupKey": f"dk{i}",
            "source": "static_source::test",
        }
        for i in range(5)
    ]


def test_sidecar_roundtrip_streams_rows(tmp_path: Path) -> None:
    logical = tmp_path / "jobs-unified.json"
    rows = _sample_rows()

    # Write the sidecar; main blob not required for the streaming read.
    write_pipeline_rows_sidecar(logical, rows)
    sidecar = _pipeline_rows_sidecar_path(logical)
    assert sidecar.exists()
    assert sidecar.suffix == ".gz"

    got = read_existing_output(
        logical,
        "now",
        canonicalize_job=canonicalize_existing_output_row,
        clean_text=clean_text,
        canonical_job_cls=CanonicalJob,
    )
    assert len(got) == len(rows)
    assert all(isinstance(r, CanonicalJob) for r in got)


def test_sidecar_missing_cold_seeds_without_blob_parse(tmp_path: Path) -> None:
    logical = tmp_path / "jobs-unified.json"
    rows = _sample_rows()

    # Write only the JSON blob (via write_atomic_if_changed which gzip-wraps).
    import json as _json

    write_atomic_if_changed(logical, _json.dumps(rows))
    assert not _pipeline_rows_sidecar_path(logical).exists()

    # The legacy blob fallback is removed: the module no longer even imports
    # read_json, and a missing sidecar must cold-seed ([]).
    import src.pipeline_io as pipeline_io_mod

    assert not hasattr(pipeline_io_mod, "read_json")

    got = read_existing_output(
        logical,
        "now",
        canonicalize_job=canonicalize_existing_output_row,
        clean_text=clean_text,
        canonical_job_cls=CanonicalJob,
    )
    assert got == []


def test_sidecar_row_predicate_applies(tmp_path: Path) -> None:
    logical = tmp_path / "jobs-unified.json"
    rows = _sample_rows()
    write_pipeline_rows_sidecar(logical, rows)

    # Keep only rows with odd ids.
    got = read_existing_output(
        logical,
        "now",
        canonicalize_job=canonicalize_existing_output_row,
        clean_text=clean_text,
        canonical_job_cls=CanonicalJob,
        row_predicate=lambda r: int(r.get("id", "0")) % 2 == 1,
    )
    kept_ids = sorted(int(r.id) for r in got)
    assert kept_ids == [1, 3]


def test_sidecar_skips_malformed_lines(tmp_path: Path) -> None:
    logical = tmp_path / "jobs-unified.json"
    # Manually craft a sidecar with blank lines and a non-dict row
    sidecar = _pipeline_rows_sidecar_path(logical)
    sidecar.parent.mkdir(parents=True, exist_ok=True)
    import gzip as _gzip
    import json as _json

    with _gzip.open(sidecar, "wt", encoding="utf-8", newline="\n") as fh:
        fh.write(_json.dumps(_sample_rows()[0]) + "\n")
        fh.write("\n")  # blank line — skipped
        fh.write(_json.dumps(["not-a-dict"]) + "\n")  # list — skipped
        fh.write(_json.dumps(_sample_rows()[1]) + "\n")

    got = read_existing_output(
        logical,
        "now",
        canonicalize_job=canonicalize_existing_output_row,
        clean_text=clean_text,
        canonical_job_cls=CanonicalJob,
    )
    assert len(got) == 2
