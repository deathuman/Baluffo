"""Rehydrate pipeline row segments from their sidecars.

AI boundary owns: seeded/fetched row rehydration before pipeline finalization.
AI boundary implement in: this file for sidecar row segments; finalize and report assembly stay in sibling leaves.
AI boundary search before contracts: pipeline finalize row inputs and fetched-rows sidecar behavior.
AI boundary verify: `npm run lint:repo-guardrails` plus focused pipeline finalization tests.
"""

from __future__ import annotations

from typing import Any

from src.jobs.text_utils import clean_text
from src.pipeline_io import read_existing_output


def hydrate_row_segments(setup: Any) -> list:
    """Rehydrate [seeded, fetched] row segments from their sidecars.

    ponytail: owning scope — the returned list is a local of THIS function,
    not of run_pipeline, so finalize's dead-ref trim can actually free it.
    """
    from src.jobs.models import CanonicalJob as _CJ
    from src.jobs.pipeline_run_setup import canonicalize_existing_output_row
    from src.pipeline_io import (
        cleanup_fetched_rows_sidecar,
        read_fetched_rows_sidecar,
    )

    rows: list = []
    if setup.effective_seed_from_existing_output:
        try:
            seeded = read_existing_output(
                setup.paths.json_path,
                setup.started_at,
                canonicalize_job=canonicalize_existing_output_row,
                clean_text=clean_text,
                canonical_job_cls=_CJ,
            )
            rows.extend(seeded)
            setup.seeded_row_count = len(seeded)
            del seeded
        except Exception:
            pass
    fetched_writer = getattr(setup, "fetched_rows_writer", None)
    if fetched_writer is not None and int(getattr(fetched_writer, "count", 0) or 0) > 0:
        try:
            buf: list = []
            for job in read_fetched_rows_sidecar(setup.paths.output_dir, canonical_job_cls=_CJ):
                buf.append(job)
                if len(buf) >= 5000:
                    rows.extend(buf)
                    buf = []
            if buf:
                rows.extend(buf)
        except Exception:
            pass
        try:
            cleanup_fetched_rows_sidecar(setup.paths.output_dir)
        except Exception:
            pass
        try:
            object.__setattr__(setup, "fetched_rows_writer", None)
        except Exception:
            pass
    return rows
