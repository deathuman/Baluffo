#!/usr/bin/env python3
"""Drive one real ``run_pipeline`` call from a chosen code tree, cache policy included.

``python src/jobs_fetcher.py --only-sources ...`` and the pipeline CLI set
``force_refresh_all`` for a targeted run: a targeted run has to fetch what it was asked
for, so it bypasses the incremental decisions. That makes the CLI useless for measuring
revalidation, because the question is *whether the decision skips the fetch* and a forced
run answers "no" by construction.

This drives ``run_pipeline`` the way the bridge does -- a filtered loader list, the
shipped cache policy, ``seed_from_existing_output=True`` -- so the arm exercises the same
incremental/revalidation decisions a scheduled run does. ``BALUFFO_DATA_DIR`` is set
before anything under ``src`` is imported, so the registry and state resolve inside the
arm's own directory and the repo's ``data/`` is never read or written.

It is a measurement arm, not a product entrypoint. ``--code`` exists so the same arm can
be run from a second worktree (a before/after study); ``--tag`` names the artifacts.

Usage::

  python tools/measurements/pipeline/live_run.py --code . \\
      --data-dir .tmp/study/arms/a/data --output-dir .tmp/study/arms/a \\
      --sources-from .tmp/study/sources.txt --tag run1 --workers 16
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

STATIC_PREFIX = "static_source::"


def read_names(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def select_loaders(
    loaders: Sequence[tuple[str, Any]],
    *,
    names: Sequence[str] = (),
    only_static: bool = False,
) -> tuple[list[tuple[str, Any]], list[str]]:
    """Filter the tree's own loaders to what this arm was asked to run.

    The selection is by loader name only, so a board that is registered under two studio
    labels is still one loader and cannot be selected twice. Missing names are returned
    rather than dropped silently: a study that lost a board must say so.
    """

    wanted = list(dict.fromkeys(str(name) for name in names))
    if only_static:
        selected = [pair for pair in loaders if str(pair[0]).startswith(STATIC_PREFIX)]
    elif wanted:
        selected = [pair for pair in loaders if str(pair[0]) in set(wanted)]
    else:
        selected = list(loaders)
    present = {str(name) for name, _ in selected}
    missing = [name for name in wanted if name not in present]
    return selected, missing


def summarise_report(
    report: dict[str, Any],
    *,
    elapsed_s: float,
    tag: str,
    arm: str,
    code: Path,
    missing: Iterable[str] = (),
) -> dict[str, Any]:
    """The compact arm summary the study reads, with per-source decisions kept.

    Decisions live on the source's detail row, not on the report's short source row, and
    durations live on the work item. Both are joined by source name here so no caller has
    to re-derive the layout from raw artifacts.
    """

    items = [item for item in (report.get("workItems") or []) if isinstance(item, dict)]
    details: dict[str, dict[str, Any]] = {}
    for row in report.get("sources") or []:
        if not isinstance(row, dict):
            continue
        for detail in row.get("details") or []:
            if isinstance(detail, dict):
                details[str(row.get("name") or detail.get("name") or "")] = detail
    sources: list[dict[str, Any]] = []
    for item in items:
        name = str(item.get("name") or "")
        detail = details.get(name) or {}
        counts = (item.get("progress") or {}).get("counts") or {}
        sources.append(
            {
                "name": name,
                # The state row the incremental decision reads is keyed by the source's
                # display name, not by this loader name; keep both so a study never has to
                # guess which one a cohort priced.
                "sourceName": str(detail.get("name") or ""),
                "status": str(item.get("status") or ""),
                "durationMs": int(item.get("durationMs") or 0),
                "keptCount": int(counts.get("keptCount") or 0),
                "fetchedCount": int(counts.get("fetchedCount") or 0),
                "cacheDecision": str(detail.get("cacheDecision") or ""),
                "cacheDecisionReason": str(detail.get("cacheDecisionReason") or ""),
                "classification": str(detail.get("classification") or ""),
                "error": str(detail.get("error") or ""),
                "httpStatus": int(detail.get("httpStatus") or 0),
            }
        )
    durations = [row["durationMs"] for row in sources]
    summary = report.get("summary") or {}
    started = datetime.fromisoformat(report["startedAt"]) if report.get("startedAt") else None
    finished = datetime.fromisoformat(report["finishedAt"]) if report.get("finishedAt") else None
    reasons: dict[str, int] = {}
    for row in sources:
        reason = row["cacheDecisionReason"] or row["status"] or "?"
        reasons[reason] = reasons.get(reason, 0) + 1
    return {
        "tag": tag,
        "arm": arm,
        "code": str(code),
        "wallS": round((finished - started).total_seconds(), 1) if started and finished else 0.0,
        "processWallS": round(float(elapsed_s), 1),
        "sourceCount": len(sources),
        "sourceWorkS": round(sum(durations) / 1000.0, 1),
        "medianSourceMs": int(statistics.median(durations)) if durations else 0,
        "p90SourceMs": int(sorted(durations)[int(len(durations) * 0.9)]) if durations else 0,
        "kept": sum(row["keptCount"] for row in sources),
        "notModifiedCount": int(summary.get("notModifiedCount") or 0),
        "revalidatedCount": int(summary.get("revalidatedCount") or 0),
        "cacheSkippedCount": int(summary.get("cacheSkippedCount") or 0),
        "failedSources": int(summary.get("failedSources") or 0),
        "excludedSources": int(summary.get("excludedSources") or 0),
        "outputCount": int(summary.get("outputCount") or 0),
        "cacheDecisionCounts": summary.get("cacheDecisionCounts") or {},
        "decisionReasonCounts": dict(sorted(reasons.items())),
        "notModifiedItems": sorted(
            row["name"] for row in sources if row["cacheDecisionReason"] == "not_modified_304"
        ),
        "revalidatedItems": sorted(
            row["name"] for row in sources if row["cacheDecision"] == "revalidate_only"
        ),
        "missingLoaders": sorted(str(name) for name in missing),
        "sources": sources,
    }


def execute(
    *,
    code: Path,
    data_dir: Path,
    output_dir: Path,
    tag: str,
    workers: int,
    names: Sequence[str] = (),
    only_static: bool = False,
    loader_fn: Callable[..., Any],
    run_pipeline_fn: Callable[..., Any],
    keep_report: bool = False,
) -> dict[str, Any]:
    """Run one arm and write its summaries. The pipeline callables are injected so a
    test can drive the orchestration without a tree or a network."""

    loaders, missing = select_loaders(
        list(loader_fn(social_enabled=False)), names=names, only_static=only_static
    )
    if not loaders:
        raise SystemExit("no loaders selected; check --sources-from / --only-static")
    started = time.perf_counter()
    report = run_pipeline_fn(
        output_dir=output_dir,
        source_loaders=loaders,
        seed_from_existing_output=True,
        max_workers=max(1, int(workers)),
        show_progress=False,
    )
    elapsed = time.perf_counter() - started
    payload = summarise_report(
        report,
        elapsed_s=elapsed,
        tag=tag,
        arm=str(data_dir),
        code=code,
        missing=missing,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / f"{tag}.run.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if keep_report:
        (output_dir / f"{tag}.report.json").write_text(
            json.dumps(report, indent=1), encoding="utf-8"
        )
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--code", default=".", help="Tree to run; a worktree for a before arm.")
    parser.add_argument(
        "--data-dir", required=True, help="Arm's BALUFFO_DATA_DIR (state lives here)."
    )
    parser.add_argument(
        "--output-dir", default="", help="Where the summaries go; defaults to the data dir."
    )
    parser.add_argument("--tag", default="run", help="Artifact name for this arm.")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--sources-from", default="", help="File of loader names, one per line.")
    parser.add_argument("--only-static", action="store_true", help="Run every static loader.")
    parser.add_argument("--keep-report", action="store_true", help="Also write the raw report.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    code = Path(args.code).resolve()
    if not (code / "src" / "jobs" / "pipeline.py").is_file():
        raise SystemExit(f"{code} does not contain src/jobs/pipeline.py")
    data_dir = Path(args.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir = Path(args.output_dir).resolve() if args.output_dir else data_dir
    names = read_names(Path(args.sources_from)) if args.sources_from else []

    # Load-bearing order: the data dir is resolved by the tree's own storage config at
    # import time, so it must be set, and the tree made importable, before src loads.
    os.environ["BALUFFO_DATA_DIR"] = str(data_dir)
    os.chdir(code)
    sys.path.insert(0, str(code))
    from src.jobs.pipeline import default_source_loaders, run_pipeline

    payload = execute(
        code=code,
        data_dir=data_dir,
        output_dir=output_dir,
        tag=args.tag,
        workers=args.workers,
        names=names,
        only_static=bool(args.only_static),
        loader_fn=default_source_loaders,
        run_pipeline_fn=run_pipeline,
        keep_report=bool(args.keep_report),
    )
    print(json.dumps({key: value for key, value in payload.items() if key != "sources"}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
