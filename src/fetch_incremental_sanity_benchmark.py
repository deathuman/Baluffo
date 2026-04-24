#!/usr/bin/env python3
"""Run an isolated two-pass fetch incremental benchmark under _out/."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any, cast

from src.jobs.interfaces import SourceLoader

DEFAULT_BENCHMARK_SOURCES = [
    "greenhouse_boards",
    "lever_sources",
    "ashby_sources",
    "teamtailor_sources",
    "smartrecruiters_sources",
    "workable_sources",
    "recruitee_sources",
    "pinpoint_sources",
    "breezy_sources",
    "jazzhr_sources",
    "personio_sources",
    "static_source::static:name:little chicken",
]


def _ensure_repo_on_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    return root


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an isolated two-pass fetch incremental benchmark."
    )
    parser.add_argument(
        "--output-dir",
        default="_out/perf-sanity-fetch-incremental",
        help="Isolated output dir for benchmark artifacts.",
    )
    parser.add_argument("--timeout", type=int, default=15)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--backoff", type=float, default=1.2)
    parser.add_argument("--max-workers", type=int, default=12)
    parser.add_argument("--max-per-domain", type=int, default=3)
    parser.add_argument("--adapter-http-concurrency", type=int, default=48)
    parser.add_argument("--static-detail-concurrency", type=int, default=10)
    parser.add_argument("--sources", nargs="*", default=DEFAULT_BENCHMARK_SOURCES)
    parser.add_argument(
        "--keep-existing-output",
        action="store_true",
        help="Reuse the output dir instead of removing it before pass one.",
    )
    return parser.parse_args(argv)


def _as_list(value: object) -> list[Any]:
    return value if isinstance(value, list) else []


def _select_loaders(source_names: list[str]) -> tuple[list[tuple[str, SourceLoader]], list[str]]:
    from src.jobs import adapters as adapters_pkg
    from src.jobs.adapters import static as static_adapter
    from src.jobs.text_utils import clean_text

    available: dict[str, SourceLoader] = {
        name: loader for name, loader in adapters_pkg.default_source_loaders(social_enabled=False)
    }
    for static_name, static_loader in static_adapter.build_static_source_loaders():
        available.setdefault(static_name, static_loader)
    for extracted_name, extracted_loader in adapters_pkg.EXTRACTED_ADAPTERS.items():
        available.setdefault(extracted_name, cast(SourceLoader, extracted_loader))
    selected: list[tuple[str, SourceLoader]] = []
    missing: list[str] = []
    for name in source_names:
        normalized_name = clean_text(name)
        loader: SourceLoader | None = available.get(name)
        if loader is None and normalized_name:
            loader = next(
                (
                    candidate_loader
                    for candidate_name, candidate_loader in available.items()
                    if clean_text(candidate_name) == normalized_name
                ),
                None,
            )
        if loader is None:
            missing.append(name)
            continue
        selected.append((normalized_name or name, loader))
    return selected, missing


def _family_summary(report: dict[str, object], source_names: list[str]) -> dict[str, object]:
    rows = [
        row
        for row in _as_list(report.get("sources"))
        if isinstance(row, dict) and str(row.get("name") or "") in source_names
    ]
    family: dict[str, object] = {}
    for row in rows:
        name = str(row.get("name") or "")
        family[name] = {
            "status": row.get("status"),
            "durationMs": int(row.get("durationMs") or 0),
            "keptCount": int(row.get("keptCount") or 0),
            "boardCount": int(row.get("boardCount") or 0)
            if row.get("boardCount") is not None
            else None,
            "boardCacheDecisionCounts": dict(row.get("boardCacheDecisionCounts") or {}),
            "boardSkippedCount": int(row.get("boardSkippedCount") or 0)
            if row.get("boardSkippedCount") is not None
            else None,
            "boardRevalidatedCount": int(row.get("boardRevalidatedCount") or 0)
            if row.get("boardRevalidatedCount") is not None
            else None,
            "boardNotModifiedCount": int(row.get("boardNotModifiedCount") or 0)
            if row.get("boardNotModifiedCount") is not None
            else None,
            "boardRefreshedCount": int(row.get("boardRefreshedCount") or 0)
            if row.get("boardRefreshedCount") is not None
            else None,
            "error": str(row.get("error") or ""),
        }
    return family


def _run_pass(
    output_dir: Path,
    selected_loaders: list[tuple[str, SourceLoader]],
    args: argparse.Namespace,
):
    from src.jobs.pipeline import run_pipeline

    return run_pipeline(
        output_dir=output_dir,
        source_loaders=selected_loaders,
        timeout_s=int(args.timeout),
        retries=int(args.retries),
        backoff_s=float(args.backoff),
        max_workers=int(args.max_workers),
        max_per_domain=int(args.max_per_domain),
        adapter_http_concurrency=int(args.adapter_http_concurrency),
        static_detail_concurrency=int(args.static_detail_concurrency),
        show_progress=False,
    )


def main(argv: list[str] | None = None) -> int:
    root = _ensure_repo_on_path()
    args = parse_args(argv)
    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = (root / output_dir).resolve()
    if output_dir.exists() and not bool(args.keep_existing_output):
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    os.environ["BALUFFO_DATA_DIR"] = str(output_dir)

    selected_loaders, missing = _select_loaders([str(name) for name in args.sources])
    if missing:
        raise SystemExit(f"Unknown sources for benchmark: {', '.join(missing)}")

    first = _run_pass(output_dir, selected_loaders, args)
    second = _run_pass(output_dir, selected_loaders, args)

    payload = {
        "outputDir": str(output_dir),
        "sources": [name for name, _loader in selected_loaders],
        "firstRun": {
            "summary": dict(first.get("summary") or {}),
            "runtime": dict(first.get("runtime") or {}),
            "familySummary": _family_summary(first, [name for name, _loader in selected_loaders]),
        },
        "secondRun": {
            "summary": dict(second.get("summary") or {}),
            "runtime": dict(second.get("runtime") or {}),
            "familySummary": _family_summary(second, [name for name, _loader in selected_loaders]),
        },
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
