#!/usr/bin/env python3
"""Report funnel: game studios sheet → registry → pipeline outcome.

Reads source-discovery-candidates.json, source-registry-active.json, and
jobs-fetch-report.json from the data dir and prints counts for sheet-derived
sources: in_sheet, in_registry, ran_in_pipeline, ok, failed.

Usage:
  python scripts/game_studios_sheet_funnel.py [--data-dir PATH]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.baluffo_config import get_storage_defaults


def _load_json(path: Path, default: list | dict) -> list | dict:
    if not path.exists():
        return default
    try:
        raw = path.read_text(encoding="utf-8")
        return json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return default


def _norm_url(url: str) -> str:
    return (url or "").strip().lower().rstrip("/")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Game studios sheet funnel: sheet → registry → pipeline"
    )
    parser.add_argument("--data-dir", type=Path, help="Data directory (default from config)")
    args = parser.parse_args()
    defaults = get_storage_defaults()
    data_dir = Path(args.data_dir or defaults.get("data_dir", "data")).expanduser().resolve()

    candidates_path = data_dir / "source-discovery-candidates.json"
    active_path = data_dir / "source-registry-active.json"
    report_path = data_dir / "jobs-fetch-report.json"

    # 1) From discovery candidates: rows with sourceDirectory === "game_studios_sheet"
    candidates = _load_json(candidates_path, [])
    if not isinstance(candidates, list):
        candidates = []
    sheet_urls = set()
    for row in candidates:
        if not isinstance(row, dict):
            continue
        if str(row.get("sourceDirectory") or "").strip() != "game_studios_sheet":
            continue
        url = _norm_url(
            str(
                row.get("sourceDirectoryEntryUrl")
                or row.get("listing_url")
                or row.get("careersUrl")
                or ""
            )
        )
        if url:
            sheet_urls.add(url)

    # 2) From active registry: static rows with sourceDirectory === "game_studios_sheet" or listing_url in sheet_urls
    active = _load_json(active_path, [])
    if not isinstance(active, list):
        active = []
    registry_sheet_urls = set()
    for row in active:
        if not isinstance(row, dict):
            continue
        if str(row.get("adapter") or "").strip() != "static":
            continue
        if str(row.get("sourceDirectory") or "").strip() == "game_studios_sheet":
            url = _norm_url(str(row.get("listing_url") or row.get("sourceDirectoryEntryUrl") or ""))
            if url:
                registry_sheet_urls.add(url)
        else:
            url = _norm_url(str(row.get("listing_url") or ""))
            if url and url in sheet_urls:
                registry_sheet_urls.add(url)

    # 3) From fetch report: sources with sourceDirectory === "game_studios_sheet"
    report = _load_json(report_path, {})
    if not isinstance(report, dict):
        report = {}
    sources = report.get("sources")
    if not isinstance(sources, list):
        sources = []
    ran_urls = set()
    ok_urls = set()
    failed_urls = set()
    failed_list: list[str] = []
    for row in sources:
        if not isinstance(row, dict):
            continue
        if str(row.get("sourceDirectory") or "").strip() != "game_studios_sheet":
            continue
        url = _norm_url(str(row.get("listingUrl") or row.get("listing_url") or ""))
        if url:
            ran_urls.add(url)
        status = str(row.get("status") or "").strip().lower()
        if status == "ok":
            if url:
                ok_urls.add(url)
        else:
            if url:
                failed_urls.add(url)
                failed_list.append(url)

    in_sheet = len(sheet_urls)
    in_registry = len(registry_sheet_urls)
    ran = len(ran_urls)
    ok = len(ok_urls)
    failed = len(failed_urls)

    print("Game studios sheet funnel (sheet -> registry -> pipeline)")
    print("=" * 52)
    print(f"  From sheet (candidates):     {in_sheet}")
    print(f"  In active registry:          {in_registry}")
    print(f"  Ran in pipeline:             {ran}")
    print(f"  Pipeline OK:                {ok}")
    print(f"  Pipeline failed:            {failed}")
    if failed_list:
        print("\n  Failed URLs (sheet-derived):")
        for u in sorted(failed_list)[:30]:
            print(f"    {u}")
        if len(failed_list) > 30:
            print(f"    ... and {len(failed_list) - 30} more")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
