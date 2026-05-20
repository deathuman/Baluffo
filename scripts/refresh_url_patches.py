#!/usr/bin/env python3
"""Refresh URL patches from a discovery report using shared discovery helpers."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from src.source_discovery.url_patches import (
    extract_redirect_failures,
    load_url_patch_manifest,
    merge_url_patches,
    resolve_greenhouse_known,
    resolve_url,
    save_url_patch_manifest,
)
from src.source_registry import URL_PATCH_MANIFEST_PATH, load_json_object, normalize_source_url
from src.url_hosts import url_host_matches_domain

DEFAULT_REPORT_PATH = Path("data/source-discovery-report.json")


async def _auto_resolve(redirects):
    results = {}
    for row in redirects:
        original = normalize_source_url(str(row.get("url") or ""))
        if not original or url_host_matches_domain(original, "linkedin.com"):
            continue
        final_url, status, _chain = await resolve_url(original)
        normalized_final = normalize_source_url(final_url)
        if normalized_final and 200 <= int(status or 0) < 400 and normalized_final != original:
            results[original] = normalized_final
            continue
        greenhouse_known = resolve_greenhouse_known(str(row.get("name") or ""))
        if greenhouse_known and greenhouse_known != original:
            results[original] = greenhouse_known
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Refresh URL patches from source-discovery-report.json."
    )
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--output", type=Path, default=URL_PATCH_MANIFEST_PATH)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    report = load_json_object(args.report, {})
    redirects = extract_redirect_failures(report)
    existing_manifest = load_url_patch_manifest(args.output)
    existing_patches = dict(existing_manifest.get("patches") or {})
    resolved_patches = asyncio.run(_auto_resolve(redirects))
    merged_patches, added, updated = merge_url_patches(existing_patches, resolved_patches)

    if args.dry_run:
        print(f"Redirect failures found: {len(redirects)}")
        print(f"Patches loaded: {len(existing_patches)}")
        print(f"Patches added: {added}")
        print(f"Patches updated: {updated}")
        return 0

    save_url_patch_manifest(
        merged_patches,
        path=args.output,
        added=added,
        updated=updated,
        reprobed=0,
    )
    print(f"Saved {len(merged_patches)} URL patches to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
