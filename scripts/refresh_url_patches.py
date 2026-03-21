#!/usr/bin/env python3
"""Refresh URL patches from discovery report.

Scans a source-discovery report for redirect failures, auto-resolves ATS
redirects, and generates an updated url-patch-manifest.json.

Usage:
    python scripts/refresh-url-patches.py
    python scripts/refresh-url-patches.py --dry-run
    python scripts/refresh-url-patches.py --report path/to/report.json
    python scripts/refresh-url-patches.py --verbose
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

DEFAULT_REPORT_PATH = Path("data/source-discovery-report.json")
DEFAULT_OUTPUT_PATH = Path("data/url-patch-manifest.json")


def load_report(report_path: Path) -> Dict[str, Any]:
    """Load discovery report from JSON file."""
    with report_path.open(encoding="utf-8") as f:
        return json.load(f)


def load_existing_manifest(manifest_path: Path) -> Dict[str, str]:
    """Load existing patches from manifest."""
    if not manifest_path.exists():
        return {}
    with manifest_path.open(encoding="utf-8") as f:
        data = json.load(f)
        return data.get("patches", {})


def extract_redirect_failures(report: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract redirect failures from discovery report."""
    failures = report.get("failures", [])
    redirects = []

    for f in failures:
        error = f.get("error", "")
        if any(x in error for x in ["301", "302", "308", "redirect", "Redirect"]):
            name = f.get("name", "unknown")
            adapter = f.get("adapter", "unknown")

            # Extract URL from error message
            url = ""
            if "for url" in error.lower():
                url_start = error.lower().find("for url") + 8
                url_part = error[url_start:].strip()
                url = url_part.split()[0].strip("'\"") if url_part else ""

            if url:
                redirects.append({
                    "name": name,
                    "url": url,
                    "adapter": adapter,
                    "original_error": error,
                })

    return redirects


async def resolve_url(url: str, timeout: float = 10.0) -> Tuple[str, int, List[str]]:
    """Follow redirects and return final URL, status, and redirect chain."""
    try:
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=timeout, max_redirects=10
        ) as client:
            response = await client.get(url)
            redirect_chain = [str(r.url) for r in response.history]
            redirect_chain.append(str(response.url))
            return str(response.url), response.status_code, redirect_chain
    except Exception as e:
        return "", 0, []


def is_valid_workable_board(url: str) -> bool:
    """Check if URL is a valid Workable board (not an error page)."""
    if "apply.workable.com" not in url:
        return True
    # Workable error pages redirect to /oops
    if "/oops" in url.lower() or "/error" in url.lower():
        return False
    return True


# Known Greenhouse URL mappings (studios that moved off Greenhouse or changed board)
GREENHOUSE_KNOWN_FIXES: Dict[str, str] = {
    "niantic": "https://careers.nianticlabs.com/",
    "larian": "https://larian.com/careers",
    "supercell": "https://supercell.com/en/careors/",
    "ioi": "https://apply.ioi.dk/jobs",
    "io-interactive": "https://apply.ioi.dk/jobs",
    "guerrilla": "https://job-boards.greenhouse.io/guerrilla-games",
    "guerrilla-games": "https://job-boards.greenhouse.io/guerrilla-games",
    "remedy": "https://www.remedygames.com/careers/",
    "remedy-entertainment": "https://www.remedygames.com/careers/",
    "bandainamco": "https://job-boards.greenhouse.io/bandainamco",
    "playstation": "https://careers.playstation.com/",
    "playstation global": "https://careers.playstation.com/",
}


def resolve_greenhouse_known(studio_name: str) -> Optional[str]:
    """Try to resolve Greenhouse board using known mappings."""
    name_lower = studio_name.lower()

    # Try direct match
    for key, url in GREENHOUSE_KNOWN_FIXES.items():
        if key in name_lower:
            return url

    return None


async def verify_url(url: str, timeout: float = 8.0) -> bool:
    """Verify if a URL returns 200 OK."""
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            response = await client.head(url)
            return 200 <= response.status_code < 400
    except Exception:
        return False


def resolve_with_greenhouse_fallback(
    failed_greenhouse: List[Dict[str, str]], verbose: bool = False
) -> Dict[str, Dict[str, Any]]:
    """Try to resolve Greenhouse boards using known mappings."""
    results = {}

    for r in failed_greenhouse:
        studio_name = r["name"].replace(" (Greenhouse)", "").strip()
        original_url = r["url"]

        suggested_url = resolve_greenhouse_known(studio_name)
        if suggested_url:
            results[original_url] = {
                "name": r["name"],
                "adapter": "greenhouse",
                "original_url": original_url,
                "suggested_url": suggested_url,
                "source": "known_fixes",
                "confidence": 85,
            }
            if verbose:
                print(f"  Known fix: {studio_name} -> {suggested_url}")

    return results


async def auto_resolve_redirects(
    redirects: List[Dict[str, str]], verbose: bool = False
) -> Dict[str, Dict[str, Any]]:
    """Auto-resolve redirect URLs."""
    results = {}

    # Filter out LinkedIn URLs (blocked)
    linkedin = []
    resolvable = []

    for r in redirects:
        if "linkedin.com" in r["url"].lower():
            linkedin.append(r)
        else:
            resolvable.append(r)

    if verbose:
        print(f"LinkedIn (skipped): {len(linkedin)}")

    # Resolve non-LinkedIn URLs
    for r in resolvable:
        final_url, status, chain = await resolve_url(r["url"])

        if status and status < 400:
            if final_url != r["url"]:
                results[r["url"]] = {
                    "name": r["name"],
                    "adapter": r["adapter"],
                    "original_url": r["url"],
                    "suggested_url": final_url,
                    "status": status,
                    "redirect_chain": chain,
                    "source": "auto_resolve",
                    "confidence": 90 if len(chain) > 1 else 80,
                }
                if verbose:
                    print(f"  Resolved: {r['name'][:40]}")
                    print(f"    {r['url'][:50]} -> {final_url[:50]}")
            else:
                if verbose:
                    print(f"  No redirect: {r['name'][:40]}")
        else:
            if verbose:
                print(f"  Failed: {r['name'][:40]} (status={status})")

    return results


def generate_manifest(
    existing_patches: Dict[str, str],
    new_patches: Dict[str, Dict[str, Any]],
    linkedin_count: int,
) -> Dict[str, Any]:
    """Generate updated manifest."""
    all_patches = dict(existing_patches)

    # Add new patches (only if different from existing)
    added = 0
    updated = 0
    for original, data in new_patches.items():
        suggested = data["suggested_url"]
        if original not in all_patches:
            all_patches[original] = suggested
            added += 1
        elif all_patches[original] != suggested:
            all_patches[original] = suggested
            updated += 1

    return {
        "_version": "1.1",
        "_updated": datetime.now().strftime("%Y-%m-%d"),
        "_description": "URL patches for studios with redirect issues. Original URL -> Corrected URL.",
        "_stats": {
            "total_patches": len(all_patches),
            "added": added,
            "updated": updated,
            "linkedin_blocked": linkedin_count,
        },
        "patches": all_patches,
    }


def save_manifest(manifest: Dict[str, Any], output_path: Path) -> None:
    """Save manifest to JSON file."""
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"Manifest saved to: {output_path}")


def print_summary(
    existing_count: int,
    linkedin_count: int,
    resolved_count: int,
    web_search_count: int,
    manifest: Dict[str, Any],
) -> None:
    """Print summary of changes."""
    new_total = len(manifest["patches"])
    stats = manifest.get("_stats", {})

    print("\n" + "=" * 60)
    print("URL PATCH REFRESH SUMMARY")
    print("=" * 60)
    print(f"Existing patches:    {existing_count}")
    print(f"New redirects found:  {linkedin_count + resolved_count + web_search_count}")
    print(f"  - LinkedIn blocked:{linkedin_count}")
    print(f"  - Auto-resolved:   {resolved_count}")
    print(f"  - Web search:      {web_search_count}")
    print(f"New total patches:   {new_total}")
    print(f"Added this run:      {stats.get('added', 0)}")
    print(f"Updated this run:    {stats.get('updated', 0)}")
    print("=" * 60)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Refresh URL patches from discovery report."
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help="Path to source-discovery-report.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output path for url-patch-manifest.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without saving",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed progress",
    )
    args = parser.parse_args(argv)

    # Load existing manifest
    existing_patches = load_existing_manifest(args.output)
    print(f"Loaded {len(existing_patches)} existing patches from {args.output}")

    # Load report
    if not args.report.exists():
        print(f"ERROR: Report not found: {args.report}")
        print("Run a discovery first: python -m src.source_discovery.orchestrator --uncapped")
        return 1

    print(f"Loading report: {args.report}")
    report = load_report(args.report)

    # Extract redirects
    redirects = extract_redirect_failures(report)
    print(f"Found {len(redirects)} redirect failures in report")

    if not redirects:
        print("No redirects found. Manifest is up to date.")
        return 0

    # Auto-resolve
    print("Auto-resolving redirects...")
    resolved = asyncio.run(auto_resolve_redirects(redirects, verbose=args.verbose))

    # Filter out invalid Workable boards
    invalid_workable = []
    for orig, data in list(resolved.items()):
        if not is_valid_workable_board(data["suggested_url"]):
            invalid_workable.append(orig)
            del resolved[orig]

    if invalid_workable and args.verbose:
        print(f"\nFiltered invalid Workable boards: {len(invalid_workable)}")

    # Try Greenhouse known fixes for remaining failures
    failed_greenhouse = [
        r for r in redirects
        if "boards.greenhouse.io" in r["url"] and r["url"] not in resolved
    ]

    greenhouse_fixes = {}
    if failed_greenhouse:
        print(f"\nTrying Greenhouse known fixes for {len(failed_greenhouse)} failed boards...")
        greenhouse_fixes = resolve_with_greenhouse_fallback(failed_greenhouse, verbose=args.verbose)

    # Combine all resolved patches
    all_resolved = {**resolved, **greenhouse_fixes}

    # Count LinkedIn
    linkedin_count = sum(
        1 for r in redirects if "linkedin.com" in r["url"].lower()
    )

    # Generate manifest
    manifest = generate_manifest(existing_patches, all_resolved, linkedin_count)

    # Print summary
    print_summary(
        len(existing_patches), linkedin_count, len(resolved), len(greenhouse_fixes), manifest
    )

    # Save or dry-run
    if args.dry_run:
        print("\nDry-run: Not saving changes.")
        print("\nProposed patches:")
        for orig, sugg in list(manifest["patches"].items())[:10]:
            print(f"  {orig[:50]} -> {sugg[:50]}")
        if len(manifest["patches"]) > 10:
            print(f"  ... and {len(manifest['patches']) - 10} more")
    else:
        save_manifest(manifest, args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
