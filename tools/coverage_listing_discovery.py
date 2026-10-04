#!/usr/bin/env python3
"""Find the listing page behind a board whose host root reads as empty.

A static candidate's ``listing_url`` is the board's host root, which is right for a
single-page careers site and wrong for everything else. Measured on the 109 static boards
the second wave left undecided: the host root returns HTTP 200 and zero job rows for all
of them, while the careers page behind it is frequently plain server-rendered HTML.

This is the BambooHR defect again -- ``/careers`` is a shell, the listing is somewhere
else -- except here the tool never held a path to be wrong about, only a host.

The listing is derived from evidence rather than guessed: the board was found because
specific openings on it were missed, so the URLs of those openings are on hand. Each
ancestor path of a real opening URL is a candidate listing, and the one the runtime's own
detector reads the most rows from is the answer.

Nothing here decides whether a board should be registered. It only reports which URL the
runtime would read, so the verdict stays with ``coverage_verify``.

Usage:
  python tools/coverage_listing_discovery.py --boards wave2-boards.json   \\
      --gji gji-jobs.json --out listing-paths.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from coverage_verify import probe  # noqa: E402

from src.source_discovery.probe import static_probe_evidence  # noqa: E402

# Path segments that are the job itself rather than the board. Stopping the walk above
# these keeps a detail URL from being proposed as its own listing.
_JOB_SEGMENTS = frozenset(
    {"job", "jobs", "vacancy", "vacancies", "position", "positions", "detail", "details"}
)

# How many ancestor paths to try per board. Deep enough to reach a careers section under
# a locale or product prefix, shallow enough to stay a bounded sweep.
_MAX_ANCESTORS = 5


def _host(url: str) -> str:
    return urlparse(url).netloc.lower().removeprefix("www.")


def opening_urls_by_host(records: Sequence[Mapping[str, Any]]) -> dict[str, list[str]]:
    """Every opening URL in the catalogue, grouped by host."""
    by_host: dict[str, list[str]] = defaultdict(list)
    for row in records:
        url = str(row.get("source_url") or "").strip()
        if url.startswith("http"):
            by_host[_host(url)].append(url)
    return {host: sorted(set(urls)) for host, urls in by_host.items()}


def candidate_listings(host: str, urls: Sequence[str], tenant: str = "") -> list[str]:
    """Ancestor paths of real opening URLs, deepest first.

    The openings are the evidence that the board exists at all, so their own paths are the
    best available starting point. Each prefix is a plausible listing page; the caller
    measures them rather than trusting the shape.

    Deepest first, and tenant-scoped, because a multi-tenant host's prefixes are shared.
    ``herp.careers/v1/pgrecruit/e5id`` has the ancestors ``/v1`` and ``/v1/pgrecruit``, and
    the first of those belongs to every tenant on the host. Shallowest-first would hand all
    nine herp boards the same listing the moment ``/v1`` answered 200, which is the
    multi-tenant collapse this tool exists to avoid.
    """
    wanted = tenant.strip().lower()
    if wanted == host.lower():
        # A single-site board's "tenant" is its own host, which appears in no path segment.
        # Scoping by it would discard every candidate and find nothing.
        wanted = ""
    found: list[str] = []
    seen: set[str] = set()
    for url in urls:
        segments = [s for s in urlparse(url).path.split("/") if s]
        for depth in range(min(len(segments), _MAX_ANCESTORS), 0, -1):
            prefix = segments[:depth]
            if len(prefix) == len(segments):
                continue
            if prefix[-1].lower() in _JOB_SEGMENTS:
                continue
            if wanted and wanted not in [p.lower() for p in prefix]:
                continue
            listing = f"https://{host}/" + "/".join(prefix)
            if listing not in seen:
                seen.add(listing)
                found.append(listing)
    return found


def urls_for_board(
    board: Mapping[str, Any], urls_by_host: Mapping[str, Sequence[str]]
) -> list[str]:
    """The board's own opening URLs, not every URL on its host.

    Grouping by host alone is wrong on a multi-tenant platform. All nine herp.careers
    boards share one host, so each board's candidate list contained every other tenant's
    openings and the first ancestor to answer 200 won -- which pointed all nine at the same
    listing. A board's listing has to come from that board's openings, so a candidate with a
    tenant keeps only the URLs whose path contains it.
    """
    host = _host(str(board.get("listing_url") or ""))
    urls = list(urls_by_host.get(host) or ())
    tenant = str(board.get("tenant") or "").strip().lower()
    if not tenant or tenant == host:
        return urls
    mine = [u for u in urls if f"/{tenant}/" in u.lower() or u.lower().endswith(f"/{tenant}")]
    return mine or urls


def discover(
    board: Mapping[str, Any],
    urls_by_host: Mapping[str, Sequence[str]],
    *,
    timeout: int,
    max_probes: int = 14,
) -> dict[str, Any]:
    """The best listing URL for one board, measured with the runtime's detector.

    Two different wins are reported, because they are different findings. ``bestRows`` is a
    board a plain GET can read, which no other tool needs help with. ``reachableUrl`` is a
    board whose registered URL was simply wrong -- the host root answers 404 or 400 while the
    real listing answers 200 -- and that is worth registering even when the 200 page carries
    no rows, because the registered URL is still the correct one and the openings recorded
    for it stand in for the zero.

    Conflating them loses the second case entirely: a JS shell that answers 200 scores zero
    rows, so "found more rows" discards it and leaves the board registered against a URL that
    errors.
    """
    root = str(board.get("listing_url") or "")
    host = _host(root)
    urls = urls_for_board(board, urls_by_host)
    result: dict[str, Any] = {
        "id": board.get("id"),
        "adapter": board.get("adapter"),
        "studio": board.get("studio"),
        "rootUrl": root,
        "rootRows": 0,
        "host": host,
        "openingUrlCount": len(urls),
        "bestUrl": "",
        "bestRows": 0,
        "reachableUrl": "",
        "reachableHttp": 0,
        "probed": [],
    }
    status, body = probe.http_get(root, timeout=timeout)
    result["rootHttp"] = status
    if status == 200:
        result["rootRows"] = static_probe_evidence(body, root).count
        result["reachableUrl"] = root
        result["reachableHttp"] = 200

    for listing in candidate_listings(host, urls, str(board.get("tenant") or ""))[:max_probes]:
        status, body = probe.http_get(listing, timeout=timeout)
        rows = static_probe_evidence(body, listing).count if status == 200 else 0
        result["probed"].append({"url": listing, "http": status, "rows": rows})
        if rows > result["bestRows"]:
            result["bestUrl"] = listing
            result["bestRows"] = rows
        if status == 200 and not result["reachableUrl"]:
            result["reachableUrl"] = listing
            result["reachableHttp"] = 200
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--boards", required=True, help="candidates json from coverage_boards")
    parser.add_argument("--gji", required=True, help="catalogue json with a records array")
    parser.add_argument("--out", required=True)
    parser.add_argument("--adapter", default="static")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--show", type=int, default=25)
    args = parser.parse_args()

    payload = json.loads(Path(args.boards).read_text(encoding="utf-8"))
    candidates = payload if isinstance(payload, list) else (payload.get("candidates") or [])
    wanted = [c for c in candidates if str(c.get("adapter") or "") == args.adapter]

    catalogue = json.loads(Path(args.gji).read_text(encoding="utf-8"))
    records = catalogue if isinstance(catalogue, list) else (catalogue.get("records") or [])
    urls_by_host = opening_urls_by_host(records)

    results = [discover(c, urls_by_host, timeout=args.timeout) for c in wanted]
    readable = [r for r in results if r["bestRows"] > r["rootRows"]]
    repaired = [r for r in results if r["reachableUrl"] and r["reachableUrl"] != r["rootUrl"]]
    gained = sum(r["bestRows"] - r["rootRows"] for r in readable)
    seen_listings: dict[str, list[str]] = {}
    for row in repaired:
        seen_listings.setdefault(row["reachableUrl"], []).append(str(row.get("studio") or ""))
    collisions = {url: names for url, names in seen_listings.items() if len(names) > 1}

    Path(args.out).write_text(
        json.dumps({"results": results}, indent=1) + "\n", encoding="utf-8", newline="\n"
    )

    print(f"boards examined            : {len(results)}")
    print(f"host root readable         : {sum(1 for r in results if r['rootRows'])}")
    print(f"a derived listing reads more: {len(readable)}  ({gained} rows)")
    print(f"host root is the wrong URL : {len(repaired)}")
    print(f"two boards on one listing  : {len(collisions)}  <- must be 0")
    print()
    print(f"{'root':>5} {'best':>6} {'rootHTTP':<8} studio -> repaired listing")
    for row in sorted(repaired, key=lambda r: (-r["bestRows"], str(r.get("studio"))))[: args.show]:
        print(
            f"{row['rootRows']:>5} {row['bestRows']:>6} {str(row.get('rootHttp')):<8} "
            f"{str(row.get('studio'))[:22]:<24} {row['reachableUrl']}"
        )
    for url, names in collisions.items():
        print(f"  COLLISION {url} <- {', '.join(names)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
