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


def candidate_listings(host: str, urls: Sequence[str]) -> list[str]:
    """Ancestor paths of real opening URLs, shallowest first.

    The openings are the evidence that the board exists at all, so their own paths are the
    best available starting point. Each prefix is a plausible listing page; the caller
    measures them rather than trusting the shape.
    """
    found: list[str] = []
    seen: set[str] = set()
    for url in urls:
        segments = [s for s in urlparse(url).path.split("/") if s]
        for depth in range(1, min(len(segments), _MAX_ANCESTORS) + 1):
            prefix = segments[:depth]
            if len(prefix) == len(segments):
                continue
            if prefix[-1].lower() in _JOB_SEGMENTS:
                continue
            listing = f"https://{host}/" + "/".join(prefix)
            if listing not in seen:
                seen.add(listing)
                found.append(listing)
    return found


def discover(
    board: Mapping[str, Any],
    urls_by_host: Mapping[str, Sequence[str]],
    *,
    timeout: int,
    max_probes: int = 14,
) -> dict[str, Any]:
    """The best listing URL for one board, measured with the runtime's detector."""
    root = str(board.get("listing_url") or "")
    host = _host(root)
    urls = urls_by_host.get(host) or ()
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
        "probed": [],
    }
    status, body = probe.http_get(root, timeout=timeout)
    result["rootHttp"] = status
    if status == 200:
        result["rootRows"] = static_probe_evidence(body, root).count

    for listing in candidate_listings(host, urls)[:max_probes]:
        status, body = probe.http_get(listing, timeout=timeout)
        rows = static_probe_evidence(body, listing).count if status == 200 else 0
        result["probed"].append({"url": listing, "http": status, "rows": rows})
        if rows > result["bestRows"]:
            result["bestUrl"] = listing
            result["bestRows"] = rows
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
    improved = [r for r in results if r["bestRows"] > r["rootRows"]]
    gained = sum(r["bestRows"] - r["rootRows"] for r in improved)

    Path(args.out).write_text(
        json.dumps({"results": results}, indent=1) + "\n", encoding="utf-8", newline="\n"
    )

    print(f"boards examined          : {len(results)}")
    print(f"host root readable       : {sum(1 for r in results if r['rootRows'])}")
    print(f"a derived listing is better: {len(improved)}")
    print(f"rows gained over the root : {gained}")
    print()
    print(f"{'root':>5} {'best':>6} {'HTTP':<5} listing")
    for row in sorted(improved, key=lambda r: -r["bestRows"])[: args.show]:
        print(
            f"{row['rootRows']:>5} {row['bestRows']:>6} {str(row.get('rootHttp')):<5} {row['bestUrl']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
