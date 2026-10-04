#!/usr/bin/env python3
"""Classify every curated static board by what a plain GET can actually see.

The plan records "246 of 276 static boards are unreadable to a plain GET" from two
samples. This makes it exhaustive, because "unreadable" has three quite different
causes that need different fixes and it matters which is which:

* **js_shell** -- HTTP 200, a handful of anchors, no job links. The listing renders in
  a browser. Fixable by routing the probe through the rendered path.
* **blocked** -- 403/429/challenge. Also fixable by the rendered path, and often
  already handled by the Playwright fallback.
* **dead** -- 404/410, or a real page with genuinely no openings. No probe change will
  help, and registering these as if they were collectable would be wrong.
* **readable** -- job links present in the raw HTML. These should already be landing,
  so a board in this class that does not land is a different bug.

Writes a JSON report rather than printing, so the classification can be pinned by test.
Run from the repo root:

  python tools/coverage_static_sweep.py --out _out/coverage/static-sweep.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
_TOOLS = str(Path(__file__).resolve().parent)
for _p in (str(ROOT), _TOOLS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from coverage_verify import probe  # noqa: E402

from src.source_discovery.probe import static_probe_evidence  # noqa: E402

# Below this many anchors a 200 response is a shell rather than a listing with jobs.
JS_SHELL_ANCHOR_CEILING = 12

READABLE = "readable"
JS_SHELL = "js_shell"
BLOCKED = "blocked"
DEAD = "dead"

STATUS_BLOCKED = {401, 403, 405, 406, 409, 418, 429, 503}
STATUS_DEAD = {404, 410}


def classify(status: int, body: str, url: str = "") -> tuple[str, int, int | None]:
    """``(class, anchor_count, rows)`` for one fetched board.

    The row count comes from the runtime's own ``static_probe_evidence``, not from a
    local anchor scan. A first version of this sweep used a local
    ``is_probable_job_detail_url`` counter and reported 112 of 276 boards as readable;
    cross-checking against the runtime detector showed 8 of 10 sampled "readable"
    boards were actually zero, because one or two nav-shaped links satisfy a loose
    detail-link rule. Two counters for one vendor is exactly the divergence this
    effort has already been bitten by, so the authority here is the detector the
    pipeline itself runs.
    """
    if status in STATUS_DEAD:
        return DEAD, 0, None
    if status in STATUS_BLOCKED or status == 0:
        return BLOCKED, 0, None
    if status != 200:
        return DEAD, 0, None
    anchors = len(re.findall(r"""<a\b[^>]*?href=["']([^"']+)["']""", body, re.I))
    evidence = static_probe_evidence(body, url or "https://invalid.example/")
    rows = int(evidence.count or 0)
    if rows:
        return READABLE, anchors, rows
    return JS_SHELL, anchors, 0


def sweep(rows: Sequence[dict[str, Any]], *, timeout: float = 30.0) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        url = str(row.get("listing_url") or row.get("careersUrl") or "")
        status, body = probe.http_get(url, timeout=timeout)
        kind, anchors, counted = classify(status, body, url)
        out.append(
            {
                "id": row.get("id") or f"static:listing_url:{url}",
                "studio": row.get("studio"),
                "host": row.get("host") or "",
                "url": url,
                "status": status,
                "kind": kind,
                "anchors": anchors,
                "rows": counted,
                "openings": int(row.get("coverageAuditOpenings") or 0),
            }
        )
    return out


def summarise(results: Sequence[dict[str, Any]]) -> dict[str, Any]:
    by_kind: dict[str, dict[str, int]] = {}
    for row in results:
        entry = by_kind.setdefault(str(row["kind"]), {"boards": 0, "openings": 0, "rows": 0})
        entry["boards"] += 1
        entry["openings"] += int(row["openings"] or 0)
        entry["rows"] += int(row["rows"] or 0)
    return {
        "boards": len(results),
        "byKind": dict(sorted(by_kind.items())),
        "openingsByKind": {kind: stats["openings"] for kind, stats in sorted(by_kind.items())},
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--boards", type=Path, default=Path("src/curated_coverage_boards.json"))
    parser.add_argument("--out", type=Path, default=Path("_out/coverage/static-sweep.json"))
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args(argv)

    payload = json.loads(args.boards.read_text(encoding="utf-8"))
    rows = [r for r in payload if str(r.get("adapter")) == "static"]
    rows.sort(key=lambda r: -int(r.get("coverageAuditOpenings") or 0))
    if args.limit:
        rows = rows[: args.limit]

    results = sweep(rows, timeout=args.timeout)
    summary = summarise(results)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps({"summary": summary, "results": results}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"static boards swept: {summary['boards']}")
    for kind, stats in summary["byKind"].items():
        print(f"  {kind:<10} boards={stats['boards']:>4}  promised openings={stats['openings']:>5}")
    print(f"\nwritten to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
