#!/usr/bin/env python3
"""Turn coverage-audit misses into registry-ready board candidates.

The catalogue sweep says 4,930 of 10,465 missing openings are unreachable only
because their board was never registered. That is mechanical, and this turns it into
a repeatable pipeline instead of a hand-registered list.

One missed opening justifies one row: there is no volume threshold, because the
point of the product is that no opening is left behind. What a row must not do is
duplicate an existing one, and identity here is the **registry id**, which encodes
host plus tenant. Never the studio label — the registry carries one board as both
"Lost Boys Interactive" and "Lost Boys Interactive (Embracer Group)", and a
label-keyed join calls a covered board missing.

Candidates are emitted, not applied. Writing to the registry is a separate,
deliberate step so the plan can be printed and asserted first.

Usage:
  python tools/coverage_boards.py --report _out/coverage/report.json \\
      --registry data/source-registry-active.json --out _out/coverage/boards.json
  python tools/coverage_boards.py ... --vendor workday --show
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

# tools/ is flat and has no package init, so a sibling import needs the directory on
# the path. Guarded so repeated loads (tests import these by path) do not grow it.
_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from coverage_board_identity import (  # noqa: E402, I001
    STATUS_ALREADY,
    STATUS_NEW,
    STATUS_NO_TENANT,
    STATUS_UNSUPPORTED,
    board_key,
    build_candidate,
    candidate_identity,
    registry_identities,
)


def collect_candidates(
    misses: Sequence[Mapping[str, Any]],
    registry_ids: set[str] | set[tuple[str, str, str]],
) -> list[dict[str, Any]]:
    """Group misses by board identity, counting the openings each board would add.

    ``registry_ids`` may be raw registry id strings or pre-parsed
    ``(adapter, host, tenant)`` identities; both are compared on identity, because
    the registry's id strings are inconsistent about trailing slashes and a string
    comparison calls a served board missing.
    """
    known = registry_identities(registry_ids)
    grouped: dict[str, dict[str, Any]] = {}
    for miss in misses:
        source_url = str(miss.get("sourceUrl") or "")
        if not source_url:
            continue
        candidate = build_candidate(source_url, company=str(miss.get("company") or ""))
        key = candidate.get("id") or board_key(source_url)
        if candidate.get("status") == STATUS_UNSUPPORTED:
            # Group by tenant even with no adapter, so the backlog reads as
            # "826 openings on 40 feishu boards" rather than one opaque row. Board
            # count is meaningless for a vendor that cannot be read; openings is
            # the thing worth sizing.
            key = f"unsupported:{candidate.get('adapter')}:{candidate.get('tenant') or key}"
        entry = grouped.setdefault(
            key,
            {**candidate, "id": key, "missingCount": 0, "sampleTitles": [], "companies": set()},
        )
        entry["missingCount"] += 1
        title = str(miss.get("title") or "")
        if title and len(entry["sampleTitles"]) < 3:
            entry["sampleTitles"].append(title)
        company = str(miss.get("company") or "")
        if company:
            entry["companies"].add(company)

    out: list[dict[str, Any]] = []
    for entry in grouped.values():
        entry["companies"] = sorted(entry["companies"])[:3]
        if candidate_identity(entry) in known:
            entry["status"] = STATUS_ALREADY
        out.append(entry)
    return sorted(out, key=lambda row: (-int(row["missingCount"]), str(row.get("id"))))


def summarise(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_status: dict[str, int] = {}
    by_adapter: dict[str, int] = {}
    openings = 0
    backlog: dict[str, dict[str, int]] = {}
    for row in candidates:
        status = str(row.get("status"))
        adapter = str(row.get("adapter"))
        by_status[status] = by_status.get(status, 0) + 1
        if status == STATUS_NEW:
            openings += int(row.get("missingCount") or 0)
            by_adapter[adapter] = by_adapter.get(adapter, 0) + 1
        elif status == STATUS_UNSUPPORTED:
            entry = backlog.setdefault(adapter, {"boards": 0, "openings": 0})
            entry["boards"] += 1
            entry["openings"] += int(row.get("missingCount") or 0)
    return {
        "boards": len(candidates),
        "byStatus": by_status,
        "openingsRecoverable": openings,
        "newBoardsByAdapter": dict(sorted(by_adapter.items(), key=lambda kv: -kv[1])),
        "unsupportedVendorBacklog": dict(
            sorted(backlog.items(), key=lambda kv: -kv[1]["openings"])
        ),
    }


def render(
    candidates: Sequence[Mapping[str, Any]],
    *,
    limit: int = 40,
    summary: Mapping[str, Any] | None = None,
) -> str:
    lines = [
        f"boards seen: {len(candidates)}",
        f"  new candidates      : {sum(1 for c in candidates if c.get('status') == STATUS_NEW)}",
        f"  already registered  : {sum(1 for c in candidates if c.get('status') == STATUS_ALREADY)}",
        f"  unsupported vendor  : {sum(1 for c in candidates if c.get('status') == STATUS_UNSUPPORTED)}",
        f"  no tenant resolved  : {sum(1 for c in candidates if c.get('status') == STATUS_NO_TENANT)}",
        "",
        f"{'openings':>8}  {'adapter':<16} {'id':<62} company",
        "-" * 120,
    ]
    for row in candidates[:limit]:
        if row.get("status") != STATUS_NEW:
            continue
        company = ", ".join(row.get("companies") or [])[:40]
        lines.append(
            f"{row.get('missingCount', 0):>8}  {str(row.get('adapter')):<16} "
            f"{str(row.get('id'))[:60]:<62} {company}"
        )
    backlog = summary.get("unsupportedVendorBacklog") or {}
    if backlog:
        lines += ["", "unreadable vendors (adapter support backlog):"]
        for vendor, stats in backlog.items():
            lines.append(
                f"{stats['openings']:>8}  {vendor:<16} across {stats['boards']} boards -- needs an adapter"
            )
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", required=True, type=Path, help="coverage_audit report.json")
    parser.add_argument("--registry", type=Path, help="source-registry-active.json")
    parser.add_argument("--out", type=Path, help="where to write boards.json")
    parser.add_argument("--vendor", help="only show this adapter")
    parser.add_argument("--show", type=int, default=40, help="rows to print")
    args = parser.parse_args(argv)

    report = json.loads(args.report.read_text(encoding="utf-8"))
    registry_ids: set[str] = set()
    if args.registry and args.registry.exists():
        raw = json.loads(args.registry.read_text(encoding="utf-8"))
        rows = raw if isinstance(raw, list) else raw.get("rows") or []
        registry_ids = {
            str(row.get("id") or "").lower()
            for row in rows
            if isinstance(row, Mapping) and row.get("id")
        }

    candidates = collect_candidates(report.get("misses") or [], registry_ids)
    if args.vendor:
        candidates = [c for c in candidates if str(c.get("adapter")) == args.vendor]

    summary = summarise(candidates)
    payload = {"summary": summary, "candidates": candidates}
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(render(candidates, limit=args.show, summary=summary))
    print()
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
