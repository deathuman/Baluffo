#!/usr/bin/env python3
"""Regenerate src/curated_coverage_boards.json from the verified candidate set.

Kept as a script rather than a one-off so the data file is reproducible: the rows
carry an `api_url` whose presence is load-bearing, and a hand-edit that dropped one
would silently return that board to the pending-forever failure mode documented in
docs/plans/catalogue-coverage-gap-plan.md.

Run from the repo root:

  python tools/coverage_refresh_curated.py \\
      --boards _out/coverage/boards.json \\
      --verified _out/coverage/verified-merged.json \\
      --registry <live source-registry-active.json> \\
      --out src/curated_coverage_boards.json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
_TOOLS = str(Path(__file__).resolve().parent)
for path in (str(ROOT), _TOOLS):
    if path not in sys.path:
        sys.path.insert(0, path)


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, Path(_TOOLS) / f"{name}.py")
    if not spec or not spec.loader:  # pragma: no cover - import guard
        raise SystemExit(f"cannot load {name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


identity = _load("coverage_board_identity")
register = _load("coverage_register")


def registry_row_id(candidate: dict[str, Any]) -> str:
    return str(candidate.get("id") or "")


def hand_curated_identities() -> set[tuple[str, str, str]]:
    """Board identities already registered in the literal curated table.

    The audit dedupes against the live registry, which does not carry unreleased
    rows, so boards registered in earlier unreleased work come back as candidates and
    merging them would fetch those boards twice.

    Compared on **identity**, not the id string: an id like
    ``ashby:board_url:https://jobs.ashbyhq.com/voodoo`` and a hand-written
    ``ashby:board_url:voodoo`` are the same board and never compare equal as strings.
    That is the same class of bug that once made every single-site static board look
    unregistered.

    Rows sourced from the curated data file are excluded, or the file would dedupe
    against itself and empty the catalogue.
    """
    from src.source_discovery.config import (
        STATIC_DISCOVERY_CANDIDATES,
        load_curated_coverage_boards,
    )

    locators = ("slug", "account", "company_id", "board_url", "api_url", "listing_url")
    # Both sides must go through the same function. The curated rows carry locator
    # fields but no host/tenant, so candidate_identity() on them yields
    # (adapter, "", "") and never matches a real identity -- which would make every
    # data-file row look hand-curated and empty the catalogue.
    from_file = {
        found
        for found in (_identity_of_row(row, locators) for row in load_curated_coverage_boards())
        if found is not None
    }
    out: set[tuple[str, str, str]] = set()
    for row in STATIC_DISCOVERY_CANDIDATES:
        rebuilt = _identity_of_row(row, locators)
        if rebuilt is not None and rebuilt not in from_file:
            out.add(rebuilt)
    return out


def _identity_of_row(row: dict[str, Any], locators: Sequence[str]) -> tuple[str, str, str] | None:
    """Board identity of a registry/discovery row, from whichever locator it carries."""
    adapter = str(row.get("adapter") or "")
    for field in locators:
        value = str(row.get(field) or "")
        if not value:
            continue
        if field in {"slug", "account", "company_id"}:
            return (adapter, identity.ADAPTER_CANONICAL_HOST.get(adapter, ""), value.lower())
        host = identity.host_of(value)
        tenant = identity.resolve_tenant(host, identity.segments_of(value))
        return identity.candidate_identity(
            identity.describe_board(adapter=adapter, host=host, tenant=tenant)
        )
    return None


def build_rows(
    candidates: Sequence[dict[str, Any]], existing: set[tuple[str, str, str]]
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in sorted(
        candidates, key=lambda c: (-int(c.get("missingCount") or 0), str(c.get("id")))
    ):
        if identity.candidate_identity(candidate) in existing:
            continue
        rebuilt = identity.describe_board(
            adapter=str(candidate.get("adapter") or ""),
            host=str(candidate.get("host") or ""),
            tenant=str(candidate.get("tenant") or ""),
            company=str((candidate.get("companies") or [""])[0]),
        )
        row = register.build_rows([{**rebuilt, "_id": rebuilt["id"]}])[0]
        row.pop("_id", None)
        row["coverageAuditOpenings"] = int(candidate.get("missingCount") or 0)
        rows.append(row)
    return rows


def write_rows(rows: Sequence[dict[str, Any]], path: Path) -> None:
    """One row per line, so the diff is reviewable data rather than 4,000 lines."""
    lines = ["["]
    for index, row in enumerate(rows):
        tail = "," if index < len(rows) - 1 else ""
        lines.append("  " + json.dumps(row, ensure_ascii=False, separators=(", ", ": ")) + tail)
    lines.append("]")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--boards", required=True, type=Path)
    parser.add_argument("--verified", required=True, type=Path)
    parser.add_argument("--registry", type=Path, help="live registry json to dedupe against")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--expect-rows", type=int, help="assert the row count before writing")
    args = parser.parse_args(argv)

    boards = json.loads(args.boards.read_text(encoding="utf-8"))["candidates"]
    results = json.loads(args.verified.read_text(encoding="utf-8"))["results"]
    live_ids = register.load_registry_ids(args.registry) if args.registry else set()
    candidates = [c for c in boards if c.get("status") == "new_candidate"]
    buckets = register.select(candidates, results, live_ids)

    rows = build_rows(buckets["register"], hand_curated_identities())
    if args.expect_rows is not None and len(rows) != args.expect_rows:
        raise SystemExit(f"expected {args.expect_rows} rows, built {len(rows)}; refusing to write")

    with_api = sum(1 for row in rows if row.get("api_url"))
    print(f"rows: {len(rows)}  (with api_url: {with_api})")
    print(f"openings: {sum(int(r['coverageAuditOpenings']) for r in rows)}")
    print(f"skipped as already hand-curated: {len(buckets['register']) - len(rows)}")
    write_rows(rows, args.out)
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
