#!/usr/bin/env python3
"""Turn verified board candidates into registry rows.

Three things have to be true before a row is written, and each has a measured reason
behind it.

**Only boards that would collect.** `tools/coverage_verify.py` decides that, and its
verdicts are three-way on purpose. `collects` is a positive answer. `unknown` is not
refusal — for a scraped board it usually means the listing renders in a browser,
which the probe's plain GET cannot see. That case is registerable, because the
runtime has a browser path the probe does not.

**Never a false 'registered'.** Rows are matched on board identity, host + tenant,
never the id string and never the studio label. One board is registered both as
"Lost Boys Interactive" and "Lost Boys Interactive (Embracer Group)", and the
registry is internally inconsistent about trailing slashes — of 2,030 active static
rows, 778 end in `/` and 1,252 do not.

**No silent partial write.** The plan is printed, its size asserted against an
expected value, and nothing is written without `--apply`. A backup goes to `_out/`
and the file is read back afterwards. A partial registry is worse than an unchanged
one, because the missing rows look absent rather than unfinished.

Usage:
  python tools/coverage_register.py --boards _out/coverage/boards.json \\\\
      --verified _out/coverage/verified.json --registry <registry.json>   # dry run
  python tools/coverage_register.py ... --apply --expect-rows 712
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from coverage_board_identity import (  # noqa: E402, I001
    candidate_identity,
    registry_identities,
)
from coverage_verify import (  # noqa: E402, I001
    VERDICT_COLLECTS,
    VERDICT_EMPTY,
    VERDICT_UNKNOWN,
)

# ``unknown`` reasons that mean "the board is real, this probe cannot see it".
#
# A plain GET sees rows on only 55 of the 159 registered boards the runtime
# successfully collects; the other 101 reach their rows only through the runtime's
# browser path. Measured examples: careers.wbd.com/jobs has 24 anchors for 80
# rendered rows, and activategames.bamboohr.com/careers has 1 anchor for 17.
#
# So a JS-rendered listing is evidence the board is alive, not evidence against it.
# Refusing to register these would discard 4,074 openings for the sake of a
# verification method the runtime does not use.
REGISTERABLE_UNKNOWN_REASONS = ("JS-rendered",)

# Reasons that are genuinely undecided and must not be registered blind.
REVIEW_UNKNOWN_REASONS = ("control failed", "HTTP", "unrecognised shape", "no list endpoint")


def classify(candidate: Mapping[str, Any], verdict_row: Mapping[str, Any] | None) -> str:
    """``register``, ``review``, or ``skip`` for one candidate."""
    verdict = str((verdict_row or {}).get("verdict") or VERDICT_UNKNOWN)
    if verdict == VERDICT_COLLECTS:
        return "register"
    if verdict == VERDICT_EMPTY:
        # An empty board is not worth a row: registering it buys no openings and adds
        # a source that will report zero forever.
        return "skip"
    reason = str((verdict_row or {}).get("reason") or "")
    if any(marker in reason for marker in REGISTERABLE_UNKNOWN_REASONS):
        return "register"
    # Everything else unknown is undecided: an HTTP error, an unrecognised shape, or a
    # failed adapter control. Those are listed so the set is explicit, but they all
    # land on the same answer, which is why there is no branch between them.
    assert REVIEW_UNKNOWN_REASONS, "the review markers are documented, not branched on"
    return "review"


def build_rows(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Registry rows in the shape the curated tables use."""
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        adapter = str(candidate.get("adapter") or "")
        companies = list(candidate.get("companies") or [])
        studio = companies[0] if companies else str(candidate.get("tenant") or "")
        row: dict[str, Any] = {
            "name": f"{studio} ({adapter})" if studio else str(candidate.get("id")),
            "studio": studio,
            "adapter": adapter,
            "nlPriority": False,
        }
        for field in (
            "slug",
            "account",
            "board_url",
            "api_url",
            "feed_url",
            "listing_url",
            "company_id",
        ):
            value = candidate.get(field)
            if value:
                row[field] = value
        if not row.get("listing_url") and not row.get("board_url") and not row.get("api_url"):
            row["careersUrl"] = f"https://{candidate.get('host')}"
        rows.append(row)
    return rows


def select(
    candidates: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    registry_ids: Sequence[str] = (),
) -> dict[str, list[dict[str, Any]]]:
    """Split candidates into register / review / skip, dropping known duplicates.

    ``registry_ids`` is passed in rather than stashed on the candidates: a set on
    the row is not JSON-serialisable, and the proposed rows get written to disk.
    """
    verdicts = {str(row.get("id")): row for row in results if row.get("id")}
    known = registry_identities(registry_ids)
    out: dict[str, list[dict[str, Any]]] = {"register": [], "review": [], "skip": []}
    seen: set[tuple[str, str, str]] = set()
    for candidate in candidates:
        identity = candidate_identity(candidate)
        if identity in known or identity in seen:
            continue
        seen.add(identity)
        bucket = classify(candidate, verdicts.get(str(candidate.get("id"))))
        entry = {**candidate, "decision": bucket}
        if bucket == "register":
            entry["openings"] = int(candidate.get("missingCount") or 0)
        out[bucket].append(entry)
    return out


def load_registry_ids(path: Path | None) -> set[str]:
    if not path or not path.exists():
        return set()
    raw = json.loads(path.read_text(encoding="utf-8"))
    rows = raw if isinstance(raw, list) else raw.get("rows") or []
    return {str(row.get("id")) for row in rows if isinstance(row, Mapping) and row.get("id")}


def render(buckets: Mapping[str, Sequence[Mapping[str, Any]]], limit: int = 25) -> str:
    register = buckets.get("register") or []
    review = buckets.get("review") or []
    skip = buckets.get("skip") or []
    lines = [
        f"  register : {len(register):>5} boards  {sum(int(b.get('openings') or 0) for b in register):>6} openings",
        f"  review   : {len(review):>5} boards  (undecided verdict, needs a human or the browser path)",
        f"  skip     : {len(skip):>5} boards  (verified empty)",
        "",
        f"{'openings':>8} {'adapter':<16} id",
        "-" * 100,
    ]
    for row in register[:limit]:
        lines.append(
            f"{int(row.get('openings') or 0):>8} {str(row.get('adapter')):<16} {str(row.get('id'))[:62]}"
        )
    if len(register) > limit:
        lines.append(f"  ... and {len(register) - limit} more")
    return "\n".join(lines)


def apply_rows(registry_path: Path, rows: Sequence[Mapping[str, Any]], backup_dir: Path) -> int:
    """Append rows to the registry, backing up first and reading back after."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / registry_path.name
    shutil.copy2(registry_path, backup)

    raw = json.loads(registry_path.read_text(encoding="utf-8"))
    existing = raw if isinstance(raw, list) else list(raw.get("rows") or [])
    existing_ids = {str(row.get("id")) for row in existing if isinstance(row, Mapping)}

    added = 0
    for row in rows:
        board_id = str(row.pop("_id", "") or "")
        if not board_id or board_id in existing_ids:
            continue
        existing.append({**row, "id": board_id})
        existing_ids.add(board_id)
        added += 1

    registry_path.write_text(
        json.dumps(existing, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    # Read back rather than trusting the write.
    readback = json.loads(registry_path.read_text(encoding="utf-8"))
    readback_ids = {str(r.get("id")) for r in readback if isinstance(r, Mapping)}
    missing = {r.get("_id") for r in rows if r.get("_id")} - readback_ids
    if missing:
        raise SystemExit(
            f"read-back found {len(missing)} rows absent after write: {sorted(missing)[:5]}"
        )
    print(f"backup: {backup}")
    print(f"added {added} rows to {registry_path} (registry now {len(readback)} rows)")
    return added


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--boards", required=True, type=Path, help="boards.json")
    parser.add_argument("--verified", required=True, type=Path, help="verified.json")
    parser.add_argument(
        "--registry", type=Path, help="registry json to read and, with --apply, write"
    )
    parser.add_argument("--out", type=Path, help="where to write proposed rows (dry run)")
    parser.add_argument("--backup-dir", type=Path, default=Path("_out/coverage/registry-backups"))
    parser.add_argument("--apply", action="store_true", help="actually write to the registry")
    parser.add_argument(
        "--expect-rows",
        type=int,
        help="assert the register count before writing; abort on mismatch",
    )
    parser.add_argument("--adapter", help="only consider this adapter")
    parser.add_argument("--show", type=int, default=25)
    args = parser.parse_args(argv)

    boards = json.loads(args.boards.read_text(encoding="utf-8"))["candidates"]
    results = json.loads(args.verified.read_text(encoding="utf-8"))["results"]
    registry_ids = load_registry_ids(args.registry)

    candidates = [c for c in boards if c.get("status") == "new_candidate"]
    if args.adapter:
        candidates = [c for c in candidates if str(c.get("adapter")) == args.adapter]

    buckets = select(candidates, results, registry_ids)
    register_rows = [{**r, "_id": r["id"]} for r in buckets["register"]]
    print(render(buckets, limit=args.show))

    proposed = build_rows(register_rows)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            json.dumps(
                {"rows": proposed, "review": list(buckets["review"])}, indent=2, ensure_ascii=False
            ),
            encoding="utf-8",
        )
        print(f"proposed rows written to {args.out} (not applied)")

    if not args.apply:
        print("\ndry run: nothing written. Re-run with --apply to write.")
        return 0

    if not args.registry:
        raise SystemExit("--apply requires --registry")
    if args.expect_rows is not None and len(register_rows) != args.expect_rows:
        raise SystemExit(
            f"expected {args.expect_rows} registerable rows, found {len(register_rows)}; refusing to write"
        )
    apply_rows(args.registry, register_rows, args.backup_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
