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
from typing import Any, cast

_ROOT_DIR = str(Path(__file__).resolve().parents[1])
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)

_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

# The guardrail check this tool must satisfy before writing. Imported here rather than inside
# `apply_rows` so a missing path fails at startup with a clear name instead of mid-write.
_REPO_HEALTH_DIR = str(Path(__file__).resolve().parent / "repo_health")
if _REPO_HEALTH_DIR not in sys.path:
    sys.path.insert(0, _REPO_HEALTH_DIR)

# Past this line the imports are path-dependent by design: this tool is run from the repo
# root as `python tools/coverage_register.py`, so `sys.path` has to be prepared first.
from source_registry_duplicate_url_policy import (  # noqa: E402
    list_seed_rows_not_registrations,
)

from src.shared.utils import now_iso  # noqa: E402
from src.source_registry_state import transition_registry_to_active  # noqa: E402

# Provenance for rows this tool registers. The boards arrive already verified by
# `coverage_verify` -- a real per-adapter fetch, three-way verdict -- so the honest actor is
# that evidence, not a discovery approval this tool never performed.
REGISTRY_REASON_COVERAGE_VERIFIED = "coverage_verified_register"
COVERAGE_REGISTER_ACTOR = "coverage_verify_register"

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
        if adapter == "workable" and not row.get("api_url"):
            # A Workable row carrying only ``account`` has no endpoint the probe or the
            # runtime can read: ``endpoint_url`` looks at api_url/feed_url/board_url/
            # listing_url, so all 29 boards probed as "missing adapter or URL" and none
            # registered. This is the runtime's own JsonFeedSpec url_template.
            account = str(candidate.get("account") or "")
            if account:
                row["api_url"] = (
                    f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true"
                )
        # Provenance for the curated-seed audit fallback: how many openings this board was
        # observed serving when the audit found it. Without it a curated row whose probe
        # reads zero has nothing to stand in for the probe, and 246 JS-rendered boards
        # would be refused as empty.
        openings = candidate.get("missingCount")
        if openings:
            row["coverageAuditOpenings"] = int(openings)
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


def build_registration_row(candidate: Mapping[str, Any]) -> dict[str, Any]:
    """Turn a coverage candidate into a row that is actually a registration.

    Candidates arrive from ``coverage_boards`` carrying evidence fields and no state. Written
    unchanged they load, count, and collect nothing: ``_infer_registry_state`` reads a missing
    ``registryState`` as ``pending``, and pending rows are not watched. That is how 27 verified
    boards ended up in the seed looking registered while fetching zero.

    The state is stamped through the repo's own transition, so provenance is real rather than
    invented here. ``coverage_register`` has no approval opinion of its own -- it is handed
    boards that ``coverage_verify`` already ran a real fetch for -- so the actor says exactly
    that rather than claiming a discovery approval it did not perform.
    """
    row = {k: v for k, v in candidate.items() if not str(k).startswith("_")}
    # Candidate bookkeeping has no meaning in a registry row and would read as evidence.
    for field in ("decision", "missingCount", "sampleTitles", "status", "companies"):
        row.pop(field, None)

    if str(row.get("adapter") or "") == "workable" and not row.get("api_url"):
        # A Workable row carrying only ``account`` has no endpoint the probe or the
        # runtime can read: ``endpoint_url`` looks at api_url/feed_url/board_url/
        # listing_url, so account-only rows probe as "missing adapter or URL" and none
        # register. This is the runtime's own JsonFeedSpec url_template.
        account = str(row.get("account") or "")
        if account:
            row["api_url"] = (
                f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true"
            )

    board_id = str(candidate.get("_id") or candidate.get("id") or "").strip()
    if board_id:
        row["id"] = board_id

    company = str(row.get("company") or "").strip()
    if company:
        row.setdefault("studio", company)
        row.setdefault("name", f"{company} ({row.get('adapter') or 'board'})")

    return cast(
        dict[str, Any],
        transition_registry_to_active(
            row,
            reason=REGISTRY_REASON_COVERAGE_VERIFIED,
            actor=COVERAGE_REGISTER_ACTOR,
            at=now_iso(),
        ),
    )


def apply_rows(registry_path: Path, rows: Sequence[Mapping[str, Any]], backup_dir: Path) -> int:
    """Append rows to the registry, backing up first and reading back after.

    Every row is built and validated as a *registration* before anything is written, using the
    same check the repo guardrail runs on the committed seed
    (``list_seed_rows_not_registrations``). It is imported rather than restated so the tool
    and the gate cannot drift apart -- this function is how such rows get in, so it owes the
    standard the gate enforces on them afterwards.
    """
    prepared = [build_registration_row(row) for row in rows]
    failures = list_seed_rows_not_registrations(prepared)
    if failures:
        raise SystemExit(
            f"refusing to write {len(failures)} row(s) that are not registrations; the "
            f"committed seed would fail the guardrail:\n  "
            + "\n  ".join(str(message) for message in failures[:5])
        )

    backup_dir.mkdir(parents=True, exist_ok=True)
    backup = backup_dir / registry_path.name
    shutil.copy2(registry_path, backup)

    raw = json.loads(registry_path.read_text(encoding="utf-8"))
    existing = raw if isinstance(raw, list) else list(raw.get("rows") or [])
    existing_ids = {str(row.get("id")) for row in existing if isinstance(row, Mapping)}

    added = 0
    for row in prepared:
        board_id = str(row.get("id") or "")
        if not board_id or board_id in existing_ids:
            continue
        existing.append(row)
        existing_ids.add(board_id)
        added += 1

    registry_path.write_text(
        json.dumps(existing, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    # Read back rather than trusting the write.
    readback = json.loads(registry_path.read_text(encoding="utf-8"))
    readback_ids = {str(r.get("id")) for r in readback if isinstance(r, Mapping)}
    wanted = {str(row.get("id")) for row in prepared if row.get("id")}
    missing = wanted - readback_ids
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

    # Preview exactly what `--apply` will prepare and validate: the same builder, so the
    # operator reviews the stamped registration rather than a candidate-shaped shadow of
    # it (that drift is how the first 0.3.011 run previewed rows it never wrote that way).
    proposed = [build_registration_row(row) for row in register_rows]
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
