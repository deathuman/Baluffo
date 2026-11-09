#!/usr/bin/env python3
"""Report or repair carried source-bundle entries that belong to another row.

The command reads a canonical jobs feed, fingerprints each ``sourceBundle``
entry's ``jobLink`` and each row's primary ``jobLink``, and moves entries whose
normalized link matches a different surviving row. It never changes row order,
IDs, titles, or dedupe policy.

Dry-run is the default. ``--apply`` writes a timestamped backup first, then
rewrites the feed and updates ``sourceBundleCount`` on both rows. JSON arrays,
gzip JSON, JSONL, and gzip JSONL are supported.

Exit codes:
  0  clean, or every movable mismatch was applied
  1  dry-run found movable mismatches
  2  invalid input, live-data refusal, or ambiguous target rows
"""

from __future__ import annotations

import argparse
import gzip
import json
import shutil
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.common.source_bundle_reconciliation import (  # noqa: E402
    ReconciliationPlan,
    apply_source_bundle_reconciliation,
    plan_source_bundle_reconciliation,
)


def _configure_console_output() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (OSError, ValueError):
            continue


def _is_jsonl(path: Path) -> bool:
    return path.name.lower().endswith((".jsonl", ".jsonl.gz", ".ndjson", ".ndjson.gz"))


def _read_feed(path: Path) -> list[dict[str, Any]]:
    opener = gzip.open if path.suffix.lower() == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        if _is_jsonl(path):
            rows = [json.loads(line) for line in handle if line.strip()]
        else:
            rows = json.load(handle)
    if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
        raise ValueError("feed must be a JSON array or JSONL of row objects")
    return rows


def _write_feed(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    if _is_jsonl(path):
        payload = "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n" for row in rows
        )
    else:
        payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":"))
    if path.suffix.lower() == ".gz":
        with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
            handle.write(payload)
    else:
        path.write_text(payload, encoding="utf-8")


def _row_label(row: dict[str, Any], index: int) -> str:
    value = row.get("id")
    return str(value if value is not None else index)


def _is_live_feed(path: Path) -> bool:
    live_data = (_ROOT / "data").resolve()
    try:
        path.resolve().relative_to(live_data)
        return True
    except ValueError:
        return False


def _report(
    *,
    feed: Path,
    rows: Sequence[dict[str, Any]],
    plan: ReconciliationPlan,
    applied: bool,
    backup_path: Path | None,
) -> dict[str, Any]:
    moves = [
        {
            "sourceRowId": _row_label(rows[move.source_index], move.source_index),
            "sourceTitle": rows[move.source_index].get("title") or "",
            "sourceJobLink": rows[move.source_index].get("jobLink") or "",
            "entry": move.entry,
            "targetRowId": _row_label(rows[move.target_index], move.target_index),
            "targetTitle": rows[move.target_index].get("title") or "",
            "targetJobLink": rows[move.target_index].get("jobLink") or "",
            "targetAlreadyHadEntry": move.target_has_entry,
        }
        for move in plan.moves
    ]
    ambiguous = [
        {
            "sourceRowId": _row_label(rows[item.source_index], item.source_index),
            "sourceTitle": rows[item.source_index].get("title") or "",
            "entry": item.entry,
            "candidateRowIds": [_row_label(rows[index], index) for index in item.candidate_indices],
        }
        for item in plan.ambiguous
    ]
    return {
        "feed": str(feed),
        "format": "jsonl" if _is_jsonl(feed) else "json",
        "rowCount": len(rows),
        "mismatchCount": plan.mismatch_count,
        "moveCount": len(plan.moves),
        "ambiguousCount": len(plan.ambiguous),
        "applied": applied,
        "backupPath": str(backup_path) if backup_path else None,
        "moves": moves,
        "ambiguous": ambiguous,
    }


def _print_report(report: dict[str, Any], *, applied: bool) -> None:
    if not report["mismatchCount"]:
        print(f"{report['feed']}: clean ({report['rowCount']} rows)")
        return
    print(
        f"{report['feed']}: {report['rowCount']} rows; "
        f"{report['moveCount']} movable, {report['ambiguousCount']} ambiguous"
    )
    for move in report["moves"]:
        entry = move["entry"]
        print(
            f"  {move['sourceRowId']} -> {move['targetRowId']}: "
            f"{entry.get('sourceJobId') or entry.get('jobLink')}"
        )
    for item in report["ambiguous"]:
        print(f"  {item['sourceRowId']}: ambiguous targets {', '.join(item['candidateRowIds'])}")
    if applied:
        print(f"applied {report['moveCount']} move(s); backup: {report['backupPath']}")
    elif report["moveCount"]:
        print("dry run; rerun with --apply to move these entries")


def main(argv: Sequence[str] | None = None) -> int:
    _configure_console_output()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--feed", required=True, type=Path, help="jobs feed to inspect")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="move entries and rewrite the feed; dry-run otherwise",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        help="backup directory for --apply (default: _out/source-bundle-reconciliation/...)",
    )
    parser.add_argument(
        "--allow-live-data",
        action="store_true",
        help="allow --apply to rewrite a feed under data/",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args(argv)

    feed = args.feed.expanduser()
    if not feed.is_file():
        print(f"error: feed not found: {feed}", file=sys.stderr)
        return 2
    if args.apply and _is_live_feed(feed) and not args.allow_live_data:
        print(
            "error: refusing to rewrite live data/ without --allow-live-data",
            file=sys.stderr,
        )
        return 2
    try:
        rows = _read_feed(feed)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: could not read {feed}: {exc}", file=sys.stderr)
        return 2

    plan = plan_source_bundle_reconciliation(rows)
    backup_path: Path | None = None
    applied = bool(args.apply and plan.moves)
    if applied:
        backup_dir = args.backup_dir
        if backup_dir is None:
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            backup_dir = (
                _ROOT
                / "_out"
                / "source-bundle-reconciliation"
                / f"{feed.name.replace('.gz', '')}-{stamp}"
            )
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup_path = backup_dir / feed.name
        shutil.copy2(feed, backup_path)
        apply_source_bundle_reconciliation(rows, plan)
        _write_feed(feed, rows)

    report = _report(
        feed=feed,
        rows=rows,
        plan=plan,
        applied=applied,
        backup_path=backup_path,
    )
    if args.as_json:
        print(json.dumps(report, indent=2, ensure_ascii=True))
    else:
        _print_report(report, applied=applied)

    if plan.ambiguous:
        return 2
    if applied or not plan.moves:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
