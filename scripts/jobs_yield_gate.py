#!/usr/bin/env python3
"""Read-only helpers for jobs adapter yield gates."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.adapters.api import default_source_loaders
from src.jobs.adapters.static_sources import static_source_name_for_registry_row
from src.jobs.text_utils import clean_text
from src.source_registry import source_identity, source_url_fingerprint

DELETION_PENDING_REASONS = frozenset(
    {
        "stale_or_dead_static_source",
        "unsupported_static_source",
        "site_changed_static_source",
        "redundant_static_stronger_coverage",
    }
)

RECOVERABLE_SIGNALS = frozenset(
    {
        "anti_bot_or_challenge",
        "blocked_or_challenge",
        "rate_limited",
        "timeout",
        "browser_required_provider_migration",
        "browser_required_static_source",
    }
)

EMPTY_SIGNALS = frozenset({"empty_confirmed", "legit_empty", "no_openings"})

DELETE_EVIDENCE_SIGNALS = frozenset(
    {
        "dead_listing_page",
        "site_changed",
        "site_changed_static_source",
        "stale_or_dead_static_source",
        "unsupported_static_source",
        "redundant_static_stronger_coverage",
    }
)


def _resolve_report_path(value: str) -> Path:
    path = Path(value)
    if path.is_dir():
        return path / "jobs-fetch-report.json"
    return path


def _load_report(value: str) -> dict[str, Any]:
    path = _resolve_report_path(value)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _load_json_array(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _source_state_rows(path: Path) -> dict[str, dict[str, Any]]:
    payload = _load_json_object(path)
    sources = payload.get("sources")
    if not isinstance(sources, dict):
        return {}
    return {str(key): value for key, value in sources.items() if isinstance(value, dict)}


def _source_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = report.get("sources")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _source_key(row: dict[str, Any]) -> str:
    return clean_text(row.get("name") or row.get("source") or row.get("adapter"))


def _source_keys_for_row(row: dict[str, Any]) -> set[str]:
    keys = {
        clean_text(row.get("id")),
        clean_text(row.get("name")),
        source_identity(row),
        source_url_fingerprint(row),
    }
    if clean_text(row.get("adapter")) == "static":
        keys.add(static_source_name_for_registry_row(row))
    return {key for key in keys if key}


def _state_for_row(row: dict[str, Any], state_rows: dict[str, dict[str, Any]]) -> dict[str, Any]:
    for key in _source_keys_for_row(row):
        state = state_rows.get(key)
        if isinstance(state, dict):
            return state
    return {}


def _row_is_tombstoned(row: dict[str, Any], tombstones: dict[str, Any]) -> bool:
    if not tombstones:
        return False
    return any(key in tombstones for key in _source_keys_for_row(row))


def _int_value(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = clean_text(value).lower()
    return text in {"1", "true", "yes", "y", "on"}


def _signals_from_row(row: dict[str, Any]) -> set[str]:
    keys = (
        "classification",
        "failureBucket",
        "zeroKeptClassification",
        "pendingReason",
        "lastFailureBucket",
        "lastZeroKeptClassification",
    )
    return {clean_text(row.get(key)).lower() for key in keys if clean_text(row.get(key))}


def _has_recoverable_signal(row: dict[str, Any]) -> bool:
    if _bool_value(row.get("browserFallbackRecommended")) or _bool_value(
        row.get("browserEscalationEligible")
    ):
        return True
    return bool(_signals_from_row(row) & RECOVERABLE_SIGNALS)


def _has_empty_signal(row: dict[str, Any]) -> bool:
    return bool(_signals_from_row(row) & EMPTY_SIGNALS)


def _candidate_source_name(row: dict[str, Any]) -> str:
    if clean_text(row.get("adapter")) == "static":
        return static_source_name_for_registry_row(row)
    return clean_text(row.get("name")) or source_identity(row)


def _candidate_record(
    *,
    row: dict[str, Any],
    bucket: str,
    reason: str,
    state: dict[str, Any],
    score: int,
) -> dict[str, Any]:
    source_name = _candidate_source_name(row)
    return {
        "sourceName": source_name,
        "bucket": bucket,
        "name": clean_text(row.get("name")),
        "studio": clean_text(row.get("studio")),
        "adapter": clean_text(row.get("adapter")),
        "pendingReason": clean_text(row.get("pendingReason")),
        "reason": reason,
        "score": score,
        "lastKeptCount": _int_value(state.get("lastKeptCount")),
        "consecutiveZeroKept": _int_value(state.get("consecutiveZeroKept")),
        "consecutiveFailures": _int_value(state.get("consecutiveFailures")),
        "browserEscalationFailureCount": _int_value(state.get("browserEscalationFailureCount")),
        "lastFailureBucket": clean_text(state.get("lastFailureBucket")),
        "lastCheckedAt": clean_text(state.get("lastCheckedAt")),
        "lastNonEmptyAt": clean_text(state.get("lastNonEmptyAt")),
        "sourceIdentity": source_identity(row),
        "sourceUrlFingerprint": source_url_fingerprint(row),
        "sourceRow": row,
    }


def collect_dead_source_candidates(
    *,
    active_path: Path,
    pending_path: Path,
    tombstones_path: Path,
    state_path: Path,
    min_zero_runs: int = 3,
) -> list[dict[str, Any]]:
    state_rows = _source_state_rows(state_path)
    tombstones = _load_json_object(tombstones_path)
    candidates: list[dict[str, Any]] = []

    for row in _load_json_array(pending_path):
        if clean_text(row.get("adapter")) != "static" or _row_is_tombstoned(row, tombstones):
            continue
        pending_reason = clean_text(row.get("pendingReason")).lower()
        if pending_reason not in DELETION_PENDING_REASONS:
            continue
        state = _state_for_row(row, state_rows)
        if _has_recoverable_signal(row) or _has_empty_signal(row):
            continue
        candidates.append(
            _candidate_record(
                row=row,
                bucket="pending",
                reason=pending_reason,
                state=state,
                score=100 + _int_value(state.get("consecutiveZeroKept")),
            )
        )

    for row in _load_json_array(active_path):
        if clean_text(row.get("adapter")) != "static" or _row_is_tombstoned(row, tombstones):
            continue
        state = _state_for_row(row, state_rows)
        if _int_value(state.get("lastKeptCount")) > 0:
            continue
        zero_runs = _int_value(state.get("consecutiveZeroKept"))
        if zero_runs < max(1, int(min_zero_runs or 1)):
            continue
        state_signals = _signals_from_row(state)
        if _has_recoverable_signal(state) or _has_empty_signal(state):
            continue
        reason = next(iter(sorted(state_signals & DELETE_EVIDENCE_SIGNALS)), "repeated_zero_kept")
        candidates.append(
            _candidate_record(
                row=row,
                bucket="active",
                reason=reason,
                state=state,
                score=zero_runs,
            )
        )

    candidates.sort(
        key=lambda row: (
            -_int_value(row.get("score")),
            clean_text(row.get("bucket")),
            clean_text(row.get("sourceName")),
        )
    )
    return candidates


def _summary_by_source(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for row in _source_rows(report):
        key = _source_key(row)
        if not key:
            continue
        entry = summaries.setdefault(
            key,
            {
                "source": key,
                "keptCount": 0,
                "fetchedCount": 0,
                "statusCounts": Counter(),
                "failureBuckets": Counter(),
            },
        )
        entry["keptCount"] += max(0, int(row.get("keptCount") or 0))
        entry["fetchedCount"] += max(0, int(row.get("fetchedCount") or 0))
        status = clean_text(row.get("status")).lower() or "unknown"
        entry["statusCounts"][status] += 1
        bucket = clean_text(row.get("failureBucket") or row.get("zeroKeptClassification"))
        if bucket:
            entry["failureBuckets"][bucket] += 1
    return summaries


def _report_rows_by_source(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in _source_rows(report):
        key = _source_key(row)
        if key:
            rows[key] = row
    return rows


def _candidate_reason_by_source(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return {}
    reasons: dict[str, str] = {}
    for row in payload:
        if not isinstance(row, dict):
            continue
        source = clean_text(row.get("sourceName"))
        if source:
            reasons[source] = clean_text(row.get("reason"))
    return reasons


def _delete_decision_for_source(
    source: str,
    first: dict[str, Any],
    second: dict[str, Any],
    *,
    candidate_reason: str = "",
) -> dict[str, Any]:
    first_kept = _int_value(first.get("keptCount"))
    second_kept = _int_value(second.get("keptCount"))
    combined = {
        **{f"first_{key}": value for key, value in first.items()},
        **{f"second_{key}": value for key, value in second.items()},
        "pendingReason": candidate_reason,
    }
    signals = _signals_from_row(first) | _signals_from_row(second) | {candidate_reason}
    signals = {signal for signal in signals if signal}
    recoverable = _has_recoverable_signal(first) or _has_recoverable_signal(second)
    empty = _has_empty_signal(first) or _has_empty_signal(second)
    eligible = (
        first_kept == 0
        and second_kept == 0
        and not recoverable
        and not empty
        and bool(signals & DELETE_EVIDENCE_SIGNALS)
    )
    reason = "delete" if eligible else "defer"
    if first_kept > 0 or second_kept > 0:
        reason = "defer_nonzero_yield"
    elif recoverable:
        reason = "defer_recoverable"
    elif empty:
        reason = "defer_legit_empty"
    elif not bool(signals & DELETE_EVIDENCE_SIGNALS):
        reason = "defer_no_delete_signal"
    return {
        "sourceName": source,
        "deleteEligible": eligible,
        "decision": reason,
        "firstKeptCount": first_kept,
        "secondKeptCount": second_kept,
        "firstStatus": clean_text(first.get("status")),
        "secondStatus": clean_text(second.get("status")),
        "signals": ",".join(sorted(signals)),
        "browserFallbackRecommended": recoverable,
        "details": combined,
    }


def collect_dead_source_decisions(
    *,
    first_report: dict[str, Any],
    second_report: dict[str, Any],
    candidates_path: Path | None = None,
) -> list[dict[str, Any]]:
    first_rows = _report_rows_by_source(first_report)
    second_rows = _report_rows_by_source(second_report)
    candidate_reasons = _candidate_reason_by_source(candidates_path)
    sources = sorted(set(first_rows) | set(second_rows) | set(candidate_reasons))
    return [
        _delete_decision_for_source(
            source,
            first_rows.get(source, {}),
            second_rows.get(source, {}),
            candidate_reason=candidate_reasons.get(source, ""),
        )
        for source in sources
    ]


def _format_counter(counter: Counter[str]) -> str:
    return ",".join(f"{key}:{counter[key]}" for key in sorted(counter))


def list_static_sources(args: argparse.Namespace) -> int:
    rows = [
        name
        for name, _loader in default_source_loaders(social_enabled=False)
        if clean_text(name).startswith("static_source::")
    ]
    if args.contains:
        needle = clean_text(args.contains).lower()
        rows = [name for name in rows if needle in name.lower()]
    rows = rows[: max(0, int(args.limit or 0))] if int(args.limit or 0) > 0 else rows
    if args.json:
        print(json.dumps(rows, indent=2, ensure_ascii=False))
    else:
        for name in rows:
            print(name)
    return 0


def _print_records(records: list[dict[str, Any]], *, json_output: bool, only_sources: bool) -> None:
    if only_sources:
        print(
            ",".join(clean_text(row.get("sourceName")) for row in records if row.get("sourceName"))
        )
        return
    if json_output:
        print(json.dumps(records, indent=2, ensure_ascii=False))
        return
    if not records:
        return
    keys = [key for key in records[0] if key not in {"details", "sourceRow"}]
    print("\t".join(keys))
    for row in records:
        print("\t".join(clean_text(row.get(key)) for key in keys))


def dead_source_candidates(args: argparse.Namespace) -> int:
    records = collect_dead_source_candidates(
        active_path=Path(args.active),
        pending_path=Path(args.pending),
        tombstones_path=Path(args.tombstones),
        state_path=Path(args.state),
        min_zero_runs=args.min_zero_runs,
    )
    if args.limit and args.limit > 0:
        records = records[: args.limit]
    _print_records(records, json_output=args.json, only_sources=args.only_sources)
    return 0


def dead_source_registry(args: argparse.Namespace) -> int:
    payload = json.loads(Path(args.candidates).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{args.candidates} must contain a JSON array")
    rows: list[dict[str, Any]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        row = item.get("sourceRow")
        if not isinstance(row, dict):
            continue
        active = dict(row)
        active["enabledByDefault"] = True
        active["registryState"] = "active"
        active.pop("pendingReason", None)
        rows.append(active)
    print(json.dumps(rows, indent=2, ensure_ascii=False))
    return 0


def compare_reports(args: argparse.Namespace) -> int:
    before = _summary_by_source(_load_report(args.before))
    after = _summary_by_source(_load_report(args.after))
    keys = sorted(set(before) | set(after))
    drops: list[str] = []
    print(
        "source\tbefore_kept\tafter_kept\tbefore_status\tafter_status\t"
        "before_failures\tafter_failures"
    )
    for key in keys:
        before_row = before.get(key, {})
        after_row = after.get(key, {})
        before_kept = int(before_row.get("keptCount") or 0)
        after_kept = int(after_row.get("keptCount") or 0)
        if after_kept < before_kept:
            drops.append(key)
        print(
            "\t".join(
                [
                    key,
                    str(before_kept),
                    str(after_kept),
                    _format_counter(before_row.get("statusCounts") or Counter()),
                    _format_counter(after_row.get("statusCounts") or Counter()),
                    _format_counter(before_row.get("failureBuckets") or Counter()),
                    _format_counter(after_row.get("failureBuckets") or Counter()),
                ]
            )
        )
    if drops and not args.allow_drops:
        print("Yield drops detected: " + ", ".join(drops), file=sys.stderr)
        return 1
    return 0


def dead_source_decisions(args: argparse.Namespace) -> int:
    records = collect_dead_source_decisions(
        first_report=_load_report(args.first),
        second_report=_load_report(args.second),
        candidates_path=Path(args.candidates) if args.candidates else None,
    )
    if args.only_delete:
        records = [row for row in records if row.get("deleteEligible")]
    _print_records(records, json_output=args.json, only_sources=args.only_sources)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only jobs adapter yield gate helpers.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser(
        "list-static-sources",
        help="Print valid generated static source IDs for --only-sources gates.",
    )
    list_parser.add_argument("--limit", type=int, default=20)
    list_parser.add_argument("--contains", default="")
    list_parser.add_argument("--json", action="store_true")
    list_parser.set_defaults(func=list_static_sources)

    candidates_parser = subparsers.add_parser(
        "dead-source-candidates",
        help="Rank static source rows that need fresh dead-source deletion evidence.",
    )
    candidates_parser.add_argument("--active", default="data/source-registry-active.json")
    candidates_parser.add_argument("--pending", default="data/source-registry-pending.json")
    candidates_parser.add_argument("--tombstones", default="data/source-registry-tombstones.json")
    candidates_parser.add_argument("--state", default="data/jobs-source-state.json")
    candidates_parser.add_argument("--min-zero-runs", type=int, default=3)
    candidates_parser.add_argument("--limit", type=int, default=50)
    candidates_parser.add_argument("--json", action="store_true")
    candidates_parser.add_argument("--only-sources", action="store_true")
    candidates_parser.set_defaults(func=dead_source_candidates)

    registry_parser = subparsers.add_parser(
        "dead-source-registry",
        help="Emit candidate source rows as a temporary active registry JSON array.",
    )
    registry_parser.add_argument("candidates", help="Candidate JSON from dead-source-candidates.")
    registry_parser.set_defaults(func=dead_source_registry)

    compare_parser = subparsers.add_parser(
        "compare",
        help="Compare before/after jobs-fetch-report.json source kept counts.",
    )
    compare_parser.add_argument("before", help="Before output dir or jobs-fetch-report.json path.")
    compare_parser.add_argument("after", help="After output dir or jobs-fetch-report.json path.")
    compare_parser.add_argument("--allow-drops", action="store_true")
    compare_parser.set_defaults(func=compare_reports)

    decisions_parser = subparsers.add_parser(
        "dead-source-decisions",
        help="Classify two fresh evidence reports into delete/defer source decisions.",
    )
    decisions_parser.add_argument("first", help="First output dir or jobs-fetch-report.json path.")
    decisions_parser.add_argument(
        "second", help="Second output dir or jobs-fetch-report.json path."
    )
    decisions_parser.add_argument("--candidates", default="")
    decisions_parser.add_argument("--json", action="store_true")
    decisions_parser.add_argument("--only-delete", action="store_true")
    decisions_parser.add_argument("--only-sources", action="store_true")
    decisions_parser.set_defaults(func=dead_source_decisions)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
