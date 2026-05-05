from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from pathlib import Path
from typing import Any

DEFAULT_TREND_PATH = Path("_out/perf-trend.ndjson")
DEFAULT_LIMIT = 20


def _coerce_duration_ms(row: dict[str, Any]) -> int | None:
    raw = row.get("totalDurationMs", row.get("durationMs"))
    try:
        duration = int(float(raw))
    except (TypeError, ValueError):
        return None
    return duration if duration >= 0 else None


def _normalize_row(row: dict[str, Any], *, index: int) -> dict[str, Any] | None:
    mode = str(row.get("mode") or "").strip()
    timestamp = str(row.get("ts") or row.get("timestamp") or "").strip()
    duration_ms = _coerce_duration_ms(row)
    if not mode or duration_ms is None:
        return None
    return {
        "index": index,
        "ts": timestamp,
        "mode": mode,
        "totalDurationMs": duration_ms,
        "status": str(row.get("status") or "").strip(),
        "commitSha": str(row.get("commitSha") or row.get("sha") or "").strip(),
        "sourceCount": row.get("sourceCount"),
        "adapterCount": row.get("adapterCount"),
        "stageDurationsMs": row.get("stageDurationsMs") if isinstance(row.get("stageDurationsMs"), dict) else {},
    }


def load_trend_rows(path: Path = DEFAULT_TREND_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    for index, line in enumerate(path.read_text(encoding="utf-8").splitlines()):
        text = line.strip()
        if not text:
            continue
        try:
            raw = json.loads(text)
        except json.JSONDecodeError:
            continue
        if not isinstance(raw, dict):
            continue
        normalized = _normalize_row(raw, index=index)
        if normalized is not None:
            rows.append(normalized)
    return rows


def _format_duration(duration_ms: int) -> str:
    return f"{duration_ms / 1000:.1f}s"


def _format_delta(current_ms: int, comparison_ms: int | None) -> str:
    if not comparison_ms:
        return "--"
    delta = ((current_ms - comparison_ms) / comparison_ms) * 100
    return f"{delta:+.1f}%"


def _date_label(row: dict[str, Any]) -> str:
    timestamp = str(row.get("ts") or "").strip()
    if not timestamp:
        return "--"
    return timestamp[:10]


def _format_stage_durations(stages: dict[str, Any]) -> str:
    parts: list[str] = []
    for stage in sorted(stages):
        try:
            duration = int(float(stages[stage]))
        except (TypeError, ValueError):
            continue
        if duration > 0:
            parts.append(f"{stage}={_format_duration(duration)}")
    return ", ".join(parts) or "--"


def trend_entries(
    rows: Iterable[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    include_stages: bool = False,
) -> list[dict[str, str]]:
    ordered = list(rows)
    baseline_by_mode: dict[str, int] = {}
    previous_by_mode: dict[str, int] = {}
    entries: list[dict[str, str]] = []

    for row in ordered:
        mode = str(row.get("mode") or "").strip()
        duration_ms = _coerce_duration_ms(row)
        if not mode or duration_ms is None:
            continue
        baseline = baseline_by_mode.setdefault(mode, duration_ms)
        previous = previous_by_mode.get(mode)
        previous_by_mode[mode] = duration_ms
        entries.append(
            {
                "mode": mode,
                "date": _date_label(row),
                "duration": _format_duration(duration_ms),
                "vsPrev": _format_delta(duration_ms, previous),
                "vsBaseline": "--" if previous is None else _format_delta(duration_ms, baseline),
                "status": str(row.get("status") or "--").strip() or "--",
                "commit": str(row.get("commitSha") or "--").strip()[:7] or "--",
                "stages": _format_stage_durations(dict(row.get("stageDurationsMs") or {}))
                if include_stages
                else "",
            }
        )

    return entries[-max(0, int(limit)) :] if limit else entries


def format_trend_table(
    rows: Iterable[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    include_stages: bool = False,
) -> str:
    entries = trend_entries(rows, limit=limit, include_stages=include_stages)
    if not entries:
        return "No perf trend rows found."

    headers = ("mode", "date", "duration", "vs prev", "vs baseline", "status", "commit")
    if include_stages:
        headers = (*headers, "stages")
    table_rows = [
        (
            entry["mode"],
            entry["date"],
            entry["duration"],
            entry["vsPrev"],
            entry["vsBaseline"],
            entry["status"],
            entry["commit"],
            *([entry["stages"]] if include_stages else []),
        )
        for entry in entries
    ]
    widths = [
        max(len(str(value)) for value in (header, *(row[index] for row in table_rows)))
        for index, header in enumerate(headers)
    ]
    lines = [
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)),
        "  ".join("-" * width for width in widths),
    ]
    lines.extend(
        "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))
        for row in table_rows
    )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print Baluffo performance trend deltas.")
    parser.add_argument(
        "path",
        nargs="?",
        default=str(DEFAULT_TREND_PATH),
        help="Path to perf trend NDJSON file.",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Rows to print.")
    parser.add_argument("--stages", action="store_true", help="Include stage duration details.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    path = Path(str(args.path))
    rows = load_trend_rows(path)
    if not rows:
        print(f"No perf trend data found at {path}.")
        return 0
    print(format_trend_table(rows, limit=int(args.limit), include_stages=bool(args.stages)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
