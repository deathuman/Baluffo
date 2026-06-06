#!/usr/bin/env python3
"""Summarize Chrome DevTools Performance trace exports."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

DEFAULT_LONG_TASK_MS = 50.0
DEFAULT_LIMIT = 20


def _read_trace(path: Path) -> dict[str, Any]:
    opener = gzip.open if path.suffix.lower() == ".gz" else open
    with opener(path, "rt", encoding="utf-8", errors="replace") as handle:
        payload = json.load(handle)
    if isinstance(payload, list):
        return {"traceEvents": payload}
    if isinstance(payload, dict):
        return payload
    return {"traceEvents": []}


def _event_data(event: dict[str, Any]) -> dict[str, Any]:
    args = event.get("args") if isinstance(event.get("args"), dict) else {}
    data = args.get("data") if isinstance(args.get("data"), dict) else {}
    return data


def _ts_ms(event: dict[str, Any], origin_ts: int) -> float:
    return round((int(event.get("ts") or origin_ts) - origin_ts) / 1000.0, 3)


def _dur_ms(event: dict[str, Any]) -> float:
    return round(int(event.get("dur") or 0) / 1000.0, 3)


def _display_url(value: str) -> str:
    parsed = urlparse(str(value or ""))
    if parsed.scheme and parsed.netloc:
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"
        return path
    return str(value or "")


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _trace_origin_ts(events: list[dict[str, Any]]) -> int:
    navigation_candidates = []
    for event in events:
        if event.get("name") != "ResourceSendRequest":
            continue
        data = _event_data(event)
        url = str(data.get("url") or "")
        parsed = urlparse(url)
        path = parsed.path or "/"
        if path == "/" or path.endswith(".html"):
            navigation_candidates.append(int(event.get("ts") or 0))
    if navigation_candidates:
        return min(value for value in navigation_candidates if value > 0)
    timestamps = [int(event.get("ts") or 0) for event in events if event.get("ts") is not None]
    return min(timestamps) if timestamps else 0


def _summarize_user_timing(
    events: list[dict[str, Any]], *, origin_ts: int, limit: int
) -> list[dict[str, Any]]:
    open_spans: dict[tuple[Any, ...], dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    for event in events:
        if "user_timing" not in str(event.get("cat") or ""):
            continue
        phase = str(event.get("ph") or "")
        name = str(event.get("name") or "")
        key = (
            event.get("pid"),
            event.get("tid"),
            name,
            json.dumps(event.get("id2") or event.get("id") or "", sort_keys=True),
        )
        if phase == "b":
            open_spans[key] = event
        elif phase == "e":
            start = open_spans.pop(key, None)
            if not start:
                continue
            rows.append(
                {
                    "name": name,
                    "startMs": round(
                        float(
                            (start.get("args") if isinstance(start.get("args"), dict) else {}).get(
                                "startTime", _ts_ms(start, origin_ts)
                            )
                        ),
                        3,
                    ),
                    "durationMs": round(
                        (int(event.get("ts") or 0) - int(start.get("ts") or 0)) / 1000.0,
                        3,
                    ),
                }
            )
        elif phase == "X":
            rows.append(
                {
                    "name": name,
                    "startMs": _ts_ms(event, origin_ts),
                    "durationMs": _dur_ms(event),
                }
            )
    return sorted(rows, key=lambda row: float(row.get("durationMs") or 0), reverse=True)[:limit]


def _summarize_lcp(
    events: list[dict[str, Any]], *, origin_ts: int, limit: int
) -> list[dict[str, Any]]:
    candidates = []
    for event in events:
        if event.get("name") != "largestContentfulPaint::Candidate":
            continue
        data = _event_data(event)
        candidates.append(
            {
                "startMs": _ts_ms(event, origin_ts),
                "candidateIndex": _safe_int(data.get("candidateIndex")),
                "nodeName": str(data.get("nodeName") or ""),
                "type": str(data.get("type") or ""),
                "size": _safe_int(data.get("size")),
            }
        )
    return sorted(candidates, key=lambda row: float(row.get("startMs") or 0))[-limit:]


def _summarize_resources(
    events: list[dict[str, Any]], *, origin_ts: int, limit: int
) -> list[dict[str, Any]]:
    requests: dict[str, dict[str, Any]] = {}
    for event in events:
        name = event.get("name")
        data = _event_data(event)
        request_id = str(data.get("requestId") or "")
        if not request_id:
            continue
        row = requests.setdefault(request_id, {"requestId": request_id})
        if name == "ResourceSendRequest":
            row.update(
                {
                    "url": _display_url(str(data.get("url") or "")),
                    "method": str(data.get("requestMethod") or "GET"),
                    "startMs": _ts_ms(event, origin_ts),
                    "_startTs": int(event.get("ts") or 0),
                }
            )
        elif name == "ResourceReceiveResponse":
            row.update(
                {
                    "status": _safe_int(data.get("statusCode")),
                    "mimeType": str(data.get("mimeType") or ""),
                    "responseMs": _ts_ms(event, origin_ts),
                    "_responseTs": int(event.get("ts") or 0),
                }
            )
        elif name == "ResourceFinish":
            row.update(
                {
                    "finishMs": _ts_ms(event, origin_ts),
                    "_finishTs": int(event.get("ts") or 0),
                    "decodedBodyLength": _safe_int(data.get("decodedBodyLength")),
                    "encodedDataLength": _safe_int(data.get("encodedDataLength")),
                    "failed": bool(data.get("didFail")),
                }
            )
    rows = []
    for row in requests.values():
        start_ts = int(row.get("_startTs") or 0)
        finish_ts = int(row.get("_finishTs") or 0)
        response_ts = int(row.get("_responseTs") or 0)
        if start_ts and finish_ts:
            row["durationMs"] = round((finish_ts - start_ts) / 1000.0, 3)
        if start_ts and response_ts:
            row["timeToFirstByteMs"] = round((response_ts - start_ts) / 1000.0, 3)
        for key in ("_startTs", "_finishTs", "_responseTs"):
            row.pop(key, None)
        if row.get("url"):
            rows.append(row)
    return sorted(rows, key=lambda row: float(row.get("durationMs") or 0), reverse=True)[:limit]


def _summarize_long_tasks(
    events: list[dict[str, Any]], *, origin_ts: int, limit: int, threshold_ms: float
) -> list[dict[str, Any]]:
    rows = []
    for event in events:
        duration = _dur_ms(event)
        if duration < threshold_ms:
            continue
        name = str(event.get("name") or "")
        if name not in {"RunTask", "FunctionCall", "EvaluateScript", "TimerFire"}:
            continue
        data = _event_data(event)
        rows.append(
            {
                "name": name,
                "startMs": _ts_ms(event, origin_ts),
                "durationMs": duration,
                "functionName": str(data.get("functionName") or ""),
                "url": _display_url(str(data.get("url") or "")),
                "lineNumber": _safe_int(data.get("lineNumber")),
            }
        )
    return sorted(rows, key=lambda row: float(row.get("durationMs") or 0), reverse=True)[:limit]


def summarize_trace_file(
    path: str | Path, *, limit: int = DEFAULT_LIMIT, long_task_ms: float = DEFAULT_LONG_TASK_MS
) -> dict[str, Any]:
    trace_path = Path(path).expanduser().resolve()
    payload = _read_trace(trace_path)
    events = [event for event in payload.get("traceEvents", []) if isinstance(event, dict)]
    timestamps = [int(event.get("ts") or 0) for event in events if event.get("ts") is not None]
    origin_ts = _trace_origin_ts(events)
    end_ts = max(timestamps) if timestamps else origin_ts
    lcp_candidates = _summarize_lcp(events, origin_ts=origin_ts, limit=limit)
    slow_resources = _summarize_resources(events, origin_ts=origin_ts, limit=limit)
    user_timings = _summarize_user_timing(events, origin_ts=origin_ts, limit=limit)
    long_tasks = _summarize_long_tasks(
        events,
        origin_ts=origin_ts,
        limit=limit,
        threshold_ms=float(long_task_ms),
    )
    return {
        "ok": True,
        "tracePath": str(trace_path),
        "eventCount": len(events),
        "traceDurationMs": round((end_ts - origin_ts) / 1000.0, 3) if timestamps else 0,
        "latestLcp": lcp_candidates[-1] if lcp_candidates else {},
        "lcpCandidates": lcp_candidates,
        "slowResources": slow_resources,
        "slowUserTimings": user_timings,
        "longMainThreadTasks": long_tasks,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize Chrome DevTools trace exports.")
    parser.add_argument("trace", nargs="+", help="Chrome trace .json or .json.gz file.")
    parser.add_argument("--output", default="", help="Optional JSON output path.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--long-task-ms", type=float, default=DEFAULT_LONG_TASK_MS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    traces = [
        summarize_trace_file(path, limit=int(args.limit), long_task_ms=float(args.long_task_ms))
        for path in args.trace
    ]
    payload = {"schemaVersion": 1, "traces": traces}
    text = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    if str(args.output or "").strip():
        output = Path(str(args.output)).expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
