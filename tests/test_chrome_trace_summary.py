from __future__ import annotations

import gzip
import json
from pathlib import Path

from scripts import perf_complete
from scripts.chrome_trace_summary import summarize_trace_file


def _write_trace(path: Path) -> None:
    events = [
        {
            "name": "admin_dashboard_health_fetch",
            "cat": "blink.user_timing",
            "ph": "b",
            "pid": 1,
            "tid": 2,
            "id2": {"local": "0x1"},
            "ts": 1_000_000,
            "args": {"startTime": 100.0},
        },
        {
            "name": "admin_dashboard_health_fetch",
            "cat": "blink.user_timing",
            "ph": "e",
            "pid": 1,
            "tid": 2,
            "id2": {"local": "0x1"},
            "ts": 19_500_000,
            "args": {},
        },
        {
            "name": "ResourceSendRequest",
            "cat": "devtools.timeline",
            "ph": "I",
            "ts": 2_000_000,
            "args": {
                "data": {
                    "requestId": "1",
                    "requestMethod": "GET",
                    "url": "http://192.168.50.61:8877/ops/dashboard-health?t=1",
                }
            },
        },
        {
            "name": "ResourceReceiveResponse",
            "cat": "devtools.timeline",
            "ph": "I",
            "ts": 20_000_000,
            "args": {
                "data": {
                    "requestId": "1",
                    "statusCode": 200,
                    "mimeType": "application/json",
                }
            },
        },
        {
            "name": "ResourceFinish",
            "cat": "devtools.timeline",
            "ph": "I",
            "ts": 20_250_000,
            "args": {
                "data": {
                    "requestId": "1",
                    "decodedBodyLength": 100,
                    "encodedDataLength": 120,
                    "didFail": False,
                }
            },
        },
        {
            "name": "largestContentfulPaint::Candidate",
            "cat": "loading,devtools.timeline",
            "ph": "R",
            "ts": 26_000_000,
            "args": {
                "data": {
                    "candidateIndex": 2,
                    "nodeName": "SPAN class='admin-fetcher-text'",
                    "size": 196318,
                    "type": "text",
                }
            },
        },
        {
            "name": "FunctionCall",
            "cat": "devtools.timeline",
            "ph": "X",
            "ts": 27_000_000,
            "dur": 95_000,
            "args": {
                "data": {
                    "functionName": "renderFetcher",
                    "url": "http://192.168.50.61:8877/frontend/admin/render.js",
                    "lineNumber": 10,
                }
            },
        },
    ]
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        json.dump({"traceEvents": events}, handle)


def test_chrome_trace_summary_extracts_lcp_resources_and_user_timing(tmp_path: Path) -> None:
    trace_path = tmp_path / "trace.json.gz"
    _write_trace(trace_path)

    summary = summarize_trace_file(trace_path)

    assert summary["ok"] is True
    assert summary["latestLcp"]["nodeName"] == "SPAN class='admin-fetcher-text'"
    assert summary["slowResources"][0]["url"] == "/ops/dashboard-health?t=1"
    assert summary["slowResources"][0]["durationMs"] == 18250.0
    assert summary["slowUserTimings"][0]["name"] == "admin_dashboard_health_fetch"
    assert summary["slowUserTimings"][0]["durationMs"] == 18500.0
    assert summary["longMainThreadTasks"][0]["functionName"] == "renderFetcher"


def test_perf_complete_folds_chrome_trace_into_targets(tmp_path: Path) -> None:
    trace_path = tmp_path / "trace.json.gz"
    _write_trace(trace_path)

    chrome = perf_complete.build_chrome_trace_summary([str(trace_path)])
    targets = perf_complete.build_optimization_targets(
        {
            "discovery": {},
            "fetch": {},
            "frontendBoot": {},
            "startup": {},
            "sync": {},
            "chromeTraces": chrome,
        }
    )

    assert chrome["latestLcp"][0]["nodeName"] == "SPAN class='admin-fetcher-text'"
    assert any(row["kind"] == "chrome-lcp" for row in targets)
    assert any(row["kind"] == "chrome-resource" for row in targets)
