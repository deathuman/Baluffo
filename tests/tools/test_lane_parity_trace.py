"""The request trace has to keep the phase and status of each real request.

These tests use fake fetchers, so they exercise the wrappers rather than the network: a page
job's own `failureStage` is the phase, a probe retry is its own row, and a 429 must survive as
429 rather than as a generic error. The pair table is judged on the 429 hosts only, because a
host that read fine in both arms says nothing about the throttle.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.tools._lane_parity_fixtures import boards, write_arm  # noqa: E402
from tools.measurements.discovery import request_trace  # noqa: E402


class _Response:
    status_code = 429


class _StatusError(Exception):
    response = _Response()


class _CodeError(Exception):
    code = 503


def test_page_trace_keeps_the_jobs_own_stage_and_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.source_discovery import directory_fetch

    trace = request_trace.RequestTrace(tmp_path / "trace.jsonl")
    seen: list[dict[str, object]] = []

    async def fake_async(client: object, job: dict, url: str, timeout: int) -> str:
        if url.endswith("/home"):
            raise _StatusError()
        return "ok"

    def fake_batch(timeout_s: int, jobs: list[dict], **kwargs: Any) -> list:
        async_fetch = kwargs["async_fetch"]
        sync_fetch = kwargs["sync_fetch"]
        homepage = jobs[0]

        async def run() -> None:
            with pytest.raises(_StatusError):
                await async_fetch(None, homepage, "https://www.example.com/home", timeout_s)
            await async_fetch(None, homepage, "https://www.example.com/jobs", timeout_s)

        asyncio.run(run())
        assert (
            sync_fetch(
                {"failureStage": "gamedevmap_recovery_fetch"},
                "https://recover.example/join-us",
                timeout_s,
            )
            == "ok"
        )
        return []

    monkeypatch.setattr(directory_fetch, "fetch_pages_batched", fake_batch)
    with request_trace.trace_page_fetches(trace):
        directory_fetch.fetch_pages_batched(
            6,
            [{"failureStage": "homepage_fetch"}],
            sync_fetch=lambda job, url, timeout: "ok",
            async_fetch=fake_async,
            total_concurrency=1,
            per_host_concurrency=1,
        )

    rows = trace.rows
    assert [(row["phase"], row["status"]) for row in rows] == [
        ("homepage_fetch", 429),
        ("homepage_fetch", 200),
        ("gamedevmap_recovery_fetch", 200),
    ]
    assert rows[0]["host"] == "www.example.com"
    assert rows[0]["error"] == "_StatusError"


def test_page_trace_uses_a_successful_sync_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.source_discovery import directory_fetch

    trace = request_trace.RequestTrace(tmp_path / "trace.jsonl")

    def fake_batch(timeout_s: int, jobs: list[dict], **kwargs: Any) -> list:
        sync_fetch = kwargs["sync_fetch"]
        assert sync_fetch(jobs[0], "https://www.example.com/careers", timeout_s) == "ok"
        return []

    monkeypatch.setattr(directory_fetch, "fetch_pages_batched", fake_batch)
    with request_trace.trace_page_fetches(trace):
        directory_fetch.fetch_pages_batched(
            6,
            [{"failureStage": "homepage_fetch"}],
            sync_fetch=lambda job, url, timeout: "ok",
            total_concurrency=1,
            per_host_concurrency=1,
        )

    assert trace.rows == [
        {
            "phase": "homepage_fetch",
            "host": "www.example.com",
            "url": "https://www.example.com/careers",
            "status": 200,
            "startMs": trace.rows[0]["startMs"],
            "endMs": trace.rows[0]["endMs"],
        }
    ]
    assert trace.rows[0]["endMs"] >= trace.rows[0]["startMs"]


def test_probe_trace_records_every_attempt_and_restores_the_hook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.source_discovery import probe_runtime

    trace = request_trace.RequestTrace(tmp_path / "trace.jsonl")
    calls = {"count": 0}

    def fake_fetch(client: object, url: str, timeout: int) -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            raise _StatusError()
        return "ok"

    monkeypatch.setattr(probe_runtime, "fetch_text_httpx", fake_fetch)
    with request_trace.trace_probe_fetches(trace):
        with pytest.raises(_StatusError):
            probe_runtime.fetch_text_httpx(None, "https://apply.workable.com/1tk", 6)
        assert probe_runtime.fetch_text_httpx(None, "https://apply.workable.com/1tk", 6) == "ok"

    assert probe_runtime.fetch_text_httpx is fake_fetch
    assert [(row["phase"], row["status"]) for row in trace.rows] == [
        ("probe_fetch", 429),
        ("probe_fetch", 200),
    ]
    assert trace.rows[0]["host"] == "apply.workable.com"
    assert trace.rows[0]["error"] == "_StatusError"


def test_summary_counts_phases_hosts_and_failures(tmp_path: Path) -> None:
    trace = request_trace.RequestTrace(tmp_path / "trace.jsonl")
    base = time.perf_counter()
    trace.record(
        phase="homepage_fetch", url="https://a.example", status=200, start_s=base, end_s=base + 0.1
    )
    trace.record(
        phase="homepage_fetch",
        url="https://a.example",
        status=429,
        start_s=base + 0.2,
        end_s=base + 0.3,
        error="HTTPStatusError",
    )
    trace.record(
        phase="probe_fetch",
        url="https://b.example/api",
        status=None,
        start_s=base + 0.4,
        end_s=base + 0.5,
        error="TimeoutError",
    )
    summary = trace.summary()

    assert summary["enabled"] is True
    assert summary["requests"] == 3
    assert summary["hosts"] == 2
    assert summary["byPhase"] == {
        "homepage_fetch": {"requests": 2, "status429": 1, "failed": 1},
        "probe_fetch": {"requests": 1, "status429": 0, "failed": 1},
    }

    trace.write()
    lines = (tmp_path / "trace.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert json.loads(lines[0])["status"] == 200


def test_pattern_tells_an_episode_from_a_spread() -> None:
    def stat(*times: int, span: int = 100) -> dict:
        rows = [
            {
                "phase": "homepage_fetch",
                "host": "a",
                "status": 200,
                "startMs": 0,
                "endMs": 1,
            },
            {
                "phase": "homepage_fetch",
                "host": "a",
                "status": 200,
                "startMs": span,
                "endMs": span + 1,
            },
        ]
        for start in times:
            rows.append(
                {
                    "phase": "homepage_fetch",
                    "host": "a",
                    "status": 429,
                    "startMs": start,
                    "endMs": start + 1,
                }
            )
        return request_trace.host_stats(rows)[("homepage_fetch", "a")]

    assert request_trace.pattern_for(stat(10)) == "single"
    assert request_trace.pattern_for(stat(10, 20)) == "episode"
    assert request_trace.pattern_for(stat(10, 80)) == "spread"
    assert request_trace.pattern_for(stat(10, 50)) == "mixed"
    assert request_trace.pattern_for(stat()) == "-"


def test_trace_report_names_only_429_hosts_in_phase_order(tmp_path: Path) -> None:
    study = tmp_path / "study"
    write_arm(study, "p1-32", 32, boards=boards("A"))
    write_arm(study, "p1-64", 64, boards=boards("A"))
    low_rows = [
        {"phase": "probe_fetch", "host": "quiet.example", "status": 200, "startMs": 0, "endMs": 1},
        {"phase": "homepage_fetch", "host": "a.example", "status": 200, "startMs": 0, "endMs": 1},
        {"phase": "homepage_fetch", "host": "b.example", "status": 429, "startMs": 10, "endMs": 11},
        {"phase": "homepage_fetch", "host": "b.example", "status": 429, "startMs": 80, "endMs": 81},
    ]
    high_rows = [
        {"phase": "homepage_fetch", "host": "a.example", "status": 429, "startMs": 0, "endMs": 1},
        {"phase": "homepage_fetch", "host": "a.example", "status": 429, "startMs": 2, "endMs": 3},
        {
            "phase": "homepage_fetch",
            "host": "a.example",
            "status": 200,
            "startMs": 100,
            "endMs": 101,
        },
        {"phase": "homepage_fetch", "host": "c.example", "status": 429, "startMs": 5, "endMs": 6},
    ]
    for name, rows in (("p1-32", low_rows), ("p1-64", high_rows)):
        arm = study / "arms" / name
        (arm / "request-trace.jsonl").write_text(
            "\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8"
        )
        record_path = arm / "arm.json"
        record = json.loads(record_path.read_text(encoding="utf-8"))
        record["requestTrace"] = {
            "enabled": True,
            "file": "request-trace.jsonl",
            "requests": len(rows),
        }
        record_path.write_text(json.dumps(record), encoding="utf-8")

    report = request_trace.build_trace_report(study, ["p1:p1-32,p1-64"])

    assert [phase["phase"] for phase in report["pairs"][0]["phases"]] == [
        "homepage_fetch",
        "probe_fetch",
    ]
    assert report["pairs"][0]["phases"][1]["hosts"] == []
    phase = report["pairs"][0]["phases"][0]
    assert {key: phase["low"][key] for key in ("requests", "status429", "failed")} == {
        "requests": 3,
        "status429": 2,
        "failed": 2,
    }
    assert {key: phase["high"][key] for key in ("requests", "status429", "failed")} == {
        "requests": 4,
        "status429": 3,
        "failed": 3,
    }
    assert phase["low"]["bins"] == [0, 1, 0, 0, 0, 0, 0, 0, 0, 1]
    assert phase["low"]["hostsRequested"] == 2
    assert phase["low"]["hostsFully429"] == 1
    assert phase["low"]["hostsPartially429"] == 0
    assert phase["low"]["pattern"] == "spread"
    assert phase["high"]["hostsFully429"] == 1
    assert phase["high"]["hostsPartially429"] == 1
    assert [row["host"] for row in phase["hosts"]] == ["a.example", "b.example"]
    assert phase["omittedSingleRequestHosts"] == 1
    by_host = {row["host"]: row for row in phase["hosts"]}
    assert by_host["a.example"]["low"]["status429"] == 0
    assert by_host["a.example"]["high"]["pattern"] == "episode"
    assert by_host["b.example"]["low"]["pattern"] == "spread"
    table = request_trace.render_trace(report)
    assert "pair p1: p1-32 vs p1-64" in table
    assert "a.example" in table and "quiet.example" not in table
    assert "c.example" not in table
    assert "2 vs 2 requested, 1 vs 1 fully throttled, 0 vs 1 partial" in table
    assert "429 window:" in table


def test_an_untraced_arm_is_refused_by_name(tmp_path: Path) -> None:
    study = tmp_path / "study"
    write_arm(study, "p1-32", 32, boards=[])

    with pytest.raises(SystemExit, match="no request trace"):
        request_trace.load_trace(study, "p1-32")


def test_a_disabled_trace_is_recorded_as_disabled(tmp_path: Path) -> None:
    study = tmp_path / "study"
    write_arm(study, "p1-32", 32, boards=[])
    record = json.loads((study / "arms" / "p1-32" / "arm.json").read_text(encoding="utf-8"))
    record["requestTrace"] = {"enabled": False}
    (study / "arms" / "p1-32" / "arm.json").write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(SystemExit, match="no request trace"):
        request_trace.load_trace(study, "p1-32")


def test_status_from_error_reads_the_shapes_the_fetchers_raise() -> None:
    assert request_trace._status_from_error(_StatusError()) == 429
    assert request_trace._status_from_error(_CodeError()) == 503
    assert (
        request_trace._status_from_error(Exception("Client error '429 Too Many Requests'")) == 429
    )
    assert request_trace._status_from_error(TimeoutError()) is None
