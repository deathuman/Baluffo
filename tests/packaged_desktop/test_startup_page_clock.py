"""Page-clock startup evidence: row promotion, page spans, readiness attempts.

The startup profile can only order page events by a clock the page supplies. Before
this, a page row carried the bridge's arrival time (delayed by queued metric POSTs)
and a per-emit wall clock that some rows never had, so page phases were placed at
the wrong point on the launch timeline and a stage whose end preceded its start was
clamped to zero. These tests pin the three pieces that make the evidence honest:
``pageTsMs`` promotion on the row, the additive ``pageStages`` block, and a trace
row for every readiness probe attempt.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.ship.startup_profile import (
    _page_bottleneck,
    _page_span_stages,
    render_startup_summary,
    summarize_startup_metrics,
)
from src.ship.startup_telemetry import build_startup_metric_row, wait_for_url
from tests.helpers.local_http_server import local_http_server

BASE = datetime(2026, 10, 20, 9, 0, 0, tzinfo=UTC)
BASE_MS = int(BASE.timestamp() * 1000)


def _ts(offset_ms: int) -> str:
    return (BASE + timedelta(milliseconds=offset_ms)).isoformat()


def _page_row(event: str, page_offset_ms: int, *, arrival_ms: int = 3000) -> dict[str, object]:
    return {
        "event": event,
        "ts": _ts(arrival_ms),
        "payload": {"pageTsMs": BASE_MS + page_offset_ms, "elapsedMs": page_offset_ms},
    }


def _launch_rows() -> list[dict[str, object]]:
    return [
        {"event": "desktop_launch_start", "ts": _ts(0)},
        {"event": "desktop_site_ready", "ts": _ts(550)},
        {"event": "desktop_bridge_ready_before_window", "ts": _ts(608)},
        {"event": "desktop_window_created", "ts": _ts(1111)},
        {"event": "desktop_shell_window_shown", "ts": _ts(1114)},
    ]


def test_page_ts_promotes_to_browser_ts_and_beats_created_at() -> None:
    row = build_startup_metric_row(
        "jobs_first_render",
        {
            "pageTsMs": BASE_MS + 1234,
            "browserCreatedAtMs": BASE_MS,
            "elapsedMs": 5,
        },
        ts=_ts(3000),
        value_container="payload",
    )

    assert row["browserTsMs"] == BASE_MS + 1234
    payload = row["payload"]
    assert isinstance(payload, dict)
    assert payload["pageTsMs"] == BASE_MS + 1234


def test_browser_created_at_stays_the_fallback_for_older_rows() -> None:
    row = build_startup_metric_row(
        "jobs_first_render",
        {"browserCreatedAtMs": BASE_MS + 500},
        ts=_ts(3000),
        value_container="payload",
    )

    assert row["browserTsMs"] == BASE_MS + 500


def test_page_stages_expose_the_report_probe_on_the_page_clock() -> None:
    rows = [
        *_launch_rows(),
        _page_row("jobs_page_boot_start", 1400),
        _page_row("jobs_local_data_init_ready", 1450),
        _page_row("jobs_report_probe_start", 1460),
        _page_row("jobs_report_probe_done", 2350),
        _page_row("jobs_first_run_gate_evaluated", 2360),
        _page_row("jobs_first_render", 2370),
        _page_row("jobs_first_interactive", 2370),
        _page_row("jobs_auth_ready", 2600),
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")
    spans = {row["key"]: row for row in summary["pageStages"]}

    assert spans["report_probe"]["durationMs"] == 890
    assert spans["report_probe"]["status"] == "measured"
    assert spans["local_data_ready_to_first_render"]["durationMs"] == 920
    assert spans["page_boot_to_first_interactive"]["durationMs"] == 970
    assert spans["local_data_ready_to_auth_ready"]["durationMs"] == 1150
    assert not any(row["negativeGap"] for row in summary["pageStages"])
    # The launch stage table is unchanged: page spans are additive evidence.
    assert [row["key"] for row in summary["stages"]][0] == "launch_to_site_ready"


def test_page_span_flags_an_end_before_its_start_instead_of_clamping() -> None:
    rows = [
        *_launch_rows(),
        _page_row("jobs_report_probe_done", 2350),
        _page_row("jobs_first_run_gate_evaluated", 2300),
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")
    spans = {row["key"]: row for row in summary["pageStages"]}

    assert spans["report_probe_to_first_run_gate"]["durationMs"] == -50
    assert spans["report_probe_to_first_run_gate"]["negativeGap"] is True


def _cold_jobs_rows(
    *, report_probe_ms: int, auth_ms: int, with_probe: bool = True
) -> list[dict[str, object]]:
    """A cold launch whose report probe runs inside the local-data-to-render span.

    Mirrors the shipped shape: the probe is almost the whole pre-render window, and the
    launch table can only see the auth stage that neighbours it. Dropping the probe rows
    leaves the same launch timeline without the page-clock evidence.
    """
    # The probe opens 10 ms before local data ready, as it does on the shipped page.
    probe_done = 1440 + report_probe_ms
    first_render = probe_done + 15
    rows: list[dict[str, object]] = [
        {"event": "desktop_launch_start", "ts": _ts(0)},
        {"event": "desktop_site_ready", "ts": _ts(550)},
        {"event": "desktop_window_created", "ts": _ts(1111)},
        {"event": "desktop_shell_window_shown", "ts": _ts(1114)},
        {"event": "desktop_page_loaded", "ts": _ts(1400)},
        _page_row("jobs_local_data_api_ready", 1420),
        _page_row("jobs_local_data_init_ready", 1450),
        _page_row("jobs_first_run_gate_evaluated", probe_done + 5),
        _page_row("jobs_auth_ready", 1450 + auth_ms),
        _page_row("jobs_first_render", first_render),
        _page_row("jobs_first_interactive", first_render + 5),
    ]
    if with_probe:
        rows.extend(
            [
                _page_row("jobs_report_probe_start", 1440),
                _page_row("jobs_report_probe_done", probe_done),
            ]
        )
    return rows


def test_page_span_reattributes_a_launch_stage_bottleneck() -> None:
    """The auth stage is the longest launch stage; the probe covers all of it."""
    without_probe = summarize_startup_metrics(
        _cold_jobs_rows(report_probe_ms=3800, auth_ms=3200, with_probe=False),
        page="jobs",
        profile_mode="cold",
    )
    summary = summarize_startup_metrics(
        _cold_jobs_rows(report_probe_ms=3800, auth_ms=3200), page="jobs", profile_mode="cold"
    )
    spans = {row["key"]: row for row in summary["pageStages"]}

    # The launch table can only name the neighbouring stage on its own clock...
    assert without_probe["classification"] == "local auth bootstrap delayed"
    assert summary["stages"] == without_probe["stages"]

    # ...but a 3800 ms probe spans the whole 3200 ms auth stage, and the envelope around
    # both (3805 ms) is not the answer: the tightest covering span names the work.
    assert spans["report_probe"]["startMs"] <= spans["local_data_ready_to_auth_ready"]["startMs"]
    assert spans["local_data_ready_to_first_render"]["durationMs"] == 3805
    assert summary["classification"] == "jobs report probe delayed"
    assert summary["bottleneck"] == {
        "source": "page",
        "key": "report_probe",
        "label": "jobs report probe delayed",
        "startEvent": "jobs_report_probe_start",
        "endEvent": "jobs_report_probe_done",
        "durationMs": 3800,
        "covers": "local_data_ready_to_auth_ready",
    }
    assert (
        "Bottleneck: jobs report probe delayed "
        "[page span report_probe 3800ms covering local_data_ready_to_auth_ready]"
    ) in render_startup_summary(summary)


def test_a_page_span_that_does_not_cover_the_blamed_stage_leaves_it_alone() -> None:
    summary = summarize_startup_metrics(
        _cold_jobs_rows(report_probe_ms=200, auth_ms=3200), page="jobs", profile_mode="cold"
    )

    assert summary["classification"] == "local auth bootstrap delayed"
    assert summary["bottleneck"]["source"] == "launch"
    assert summary["bottleneck"]["key"] == "local_data_ready_to_auth_ready"
    assert "page span" not in render_startup_summary(summary)


def test_page_bottleneck_ignores_a_span_whose_end_precedes_its_start() -> None:
    spans = [
        {
            "key": "report_probe",
            "startEvent": "jobs_report_probe_start",
            "endEvent": "jobs_report_probe_done",
            "startMs": 3000,
            "endMs": 1000,
            "durationMs": -2000,
            "negativeGap": True,
        }
    ]

    assert (
        _page_bottleneck(
            spans,
            page="jobs",
            launch_top={
                "key": "local_data_ready_to_auth_ready",
                "startMs": 1100,
                "endMs": 1700,
                "durationMs": 600,
            },
            launch_classification="local auth bootstrap delayed",
        )
        is None
    )


def test_page_bottleneck_keeps_an_ok_or_incomplete_verdict() -> None:
    spans = _page_span_stages(
        {
            "jobs_local_data_init_ready": 0,
            "jobs_report_probe_start": 0,
            "jobs_report_probe_done": 4000,
        },
        "jobs",
    )
    launch_top = {
        "key": "local_data_ready_to_auth_ready",
        "startMs": 100,
        "endMs": 200,
        "durationMs": 100,
    }

    for verdict in ("ok", "metrics incomplete", "insufficient data"):
        assert (
            _page_bottleneck(
                spans,
                page="jobs",
                launch_top=launch_top,
                launch_classification=verdict,
            )
            is None
        )


def _admin_rows() -> list[dict[str, object]]:
    return [
        {"event": "desktop_launch_start", "ts": _ts(0)},
        {"event": "desktop_site_ready", "ts": _ts(550)},
        {"event": "desktop_window_created", "ts": _ts(1111)},
        {"event": "desktop_shell_window_shown", "ts": _ts(1114)},
        {"event": "desktop_page_loaded", "ts": _ts(1400)},
        _page_row("admin_page_boot_start", 1400),
        _page_row("admin_local_data_init_ready", 1450),
        _page_row("admin_ready", 2050),
        _page_row("admin_first_interactive", 2300),
    ]


def test_admin_summary_carries_page_spans_and_attribution() -> None:
    summary = summarize_startup_metrics(_admin_rows(), page="admin", profile_mode="cold")
    spans = {row["key"]: row for row in summary["pageStages"]}

    assert spans["page_boot_to_local_data_ready"]["durationMs"] == 50
    assert spans["page_boot_to_first_interactive"]["durationMs"] == 900
    # The page total covers every later stage, so it must never become the classification;
    # with nothing narrower bracketing a blamed stage the launch verdict stands.
    assert summary["bottleneck"]["source"] == "launch"
    assert summary["bottleneck"]["label"] == summary["classification"]


def test_probe_page_summary_carries_the_same_fields() -> None:
    rows: list[dict[str, Any]] = [
        {"event": "desktop_launch_start", "ts": _ts(0)},
        {"event": "desktop_site_ready", "ts": _ts(550)},
        {"event": "desktop_window_created", "ts": _ts(1111)},
        {"event": "desktop_shell_window_shown", "ts": _ts(1114)},
        {
            "event": "desktop_probe_html_parse_start",
            "ts": _ts(1200),
            "payload": {"elapsedMs": 1200},
        },
        {"event": "desktop_probe_ready", "ts": _ts(1500), "payload": {"elapsedMs": 1500}},
    ]

    summary = summarize_startup_metrics(rows, page="desktop-probe", profile_mode="cold")

    # A probe page emits no page-boot/local-data events, so its spans are honest empties
    # rather than missing fields.
    assert [row["status"] for row in summary["pageStages"]] == ["unavailable"] * 6
    assert summary["bottleneck"]["source"] == "launch"
    assert summary["classification"] == summary["bottleneck"]["label"]


def test_wait_for_url_traces_every_attempt_not_just_the_first(tmp_path: Path) -> None:
    answers = {"count": 0}

    def flaky() -> tuple[int, dict[str, str], bytes]:
        answers["count"] += 1
        if answers["count"] < 3:
            return (503, {"Content-Type": "text/plain"}, b"busy")
        return (200, {"Content-Type": "text/plain"}, b"ok")

    with local_http_server({"/flaky": flaky}) as base_url:
        wait_for_url(
            f"{base_url}/flaky",
            timeout_s=5.0,
            interval_s=0.01,
            trace_data_dir=tmp_path,
        )

    journal = (tmp_path / "desktop-startup-metrics.jsonl").read_text(encoding="utf-8")
    rows = [json.loads(line) for line in journal.splitlines() if line.strip()]
    attempts = [row for row in rows if row["event"] == "desktop_url_probe_attempt"]

    assert [row["fields"]["attempt"] for row in attempts] == [1, 2, 3]
    assert [row["fields"]["status"] for row in attempts] == [503, 503, 200]
    assert [row["fields"]["ok"] for row in attempts] == [False, False, True]
    assert all(isinstance(row["fields"]["sinceStartMs"], int) for row in attempts)
    assert attempts[-1]["fields"]["sinceStartMs"] >= attempts[0]["fields"]["sinceStartMs"]
