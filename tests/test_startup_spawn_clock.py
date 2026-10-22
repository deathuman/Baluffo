"""Spawn-relative marks are the number the release carries, so their rules are pinned.

The load-bearing decisions are the clock choice per row kind (launcher ``ts``,
page ``browserTsMs``), keeping the bridge arrival beside the page value instead
of replacing it, and refusing to call a partial launch complete.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.ship.startup_spawn_clock import build_spawn_clock, page_clock_ms, parse_epoch_ms

SPAWN = "2026-10-22T12:00:00.000+00:00"
SPAWN_MS = datetime.fromisoformat(SPAWN).timestamp() * 1000.0


def _at(seconds: float) -> str:
    return datetime.fromtimestamp((SPAWN_MS / 1000.0) + seconds, tz=UTC).isoformat()


def _launcher(event: str, seconds: float) -> dict[str, object]:
    return {"event": event, "ts": _at(seconds)}


def _page(event: str, *, page_seconds: float, arrival_seconds: float) -> dict[str, object]:
    page_ms = SPAWN_MS + page_seconds * 1000.0
    return {
        "event": event,
        "ts": _at(arrival_seconds),
        "browserTsMs": page_ms,
        "payload": {"pageTsMs": page_ms},
    }


def _launch_rows(page_event: str = "jobs_first_interactive") -> list[dict[str, object]]:
    return [
        _launcher("desktop_launch_start", 0.100),
        _launcher("desktop_site_ready", 0.300),
        _launcher("desktop_bridge_ready", 0.900),
        _launcher("desktop_window_created", 0.950),
        _launcher("desktop_shell_window_shown", 1.000),
        _page(page_event, page_seconds=1.100, arrival_seconds=1.400),
    ]


def test_parse_epoch_ms_accepts_z_suffix_and_rejects_garbage() -> None:
    assert parse_epoch_ms(SPAWN) == SPAWN_MS
    assert parse_epoch_ms("2026-10-22T12:00:00Z") == SPAWN_MS
    assert parse_epoch_ms("") is None
    assert parse_epoch_ms("not-a-timestamp") is None


def test_page_clock_prefers_browser_timestamp_then_payload() -> None:
    assert page_clock_ms({"browserTsMs": 1234.0}) == 1234.0
    assert page_clock_ms({"payload": {"pageTsMs": 99}}) == 99.0
    assert page_clock_ms({"browserTsMs": None, "payload": {"pageTsMs": 99}}) == 99.0
    assert page_clock_ms({"ts": _at(1.0)}) is None


def test_launcher_rows_are_placed_by_ts() -> None:
    clock = build_spawn_clock(_launch_rows(), spawn_epoch_ms=SPAWN_MS, page="jobs")

    assert clock["bootMs"] == 100
    assert clock["siteReadyMs"] == 300
    assert clock["bridgeReadyMs"] == 900
    assert clock["windowCreatedMs"] == 950
    assert clock["windowShownMs"] == 1000
    assert clock["marks"]["desktop_launch_start"]["clock"] == "launcher"


def test_page_row_uses_its_page_clock_and_keeps_the_arrival_delta() -> None:
    clock = build_spawn_clock(_launch_rows(), spawn_epoch_ms=SPAWN_MS, page="jobs")

    assert clock["pageEvent"] == "jobs_first_interactive"
    assert clock["firstInteractiveMs"] == 1100
    assert clock["firstInteractiveClock"] == "page"
    # The POST queued for 300 ms; that delay must stay visible, not move the mark.
    assert clock["firstInteractiveArrivalMs"] == 1400
    assert clock["complete"] is True


def test_page_row_without_a_page_clock_falls_back_to_arrival() -> None:
    rows: list[dict[str, object]] = [
        _launcher("desktop_launch_start", 0.100),
        _launcher("desktop_shell_window_shown", 1.000),
        {"event": "jobs_first_interactive", "ts": _at(1.500), "payload": {}},
    ]

    clock = build_spawn_clock(rows, spawn_epoch_ms=SPAWN_MS, page="jobs")

    assert clock["firstInteractiveClock"] == "arrival"
    assert clock["firstInteractiveMs"] == 1500
    assert "arrivalMs" not in clock["marks"]["jobs_first_interactive"]


def test_jobs_falls_back_to_first_render_and_admin_to_ready() -> None:
    jobs: list[dict[str, object]] = [
        _page("jobs_first_render", page_seconds=0.800, arrival_seconds=0.900)
    ]
    admin: list[dict[str, object]] = [
        _page("admin_ready", page_seconds=0.700, arrival_seconds=1.000)
    ]

    jobs_clock = build_spawn_clock(jobs, spawn_epoch_ms=SPAWN_MS, page="jobs")
    admin_clock = build_spawn_clock(admin, spawn_epoch_ms=SPAWN_MS, page="admin")

    assert jobs_clock["pageEvent"] == "jobs_first_render"
    assert admin_clock["pageEvent"] == "admin_ready"
    assert admin_clock["firstInteractiveMs"] == 700


def test_first_row_per_event_wins_over_later_duplicates() -> None:
    rows = [
        _launcher("desktop_launch_start", 0.100),
        _launcher("desktop_launch_start", 9.000),
    ]

    clock = build_spawn_clock(rows, spawn_epoch_ms=SPAWN_MS, page="jobs")

    assert clock["bootMs"] == 100


def test_malformed_timestamp_drops_only_that_mark() -> None:
    rows: list[dict[str, object]] = [
        {"event": "desktop_launch_start", "ts": "not-a-timestamp"},
        _launcher("desktop_shell_window_shown", 1.000),
    ]

    clock = build_spawn_clock(rows, spawn_epoch_ms=SPAWN_MS, page="jobs")

    assert clock["bootMs"] is None
    assert clock["windowShownMs"] == 1000


def test_partial_launch_is_not_complete() -> None:
    rows = [
        _launcher("desktop_launch_start", 0.100),
        _launcher("desktop_shell_window_shown", 1.000),
    ]

    clock = build_spawn_clock(rows, spawn_epoch_ms=SPAWN_MS, page="jobs")

    assert clock["complete"] is False
    assert clock["firstInteractiveMs"] is None


def test_inferred_window_mark_is_reported_and_labelled() -> None:
    """A launcher that cannot enumerate windows still gets a window figure.

    The Linux launcher journals ``desktop_shell_window_shown_inferred`` when its
    reveal timeout expires. Reporting it as an observed paint would overstate the
    measurement, so the clock and an explicit ``observed`` flag travel with it.
    """
    rows = [
        _launcher("desktop_launch_start", 0.100),
        _launcher("desktop_shell_window_shown_inferred", 0.950),
        _page("jobs_first_interactive", page_seconds=1.100, arrival_seconds=1.250),
    ]

    clock = build_spawn_clock(rows, spawn_epoch_ms=SPAWN_MS, page="jobs")

    assert clock["windowShownMs"] == 950
    assert clock["windowShownClock"] == "launcher-inferred"
    assert clock["windowShownObserved"] is False
    assert clock["marks"]["desktop_shell_window_shown_inferred"]["clock"] == "launcher-inferred"
    assert clock["complete"] is True


def test_observed_window_mark_wins_over_an_inferred_one() -> None:
    rows = [
        _launcher("desktop_launch_start", 0.100),
        _launcher("desktop_shell_window_shown_inferred", 0.950),
        _launcher("desktop_shell_window_shown", 0.700),
        _page("jobs_first_interactive", page_seconds=1.100, arrival_seconds=1.250),
    ]

    clock = build_spawn_clock(rows, spawn_epoch_ms=SPAWN_MS, page="jobs")

    assert clock["windowShownMs"] == 700
    assert clock["windowShownClock"] == "launcher"
    assert clock["windowShownObserved"] is True
