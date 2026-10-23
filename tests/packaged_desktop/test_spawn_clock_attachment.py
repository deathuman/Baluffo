"""The probe report carries the spawn clock, so the attachment edge is pinned here.

Two properties matter: a launch whose spawn time is known gets the marks on its
``startupProfile`` (which is what ``startup-profile-summary.json`` serializes),
and a caller that has no spawn time gets no block at all rather than a block of
nulls that looks like a measured zero.
"""

from __future__ import annotations

from datetime import UTC, datetime

from src.ship.packaged_smoke.orchestrator import _attach_spawn_clock

SPAWN = "2026-10-22T12:00:00.000+00:00"
SPAWN_MS = datetime.fromisoformat(SPAWN).timestamp() * 1000.0


def _at(seconds: float) -> str:
    return datetime.fromtimestamp((SPAWN_MS / 1000.0) + seconds, tz=UTC).isoformat()


def _rows() -> list[dict[str, object]]:
    page_ms = SPAWN_MS + 1348
    return [
        {"event": "desktop_launch_start", "ts": _at(0.120)},
        {"event": "desktop_shell_window_shown", "ts": _at(1.424)},
        {
            "event": "jobs_first_interactive",
            "ts": _at(1.540),
            "browserTsMs": page_ms,
            "payload": {"pageTsMs": page_ms},
        },
    ]


def test_attachment_records_spawn_marks_for_a_known_spawn() -> None:
    startup_profile: dict[str, object] = {}

    _attach_spawn_clock(startup_profile, _rows(), spawn_epoch_ms=SPAWN_MS, page="jobs")

    clock = startup_profile["spawnClock"]
    assert isinstance(clock, dict)
    assert clock["bootMs"] == 120
    assert clock["windowShownMs"] == 1424
    assert clock["firstInteractiveMs"] == 1348
    assert clock["firstInteractiveClock"] == "page"
    assert clock["firstInteractiveArrivalMs"] == 1540
    assert clock["complete"] is True


def test_attachment_skips_a_launch_with_no_spawn_time() -> None:
    startup_profile: dict[str, object] = {}

    _attach_spawn_clock(startup_profile, _rows(), spawn_epoch_ms=None, page="jobs")

    assert "spawnClock" not in startup_profile


def test_attachment_skips_a_launch_that_journaled_nothing() -> None:
    startup_profile: dict[str, object] = {}

    _attach_spawn_clock(startup_profile, [], spawn_epoch_ms=SPAWN_MS, page="jobs")

    assert "spawnClock" not in startup_profile
