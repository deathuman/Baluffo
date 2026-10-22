"""Spawn-relative startup marks for the packaged desktop.

The startup profile times its stages from ``desktop_launch_start``, the first row
the launcher journals, so it never bills the PyInstaller boot or the launcher's
own preamble. This module places the same rows against the moment the harness
spawned the EXE, which is the wait a double-click produces.

Two clocks are in play and they are not interchangeable:

* launcher rows are placed by ``ts``, the timestamp the launcher wrote;
* page rows carry ``browserTsMs`` (or ``payload.pageTsMs``), the page's own epoch
  clock. Their ``ts`` is the bridge *arrival* time, and a queued metric POST can
  delay it by hundreds of milliseconds, so it is reported beside the page value
  instead of replacing it.

The page clock is a cross-process clock. It can place a page event tens of
milliseconds before the launcher's ``desktop_shell_window_shown`` row, because
the launcher records the shell only after it detects the window; readers should
compare the marks rather than expect the two clocks to agree exactly.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

LAUNCH_EVENTS: tuple[str, ...] = (
    "desktop_launch_start",
    "desktop_site_ready",
    "desktop_bridge_ready",
    "desktop_window_created",
    "desktop_shell_window_shown",
    "desktop_page_loaded",
)

PAGE_INTERACTIVE_EVENTS: dict[str, tuple[str, ...]] = {
    "jobs": ("jobs_first_interactive", "jobs_first_render"),
    "admin": ("admin_first_interactive", "admin_ready"),
}


def parse_epoch_ms(value: Any) -> float | None:
    """Epoch milliseconds for an ISO-8601 row timestamp, or None when unusable."""
    token = str(value or "").strip()
    if not token:
        return None
    try:
        parsed = datetime.fromisoformat(token.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp() * 1000.0


def _positive_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if number > 0.0 else None


def page_clock_ms(row: Mapping[str, object]) -> float | None:
    """The page's own epoch timestamp, when the row carries one."""
    direct = _positive_number(row.get("browserTsMs"))
    if direct is not None:
        return direct
    payload = row.get("payload")
    if isinstance(payload, Mapping):
        return _positive_number(payload.get("pageTsMs"))
    return None


def _first_rows(rows: Sequence[Mapping[str, object]]) -> dict[str, Mapping[str, object]]:
    events: dict[str, Mapping[str, object]] = {}
    for row in rows:
        event = str(row.get("event") or "").strip()
        if event and event not in events:
            events[event] = row
    return events


def _page_event(events: Mapping[str, Mapping[str, object]], page: str) -> str:
    for candidate in PAGE_INTERACTIVE_EVENTS.get(page, PAGE_INTERACTIVE_EVENTS["jobs"]):
        if candidate in events:
            return candidate
    return ""


def _launcher_mark(
    events: Mapping[str, Mapping[str, object]], event: str, spawn_epoch_ms: float
) -> dict[str, Any] | None:
    row = events.get(event)
    epoch = parse_epoch_ms(row.get("ts")) if row is not None else None
    if epoch is None:
        return None
    return {"ms": int(round(epoch - spawn_epoch_ms)), "clock": "launcher"}


def build_spawn_clock(
    rows: Sequence[Mapping[str, object]], *, spawn_epoch_ms: float, page: str
) -> dict[str, Any]:
    """Spawn-relative marks for one launch, with the clock each mark used."""
    events = _first_rows(rows)
    marks: dict[str, dict[str, Any]] = {}
    for event in LAUNCH_EVENTS:
        mark = _launcher_mark(events, event, spawn_epoch_ms)
        if mark is not None:
            marks[event] = mark
    page_event = _page_event(events, page)
    page_row = events.get(page_event) if page_event else None
    page_mark: dict[str, Any] | None = None
    if page_row is not None:
        arrival = parse_epoch_ms(page_row.get("ts"))
        page_clock = page_clock_ms(page_row)
        chosen = page_clock if page_clock is not None else arrival
        if chosen is not None:
            page_mark = {
                "ms": int(round(chosen - spawn_epoch_ms)),
                "clock": "page" if page_clock is not None else "arrival",
            }
            if page_clock is not None and arrival is not None:
                page_mark["arrivalMs"] = int(round(arrival - spawn_epoch_ms))
            marks[page_event] = page_mark
    boot_ms = (marks.get("desktop_launch_start") or {}).get("ms")
    window_shown_ms = (marks.get("desktop_shell_window_shown") or {}).get("ms")
    first_interactive_ms = (page_mark or {}).get("ms")
    return {
        "page": page,
        "pageEvent": page_event,
        "marks": marks,
        "bootMs": boot_ms,
        "siteReadyMs": (marks.get("desktop_site_ready") or {}).get("ms"),
        "bridgeReadyMs": (marks.get("desktop_bridge_ready") or {}).get("ms"),
        "windowCreatedMs": (marks.get("desktop_window_created") or {}).get("ms"),
        "windowShownMs": window_shown_ms,
        "pageLoadedMs": (marks.get("desktop_page_loaded") or {}).get("ms"),
        "firstInteractiveMs": first_interactive_ms,
        "firstInteractiveClock": (page_mark or {}).get("clock", ""),
        "firstInteractiveArrivalMs": (page_mark or {}).get("arrivalMs"),
        "complete": bool(
            boot_ms is not None and window_shown_ms is not None and first_interactive_ms is not None
        ),
    }
