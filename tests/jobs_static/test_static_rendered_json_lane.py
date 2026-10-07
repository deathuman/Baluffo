"""Lane 2.6 end to end: a board whose only openings live in an XHR.

The measured case is Feishu: eight tenant boards, 826 recorded openings, a GET that answers
200 with 141 KB and zero anchors, and rows that exist only in the response the page's own app
fetches. Registering those boards as static rows would have been a false green -- the delivery
metric counts a registry row as landed while it produces nothing -- so the lane has to make the
rows real first.

Order matters as much as the extraction. The lane runs last, after anchors, embedded JSON and
rendered cards have all found nothing, because it is the only one that costs a settling browser
render. And it runs *only* when the cheaper lanes are exhausted, so a board that works without a
browser never pays for one.
"""

from __future__ import annotations

from src.exceptions import AdapterValidationError

from ._helpers import jf

_CAPTURED = """
<html><body><div id="app"></div>
<script>
  fetch("/api/v1/search/job_post/count", {method: "POST"})
    .then(r => r.json()).then(d => window.render(d));
</script>
</body></html>
"""

_LISTINGS_PAYLOAD = (
    "https://studio.example/api/v1/search/job_post/count",
    '{"total": 2, "job_post_list": ['
    '{"id": "1", "title": "Gameplay Engineer", "job_post_url": "https://studio.example/position/1/detail", "city": "Tokyo"},'
    '{"id": "2", "title": "Level Designer", "job_post_url": "https://studio.example/position/2/detail", "city": "Osaka"}]}',
)

_TELEMETRY_PAYLOAD = (
    "https://studio.example/analytics/collect",
    '{"events": [{"title": "x", "url": "/y"}]}',
)


def _sources(**extra):
    row = {
        "name": "Studio (Manual Website)",
        "studio": "Studio",
        "company": "Studio",
        "adapter": "static",
        "pages": ["https://studio.example/index"],
        "id": "static:listing_url:https://studio.example/index",
    }
    row.update(extra)
    return [row]


def test_rows_come_out_of_the_captured_payload() -> None:
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: _CAPTURED,
        try_playwright_json=lambda _url, _timeout: (_CAPTURED, "", [_LISTINGS_PAYLOAD]),
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=_sources(),
    )
    assert sorted(row["title"] for row in rows) == ["Gameplay Engineer", "Level Designer"]
    assert rows[0]["adapter"] == "static"
    assert rows[0]["studio"] == "Studio"


def _extracted_rows(**capture_kwargs):
    """Rows from this board, or ``[]`` when the source legitimately extracts nothing.

    A static source that finds no rows raises `AdapterValidationError` ("no jobs extracted")
    rather than returning an empty list -- the runtime's way of saying it asked and found
    nothing, which is a different answer from a source that was never asked. The lane tests
    care about the row set, so they ask for that and let the raise through when it happens.
    """
    try:
        return jf.run_static_studio_pages_source(
            fetch_text=lambda _url, _timeout: _CAPTURED,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            sources=_sources(),
            **capture_kwargs,
        )
    except AdapterValidationError:
        return []


def test_a_board_without_the_capture_seam_is_unchanged() -> None:
    """No capture callable must mean no capture lane, not a crash."""
    assert _extracted_rows() == []


def test_a_refused_capture_produces_no_rows() -> None:
    """The breaker refusing in cooldown returns no payloads; that is not an empty board."""
    assert (
        _extracted_rows(
            try_playwright_json=lambda _url, _timeout: (
                "",
                "browser fallback unavailable (cooldown active)",
                [],
            )
        )
        == []
    )


def test_a_capture_that_returns_no_payloads_produces_no_rows() -> None:
    assert _extracted_rows(try_playwright_json=lambda _url, _timeout: (_CAPTURED, "", [])) == []


def test_the_game_row_filter_still_runs_on_captured_rows() -> None:
    """A board's back-office roles are dropped here exactly as on every other path."""
    payload = (
        "https://studio.example/api/jobs",
        '{"jobs": ['
        '{"title": "Gameplay Engineer", "url": "https://studio.example/jobs/1"},'
        '{"title": "Senior Legal Counsel", "url": "https://studio.example/jobs/2"}]}',
    )
    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: _CAPTURED,
        try_playwright_json=lambda _url, _timeout: (_CAPTURED, "", [payload]),
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=_sources(),
    )
    assert [row["title"] for row in rows] == ["Gameplay Engineer"]


def test_telemetry_payloads_do_not_become_rows() -> None:
    assert (
        _extracted_rows(
            try_playwright_json=lambda _url, _timeout: (_CAPTURED, "", [_TELEMETRY_PAYLOAD])
        )
        == []
    )


def test_the_capture_is_not_attempted_when_anchors_already_found_rows() -> None:
    """The lane is last for a reason: a board that works without a browser must not pay for
    one."""
    attempts: list[str] = []
    listing = (
        '<html><body><a href="https://studio.example/jobs/1">Gameplay Engineer</a></body></html>'
    )

    def _capture(url, timeout):
        attempts.append(url)
        return ("", "", [])

    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: listing,
        try_playwright_json=_capture,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=_sources(),
    )
    assert rows, "the anchor lane found the row"
    assert attempts == [], "no browser render was needed"


def test_the_capture_is_not_attempted_when_embedded_json_already_found_rows() -> None:
    """Lane 2.5 outranks 2.6: both are JSON, and 2.5 costs no browser at all."""
    attempts: list[str] = []
    listing = (
        '<html><body><script id="__NEXT_DATA__" type="application/json">'
        '{"props": {"pageProps": {"jobs": ['
        '{"title": "Gameplay Engineer", "url": "https://studio.example/jobs/1"}'
        "]}}}</script></body></html>"
    )

    def _capture(url, timeout):
        attempts.append(url)
        return ("", "", [])

    rows = jf.run_static_studio_pages_source(
        fetch_text=lambda _url, _timeout: listing,
        try_playwright_json=_capture,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=_sources(),
    )
    assert [row["title"] for row in rows] == ["Gameplay Engineer"]
    assert attempts == [], "the embedded lane answered without a browser"
