"""Workday CXS pagination must follow the board's own reported `total`.

The collector looped `range(0, limit * 5, limit)` — five pages of 20, a hard 100-posting
ceiling regardless of board size. Measured on 2026-10-04 against the live API:

    board       claimed total   collected before   collected now
    NVIDIA              2000               100               2000
    Intel                602               100                602
    Aristocrat           209               100                209

NVIDIA alone is 1,900 openings the loop never requested, and it is the largest single block
in the coverage catalogue. The ceiling is now a bound on a pathological `total`, not a page
budget.

These tests assert the invariant against a scripted endpoint. A live number is evidence, not
an oracle: the tests must not fail because a board changed size.
"""

from __future__ import annotations

from typing import Any

from src.jobs.adapters import provider_structured_listing as runner

_LISTING = "https://example.wd5.myworkdayjobs.com/Example/Site"


def _postings(start: int, count: int, *, total: int) -> dict[str, Any]:
    return {
        "total": total,
        "jobPostings": [
            {
                "externalPath": f"/job/Example/Site/Role-{index}",
                "title": f"Role {index}",
                "locationsText": ["Remote"],
            }
            for index in range(start, start + count)
        ],
    }


def _scripted(monkeypatch: Any, pages: list[dict[str, Any]]) -> dict[str, int]:
    """Serve `pages` in order; count requests and record the offsets asked for."""
    seen: dict[str, int] = {"requests": 0}
    offsets: list[int] = []

    def fake_fetch(**kwargs: Any) -> dict[str, Any]:
        index = seen["requests"]
        seen["requests"] += 1
        offsets.append(int(kwargs["payload"].get("offset") or 0))
        if index >= len(pages):
            return {"total": 0, "jobPostings": []}
        return pages[index]

    seen["offsets"] = offsets  # type: ignore[assignment]
    monkeypatch.setattr(runner, "_fetch_workday_cxs_page", fake_fetch)
    return seen


def _collect() -> list[dict[str, Any]]:
    return runner._collect_workday_api_rows(
        listing_url=_LISTING, studio="Example", timeout_s=5, retries=0, backoff_s=0.0
    )


def test_pagination_follows_the_reported_total(monkeypatch: Any) -> None:
    """The defect: five pages of 20 against a board that reports 2,000."""
    _scripted(
        monkeypatch,
        [_postings(start, 20, total=2000) for start in range(0, 2000, 20)],
    )

    rows = _collect()

    assert len(rows) == 2000, "every reported posting must be requested, not the first 100"


def test_pagination_requests_pages_up_to_the_total(monkeypatch: Any) -> None:
    """100 offsets for a 2,000-posting board, not 5."""
    seen = _scripted(
        monkeypatch, [_postings(start, 20, total=2000) for start in range(0, 2000, 20)]
    )

    _collect()

    assert seen["offsets"] == list(range(0, 2000, 20))


def test_offsets_never_run_past_the_reported_total(monkeypatch: Any) -> None:
    """A total of 45 must not cost three full pages of requests."""
    seen = _scripted(monkeypatch, [_postings(0, 20, total=45), _postings(20, 20, total=45)])

    rows = _collect()

    assert len(rows) == 40
    assert seen["offsets"] == [0, 20, 40], "the third page is past total and must not be asked for"


def test_a_short_page_ends_pagination_whatever_total_says(monkeypatch: Any) -> None:
    """Workday over-reports `total` on filtered boards; a short page is authoritative."""
    seen = _scripted(monkeypatch, [_postings(0, 20, total=9000), _postings(20, 7, total=9000)])

    rows = _collect()

    assert len(rows) == 27
    assert seen["requests"] == 2, "a short page means the board is exhausted"


def test_repeated_postings_across_pages_are_deduplicated(monkeypatch: Any) -> None:
    """Workday repeats a posting when facets shift under the offset.

    Deduplicating by position rather than by the posting's own external path turns a
    repeating page into duplicate rows in the output.
    """
    _scripted(
        monkeypatch,
        [
            _postings(0, 20, total=60),
            _postings(20, 20, total=60),
            _postings(0, 20, total=60),
        ],
    )

    rows = _collect()

    identities = [row.get("sourceJobId") or row.get("url") for row in rows]
    assert len(identities) == len(set(identities)), "no row may repeat"
    assert len(rows) == 40


def test_a_page_repeating_only_known_postings_stops_the_loop(monkeypatch: Any) -> None:
    """A page that yields nothing new is the end, not a reason to keep requesting."""
    seen = _scripted(monkeypatch, [_postings(0, 20, total=900), _postings(0, 20, total=900)])

    rows = _collect()

    assert len(rows) == 20
    assert seen["requests"] == 2, "must not loop on a page that repeats the first one"


def test_a_board_smaller_than_one_page_costs_one_request(monkeypatch: Any) -> None:
    seen = _scripted(monkeypatch, [_postings(0, 6, total=6)])

    rows = _collect()

    assert len(rows) == 6
    assert seen["requests"] == 1


def test_an_empty_board_costs_one_request(monkeypatch: Any) -> None:
    seen = _scripted(monkeypatch, [{"total": 0, "jobPostings": []}])

    assert _collect() == []
    assert seen["requests"] == 1


def test_identical_pages_stop_on_the_first_repeat(monkeypatch: Any) -> None:
    seen = _scripted(monkeypatch, [{"jobPostings": _postings(0, 20, total=0)["jobPostings"]}] * 400)

    rows = _collect()

    assert len(rows) == 20
    assert seen["requests"] == 2, "must not loop on a page that repeats the first one"


def test_an_endless_board_of_fresh_postings_stops_at_the_ceiling(monkeypatch: Any) -> None:
    """A board that never shortens, never repeats, and never reports `total` must still stop.

    Every page is genuinely new, so the repeat guard cannot fire. Only
    `_WORKDAY_MAX_POSTINGS` bounds this loop, so the ceiling is asserted rather than assumed.
    """
    endless = [
        {"jobPostings": _postings(page * 20, 20, total=0)["jobPostings"]} for page in range(400)
    ]
    seen = _scripted(monkeypatch, endless)

    rows = _collect()

    assert len(rows) == runner._WORKDAY_MAX_POSTINGS
    assert seen["requests"] == runner._WORKDAY_MAX_POSTINGS // runner._WORKDAY_PAGE_LIMIT
