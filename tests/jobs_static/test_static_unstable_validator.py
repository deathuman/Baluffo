"""A board whose validator never survives its own read stops being asked.

The loopback server in ``test_static_read_validator_seed.py`` can be told to republish a fresh
ETag on every response, which is the live shape of 22 of the 84 boards that answered 200 without
echoing: the conditional HEAD learns a tag the read never sees again. These tests drive the real
runner through that cycle and hold the retirement to two consecutive windows.

Split from the seed module to stay inside the test line budget.
"""

from __future__ import annotations

from unittest import mock

from tests.jobs_static.test_static_read_validator_seed import (
    ETAG,
    _apply,
    _expire_listing_window,
    _listing_server,
    _parsed_row,
    _run,
    _stored_row,
)


def test_a_board_that_republishes_its_validator_is_read_without_a_probe() -> None:
    """A third tag between the HEAD and the GET retires the HEAD, not the read."""
    name = "Churning Validator Studio"
    with _listing_server(churning_etags=True) as (base, requests):
        listing_url = f"{base}/careers"
        rows = {name: _stored_row(listing_url, kept=12)}
        rows[name]["lastHttpEtag"] = ETAG
        _expire_listing_window(rows, name)

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            first = _run(name, listing_url, rows)

        assert requests == [("HEAD", "/careers", ETAG), ("GET", "/careers", "")]
        assert first["httpProbeEtag"] and first["httpProbeEtag"] != ETAG
        assert first["httpProbeUnstableAt"], "the read published a tag the probe did not"
        rows = _apply(rows, first)
        assert rows[name]["lastHttpProbeUnstableAt"] == first["httpProbeUnstableAt"]
        assert "lastHttpProbeIgnoredAt" not in rows[name], (
            "one window can be a single publish between the two requests"
        )

        _expire_listing_window(rows, name)
        requests.clear()
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            second = _run(name, listing_url, rows)

        assert len(requests) == 2 and requests[0][0] == "HEAD", (
            "an unconfirmed churn still asks, because the next window is the confirmation"
        )
        assert second["httpProbeUnstableAt"]
        rows = _apply(rows, second)
        assert rows[name]["lastHttpProbeIgnoredAt"] == second["httpProbeUnstableAt"], (
            "the second consecutive window is the proof"
        )

        _expire_listing_window(rows, name)
        requests.clear()
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            third = _run(name, listing_url, rows)

        assert requests == [("GET", "/careers", "")], (
            "a retired probe is skipped: the read happens without a HEAD in front of it"
        )
        assert third["status"] == "ok"
