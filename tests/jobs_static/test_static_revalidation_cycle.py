"""The whole 304 cycle, against a real socket.

The pieces were each right on their own: the flow answers a conditional HEAD with an
exclusion, the applier refreshes the success clock on a 304, and the decision reads a window
from that clock. Tested apart, they never show the loop closing -- an applier that spells the
reason differently, or a decision that keeps returning ``revalidate_only``, still passes its
own test while the board spends a HEAD on every run.

This drives one loopback server through all of it: the probe, the 304, the run that must
spend no request at all, the skip runs inside the window, and the expired window that spends
exactly one HEAD to reopen the probe.

Time is simulated by moving the stored timestamps back rather than sleeping; the state is the
only clock the decision reads.
"""

from __future__ import annotations

import http.server
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from unittest import mock
from urllib.parse import urlparse

from src import jobs_fetcher as jf
from src.jobs import state_incremental as si
from src.jobs.common.http import default_fetch_text
from src.jobs.state_source_state import update_source_state_rows

ETAG = '"listing-v1"'
NEW_ETAG = '"listing-v2"'
EMPTY_LISTING = b"<html><body><h1>Careers</h1><p>Nothing open right now.</p></body></html>"


@contextmanager
def _conditional_server(
    etag: str | None = ETAG,
    page_etags: dict[str, str | None] | None = None,
) -> Iterator[tuple[str, list[tuple[str, str]]]]:
    """A loopback listing that answers 304 to the ETag it currently publishes.

    ``tests.helpers.local_http_server`` cannot serve this: its callable routes see no request
    headers, and the header is the whole question here. ``etag`` is the version the server is
    serving now, which is how a test makes the board change under a stored validator; ``None``
    is a board that publishes no validator at all. ``page_etags`` overrides that version per
    path, which is how a multi-page board is made to change on one page only.
    """
    requests: list[tuple[str, str]] = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def _respond(self, *, with_body: bool) -> None:
            path = urlparse(self.path).path
            current_etag = (page_etags or {}).get(path, etag)
            conditional = (self.headers.get("If-None-Match") or "").strip()
            requests.append((self.command, conditional))
            if current_etag and conditional == current_etag:
                self.send_response(304)
                self.send_header("ETag", current_etag)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            if current_etag:
                self.send_header("ETag", current_etag)
            self.send_header("Content-Length", str(len(EMPTY_LISTING)))
            self.end_headers()
            if with_body:
                self.wfile.write(EMPTY_LISTING)

        def do_GET(self) -> None:  # noqa: N802 - http.server's naming
            self._respond(with_body=True)

        def do_HEAD(self) -> None:  # noqa: N802 - http.server's naming
            self._respond(with_body=False)

        def log_message(self, *args: Any) -> None:
            return None

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _source(name: str, listing_url: str | list[str]) -> dict[str, object]:
    pages = [listing_url] if isinstance(listing_url, str) else list(listing_url)
    return {
        "name": name,
        "studio": name,
        "company": name,
        "adapter": "static",
        "pages": pages,
        "id": f"static:listing_url:{pages[0]}",
    }


def _parsed_row(listing_url: str, name: str) -> dict[str, object]:
    return {
        "sourceJobId": "static:changed:qa",
        "title": "QA Analyst",
        "company": name,
        "city": "",
        "country": "Unknown",
        "locations": [],
        "locationSummary": "",
        "workType": "",
        "contractType": "",
        "jobLink": f"{listing_url}/qa",
        "sector": "Game",
        "postedAt": "",
    }


def _stored_row(listing_url: str, *, kept: int, zero_kept_streak: int) -> dict[str, Any]:
    """A row as a full read leaves it, long past its window and holding a validator."""
    long_ago = (datetime.now(UTC) - timedelta(days=20)).isoformat()
    return {
        "lastStatus": "ok",
        "lastAdapter": "static",
        "listingUrl": listing_url,
        "lastKeptCount": kept,
        "lastJobsFound": kept,
        "consecutiveZeroKept": zero_kept_streak,
        "lastHttpEtag": ETAG,
        "lastHttpStatus": 200,
        "lastSuccessAt": long_ago,
        "lastCheckedAt": long_ago,
        "nextEligibleCheckAt": (datetime.now(UTC) - timedelta(minutes=5)).isoformat(),
    }


def _run(name: str, listing_url: str | list[str], rows: dict[str, Any]) -> dict[str, Any]:
    body_fetches: list[str] = []

    def fetch_text(url: str, timeout_s: int) -> str:
        body_fetches.append(url)
        return default_fetch_text(url, timeout_s, headers={})

    jf.SOURCE_DIAGNOSTICS.clear()
    kept = jf.run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[_source(name, listing_url)],
        source_state_rows=rows,
        force_refresh_all=False,
    )
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    return {
        "name": name,
        "adapter": "static",
        "status": detail.get("status") or "",
        "keptCount": len(kept or []),
        "bodyFetches": body_fetches,
        "cacheDecision": detail.get("cacheDecision") or "",
        "cacheDecisionReason": detail.get("cacheDecisionReason") or "",
        "exclusionReason": detail.get("exclusionReason") or "",
        "httpStatus": int(detail.get("httpStatus") or 0),
        "httpEtag": detail.get("httpEtag") or "",
        "httpLastModified": detail.get("httpLastModified") or "",
        "clearHttpEtag": bool(detail.get("clearHttpEtag")),
        "httpPageValidators": detail.get("httpPageValidators"),
        "pageValidators": detail.get("httpPageValidators"),
        "httpPageSeedAt": detail.get("httpPageSeedAt"),
    }


def _apply(rows: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    return update_source_state_rows(
        source_state_rows=rows,
        source_reports=[report],
        canonical_rows=[],
        finished_at=datetime.now(UTC).isoformat(),
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=720,
    )


def _decision(name: str, rows: dict[str, Any]) -> dict[str, str]:
    return si.get_incremental_cache_decision(name, rows, adapter="static")


def _move_clock_back(rows: dict[str, Any], name: str, *, minutes: float) -> None:
    """Simulate ``minutes`` of elapsed time, including the deadline a skip may have set."""
    then = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
    rows[name]["lastSuccessAt"] = then
    rows[name]["lastCheckedAt"] = then
    rows[name]["nextEligibleCheckAt"] = then


def test_a_kept_board_probes_once_then_waits_out_its_listing_window() -> None:
    """probe -> 304 -> a run that spends nothing -> the window reopens the probe."""
    name = "Revalidation Cycle Studio"
    with _conditional_server() as (base, requests):
        listing_url = f"{base}/careers"
        rows = {name: _stored_row(listing_url, kept=12, zero_kept_streak=0)}
        assert _decision(name, rows) == {
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "static_revalidate_due",
        }

        report = _run(name, listing_url, rows)
        assert requests == [("HEAD", ETAG)], "the probe must be one conditional HEAD"
        assert report["status"] == "excluded"
        assert report["exclusionReason"] == "cache_not_modified_304"
        assert report["cacheDecisionReason"] == "not_modified_304"
        assert report["httpStatus"] == 304
        assert report["bodyFetches"] == [], "a 304 must not download the listing"

        rows = _apply(rows, report)
        assert rows[name]["lastHttpStatus"] == 304
        assert rows[name]["cacheDecisionReason"] == "not_modified_304"
        assert _decision(name, rows) == {
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "static_revalidated_fresh",
        }

        requests.clear()
        skipped = _run(name, listing_url, rows)
        assert requests == [], "the run after a 304 must not ask the board again"
        assert skipped["bodyFetches"] == []
        assert skipped["status"] == "excluded"
        assert skipped["exclusionReason"] == "cache_static_revalidated_fresh"
        rows = _apply(rows, skipped)
        assert _decision(name, rows) == {
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "static_revalidated_fresh",
        }, "a skip is not a check: it must not restart the window"

        requests.clear()
        still_fresh = _run(name, listing_url, rows)
        assert requests == []
        assert still_fresh["exclusionReason"] == "cache_static_revalidated_fresh"

        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)
        assert _decision(name, rows) == {
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "static_revalidate_due",
        }

        requests.clear()
        reopened = _run(name, listing_url, rows)
        assert requests == [("HEAD", ETAG)], "the expired window spends exactly one HEAD"
        assert reopened["exclusionReason"] == "cache_not_modified_304"


def test_a_zero_kept_board_waits_out_its_empty_window_before_asking_again() -> None:
    """The same cycle, on the twelve-hour window a board that keeps nothing earns."""
    name = "Empty Revalidation Studio"
    with _conditional_server() as (base, requests):
        listing_url = f"{base}/openings"
        rows = {name: _stored_row(listing_url, kept=0, zero_kept_streak=3)}
        assert _decision(name, rows) == {
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "static_empty_revalidate_due",
        }

        report = _run(name, listing_url, rows)
        assert requests == [("HEAD", ETAG)]
        assert report["exclusionReason"] == "cache_not_modified_304"
        rows = _apply(rows, report)
        assert _decision(name, rows) == {
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "static_empty_fresh",
        }

        requests.clear()
        skipped = _run(name, listing_url, rows)
        assert requests == []
        assert skipped["exclusionReason"] == "cache_static_empty_fresh"
        rows = _apply(rows, skipped)

        # The listing window has expired; the empty window has not. Still no request.
        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)
        assert _decision(name, rows) == {
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "static_empty_fresh",
        }
        requests.clear()
        still_fresh = _run(name, listing_url, rows)
        assert requests == []
        assert still_fresh["exclusionReason"] == "cache_static_empty_fresh"

        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_EMPTY_SOURCE_MINUTES + 10)
        assert _decision(name, rows) == {
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "static_empty_revalidate_due",
        }

        requests.clear()
        reopened = _run(name, listing_url, rows)
        assert requests == [("HEAD", ETAG)]
        assert reopened["exclusionReason"] == "cache_not_modified_304"


def test_a_changed_listing_falls_through_to_the_full_read_and_stores_the_new_etag() -> None:
    """A 200 carrying a different validator is not a 304: read the board, keep the new tag."""
    name = "Changed Listing Studio"
    with _conditional_server(etag=NEW_ETAG) as (base, requests):
        listing_url = f"{base}/careers"
        rows = {name: _stored_row(listing_url, kept=4, zero_kept_streak=0)}
        assert rows[name]["lastHttpEtag"] == ETAG

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ) as parse:
            report = _run(name, listing_url, rows)

        assert requests == [
            ("HEAD", ETAG),
            ("GET", ""),
        ], "the stale validator is asked first, then the listing is downloaded"
        assert report["status"] == "ok"
        assert report["keptCount"] == 1
        assert report["httpStatus"] == 200
        assert report["httpEtag"] == NEW_ETAG
        assert report["bodyFetches"] == [listing_url]
        assert parse.called

        rows = _apply(rows, report)
        assert rows[name]["lastHttpEtag"] == NEW_ETAG
        assert rows[name]["lastKeptCount"] == 1

        # The validator the read earned is the one the next probe sends.
        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)
        assert _decision(name, rows)["cacheDecision"] == "revalidate_only"
        requests.clear()
        probed = _run(name, listing_url, rows)
        assert requests == [("HEAD", NEW_ETAG)]
        assert probed["exclusionReason"] == "cache_not_modified_304"


def test_a_200_probe_without_a_validator_drops_the_stored_etag() -> None:
    """A board that stopped publishing an ETag must not be asked with the old one."""
    name = "Dropped Validator Studio"
    with _conditional_server(etag=None) as (base, requests):
        listing_url = f"{base}/careers"
        rows = {name: _stored_row(listing_url, kept=4, zero_kept_streak=0)}
        assert rows[name]["lastHttpEtag"] == ETAG

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            report = _run(name, listing_url, rows)

        assert requests == [("HEAD", ETAG), ("GET", "")]
        assert report["status"] == "ok"
        assert report["httpStatus"] == 200
        assert report["httpEtag"] == ""
        assert report["clearHttpEtag"] is True

        rows = _apply(rows, report)
        assert not rows[name].get("lastHttpEtag"), "the retired tag must not survive the read"

        # The next window's probe has nothing stale left to send.
        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)
        assert _decision(name, rows)["cacheDecision"] == "revalidate_only"
        requests.clear()
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            _run(name, listing_url, rows)
        assert requests == [("HEAD", ""), ("GET", "")], "no stale validator is replayed"


def _stored_paged_row(pages: list[str], *, kept: int = 6) -> dict[str, Any]:
    """A multi-page board as a full read leaves it: page 1's pair, no page map yet."""
    return _stored_row(pages[0], kept=kept, zero_kept_streak=0)


def _page_map(pages: list[str], tags: list[tuple[str, str]]) -> dict[str, dict[str, str]]:
    return {
        page: {"etag": etag, "lastModified": last_modified}
        for page, (etag, last_modified) in zip(pages, tags, strict=False)
    }


def test_a_multi_page_board_seeds_every_page_then_skips_only_when_all_are_unchanged() -> None:
    """Each page is asked on its own tag; the board completes only when all answer 304."""
    name = "Paged Revalidation Studio"
    with _conditional_server(page_etags={"/one": ETAG, "/two": NEW_ETAG}) as (base, requests):
        pages = [f"{base}/one", f"{base}/two"]
        rows = {name: _stored_paged_row(pages)}
        assert _decision(name, rows) == {
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "static_revalidate_due",
        }

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            seeded = _run(name, pages, rows)

        assert requests == [
            ("HEAD", ETAG),
            ("HEAD", ""),
            ("GET", ""),
            ("GET", ""),
        ], "page 1 asks with the stored pair; page 2 must not be asked with page 1's tag"
        assert seeded["status"] == "ok"
        assert seeded["bodyFetches"] == pages
        assert seeded["pageValidators"] == _page_map(pages, [(ETAG, ""), (NEW_ETAG, "")])

        rows = _apply(rows, seeded)
        assert rows[name]["lastHttpPageValidators"] == seeded["pageValidators"]

        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)
        assert _decision(name, rows) == {
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "static_revalidate_due",
        }

        requests.clear()
        skipped = _run(name, pages, rows)
        assert requests == [("HEAD", ETAG), ("HEAD", NEW_ETAG)], "each page's own tag is sent"
        assert skipped["exclusionReason"] == "cache_not_modified_304"
        assert skipped["bodyFetches"] == [], "all pages unchanged means no page is downloaded"

        rows = _apply(rows, skipped)
        assert _decision(name, rows) == {
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "static_revalidated_fresh",
        }, "the per-page map earns the same window a single-page pair does"

        requests.clear()
        idle = _run(name, pages, rows)
        assert requests == [], "the run after the skip must not ask the board again"
        assert idle["exclusionReason"] == "cache_static_revalidated_fresh"


def test_one_changed_page_makes_the_whole_board_read_and_keeps_the_other_page_tag() -> None:
    """A 304 on page 1 cannot vouch for page 2, and a partial read is not a skip."""
    name = "Partially Changed Paged Studio"
    old_second = '"listing-v0"'
    with _conditional_server(page_etags={"/one": ETAG, "/two": NEW_ETAG}) as (base, requests):
        pages = [f"{base}/one", f"{base}/two"]
        rows = {name: _stored_paged_row(pages)}
        rows[name]["lastHttpPageValidators"] = _page_map(pages, [(ETAG, ""), (old_second, "")])

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            report = _run(name, pages, rows)

        assert requests == [
            ("HEAD", ETAG),
            ("HEAD", old_second),
            ("GET", ""),
            ("GET", ""),
        ], "the unchanged page 304s, the changed one 200s, and both are then downloaded"
        assert report["bodyFetches"] == pages
        assert report["status"] == "ok"
        assert report["pageValidators"] == _page_map(pages, [(ETAG, ""), (NEW_ETAG, "")])

        rows = _apply(rows, report)
        assert rows[name]["lastHttpPageValidators"][pages[1]]["etag"] == NEW_ETAG
        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)
        requests.clear()
        probed = _run(name, pages, rows)
        assert probed["exclusionReason"] == "cache_not_modified_304"
        assert requests == [("HEAD", ETAG), ("HEAD", NEW_ETAG)], "the new tag is what is asked"


def test_a_page_that_stops_publishing_a_validator_drops_only_its_entry() -> None:
    """Retirement is per page: the tagless page is forgotten, the tagged one is kept."""
    name = "Retired Page Validator Studio"
    with _conditional_server(etag=None, page_etags={"/one": ETAG}) as (base, requests):
        pages = [f"{base}/one", f"{base}/two"]
        rows = {name: _stored_paged_row(pages)}
        rows[name]["lastHttpPageValidators"] = _page_map(pages, [(ETAG, ""), ('"listing-v0"', "")])

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            report = _run(name, pages, rows)

        assert requests == [("HEAD", ETAG), ("HEAD", '"listing-v0"'), ("GET", ""), ("GET", "")]
        assert report["pageValidators"] == _page_map(pages, [(ETAG, "")]), (
            "the page that answers 200 without a validator must not keep its old one"
        )

        rows = _apply(rows, report)
        assert pages[1] not in rows[name]["lastHttpPageValidators"]

        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)
        requests.clear()
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            _run(name, pages, rows)
        assert requests[:2] == [("HEAD", ETAG), ("HEAD", "")], (
            "no retired validator is replayed; the tagless page is asked plainly"
        )


def test_the_page_validator_map_survives_a_state_round_trip(tmp_path) -> None:
    """The map is persisted, so it must clear the state whitelist unchanged."""
    from src.jobs.state_source_records import read_source_state, write_source_state

    state_path = tmp_path / "jobs-source-state.json"
    write_source_state(
        state_path,
        {
            "Paged Studio": {
                "lastHttpPageValidators": {
                    "https://example.test/one": {"etag": ETAG, "lastModified": ""},
                    "https://example.test/two": {"lastModified": "Wed, 21 Oct 2015 07:28:00 GMT"},
                    "https://example.test/three": {"etag": "", "lastModified": ""},
                    "https://example.test/four": "not-a-map",
                }
            }
        },
    )

    stored = read_source_state(state_path)["Paged Studio"]["lastHttpPageValidators"]
    assert stored == {
        "https://example.test/one": {"etag": ETAG, "lastModified": ""},
        "https://example.test/two": {
            "etag": "",
            "lastModified": "Wed, 21 Oct 2015 07:28:00 GMT",
        },
    }, "a page with no validator left is dropped, not stored as an empty tag"


def test_a_tagless_multi_page_board_is_swept_once_a_week_not_every_run() -> None:
    """A page with no validator cannot answer 304, so asking it is pure latency.

    Four HEADs against a Japanese host measured ~4.8 s, so re-asking tagless pages on
    every window would tax exactly the runs the probe is meant to shorten. The sweep
    is still repeated, because that is how a board that starts publishing a tag is
    noticed -- just not on every run.
    """
    name = "Tagless Paged Studio"
    with _conditional_server(etag=None) as (base, requests):
        pages = [f"{base}/one", f"{base}/two"]
        rows = {name: _stored_row(pages[0], kept=6, zero_kept_streak=0)}
        rows[name].pop("lastHttpEtag")

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            first = _run(name, pages, rows)

        assert requests == [("HEAD", ""), ("HEAD", ""), ("GET", ""), ("GET", "")], (
            "the first due run seeds every page, because none has a validator yet"
        )
        rows = _apply(rows, first)
        assert rows[name]["lastHttpPageSeedAt"], "the seed clock starts on the sweep"

        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)
        requests.clear()
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            second = _run(name, pages, rows)

        assert requests == [("GET", ""), ("GET", "")], (
            "inside the seed window the board is read, not probed page by page"
        )
        assert second["bodyFetches"] == pages

        rows = _apply(rows, second)
        rows[name]["lastHttpPageSeedAt"] = (datetime.now(UTC) - timedelta(days=8)).isoformat()
        _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)
        requests.clear()
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            _run(name, pages, rows)

        assert requests[:2] == [("HEAD", ""), ("HEAD", "")], (
            "after the week the sweep runs again, so a new validator is discovered"
        )


def test_a_skipped_first_page_never_borrows_another_page_validator() -> None:
    """The singular pair mirrors page 1. A tag from page 2 would later seed page 1 wrong."""
    name = "Second Page Tag Only Studio"
    with _conditional_server(page_etags={"/one": ETAG, "/two": NEW_ETAG}) as (base, requests):
        pages = [f"{base}/one", f"{base}/two"]
        rows = {name: _stored_paged_row(pages)}
        rows[name].pop("lastHttpEtag")
        rows[name]["lastHttpPageSeedAt"] = datetime.now(UTC).isoformat()
        rows[name]["lastHttpPageValidators"] = _page_map([pages[1]], [(NEW_ETAG, "")])

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            report = _run(name, pages, rows)

        assert requests == [("HEAD", NEW_ETAG), ("GET", ""), ("GET", "")], (
            "page 1 has no tag and the seed week has not elapsed, so only page 2 is asked"
        )
        assert report["exclusionReason"] != "cache_not_modified_304", (
            "an unprobed page cannot vouch for the board"
        )
        assert report["httpEtag"] == "", "page 2's tag must not become the board's pair"


def test_the_sweep_stops_at_the_first_page_it_knows_has_changed() -> None:
    """Once a page disowns its validator the read is certain; the pages behind it are not asked."""
    name = "Three Page Studio"
    with _conditional_server(page_etags={"/one": ETAG, "/two": NEW_ETAG, "/three": ETAG}) as (
        base,
        requests,
    ):
        pages = [f"{base}/one", f"{base}/two", f"{base}/three"]
        rows = {name: _stored_row(pages[0], kept=6, zero_kept_streak=0)}
        rows[name]["lastHttpPageValidators"] = _page_map(
            pages, [(ETAG, ""), ('"listing-v0"', ""), (ETAG, "")]
        )

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            report = _run(name, pages, rows)

        assert requests == [
            ("HEAD", ETAG),
            ("HEAD", '"listing-v0"'),
            ("GET", ""),
            ("GET", ""),
            ("GET", ""),
        ], "page 3 is never asked: page 2 already settled that the read happens"
        assert report["pageValidators"][pages[0]]["etag"] == ETAG
        assert report["pageValidators"][pages[1]]["etag"] == NEW_ETAG, "the change is stored"
        assert report["pageValidators"][pages[2]]["etag"] == ETAG, (
            "an unasked page keeps the tag it had: its own answer would have been the only evidence"
        )


# ------------------------------------------------------------------ a tagless board

READ_CLOCK = datetime(2026, 6, 2, 9, 0, tzinfo=UTC)
READ_CLOCK_HTTP = "Tue, 02 Jun 2026 09:00:00 GMT"


@contextmanager
def _tagless_server(*, modified_at: datetime) -> Iterator[tuple[str, list[tuple[str, str]]]]:
    """A listing that publishes no validator but does honour ``If-Modified-Since``.

    ``modified_at`` is the document's own version. A request asking with an earlier date is a
    200; one asking at or after it is a 304. Nothing is sent back as an ETag or Last-Modified,
    which is the whole point: the flow has no server tag to ask with, only the clock of its
    own last read.
    """
    requests: list[tuple[str, str]] = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def _respond(self, *, with_body: bool) -> None:
            asked = (self.headers.get("If-Modified-Since") or "").strip()
            requests.append((self.command, asked))
            if asked:
                parsed = parsedate_to_datetime(asked)
                if parsed is not None and parsed >= modified_at:
                    self.send_response(304)
                    self.end_headers()
                    return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(EMPTY_LISTING)))
            self.end_headers()
            if with_body:
                self.wfile.write(EMPTY_LISTING)

        def do_GET(self) -> None:  # noqa: N802 - http.server's naming
            self._respond(with_body=True)

        def do_HEAD(self) -> None:  # noqa: N802 - http.server's naming
            self._respond(with_body=False)

        def log_message(self, *args: Any) -> None:
            return None

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _tagless_stored_row(listing_url: str, *, streak: int) -> dict[str, Any]:
    """A read that kept nothing and published no validator, long past its empty window."""

    return {
        "lastStatus": "ok",
        "lastAdapter": "static",
        "listingUrl": listing_url,
        "lastKeptCount": 0,
        "lastJobsFound": 0,
        "consecutiveZeroKept": streak,
        "lastSuccessAt": READ_CLOCK.isoformat(),
        "lastCheckedAt": READ_CLOCK.isoformat(),
        "nextEligibleCheckAt": (datetime.now(UTC) - timedelta(minutes=5)).isoformat(),
    }


def test_a_tagless_board_asks_with_its_read_clock_and_a_304_satisfies_the_empty_window() -> None:
    """No validator to send is not the same as no question to ask."""
    name = "Tagless Cycle Studio"
    with _tagless_server(modified_at=datetime(2026, 6, 1, 12, 0, tzinfo=UTC)) as (base, requests):
        listing_url = f"{base}/openings"
        rows = {name: _tagless_stored_row(listing_url, streak=3)}
        assert _decision(name, rows) == {
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "static_empty_revalidate_due",
        }

        report = _run(name, listing_url, rows)

        assert requests == [("HEAD", READ_CLOCK_HTTP)], (
            "the probe carries the read clock, not an absent validator"
        )
        assert report["exclusionReason"] == "cache_not_modified_304"
        assert report["bodyFetches"] == [], "a 304 must not download the listing"
        assert report["httpStatus"] == 304
        assert report["httpLastModified"] == "", (
            "the date we asked with is ours; the server published nothing to store"
        )

        rows = _apply(rows, report)
        assert not rows[name].get("lastHttpLastModified")
        assert not rows[name].get("lastHttpEtag")
        assert _decision(name, rows) == {
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "static_empty_fresh",
        }


def test_a_tagless_board_that_ignores_the_clock_reads_exactly_as_before() -> None:
    """The synthesised date is a request header, so a server that ignores it costs nothing."""
    name = "Tagless Ignoring Studio"
    with _tagless_server(modified_at=datetime(2027, 1, 1, tzinfo=UTC)) as (base, requests):
        listing_url = f"{base}/openings"
        rows = {name: _tagless_stored_row(listing_url, streak=3)}

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            report = _run(name, listing_url, rows)

        assert requests == [("HEAD", READ_CLOCK_HTTP), ("GET", "")], (
            "the conditional question is asked, then the page is read when it answers 200"
        )
        assert report["bodyFetches"] == [listing_url]
        assert report["status"] == "ok"
        assert report["keptCount"] == 1

        rows = _apply(rows, report)
        assert not rows[name].get("lastHttpLastModified"), (
            "a page that publishes no tag still has none after the read"
        )
