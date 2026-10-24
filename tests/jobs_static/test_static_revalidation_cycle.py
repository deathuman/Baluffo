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
from typing import Any
from unittest import mock

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
) -> Iterator[tuple[str, list[tuple[str, str]]]]:
    """A loopback listing that answers 304 to the ETag it currently publishes.

    ``tests.helpers.local_http_server`` cannot serve this: its callable routes see no request
    headers, and the header is the whole question here. ``etag`` is the version the server is
    serving now, which is how a test makes the board change under a stored validator; ``None``
    is a board that publishes no validator at all.
    """
    requests: list[tuple[str, str]] = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def _respond(self, *, with_body: bool) -> None:
            conditional = (self.headers.get("If-None-Match") or "").strip()
            requests.append((self.command, conditional))
            if etag and conditional == etag:
                self.send_response(304)
                self.send_header("ETag", etag)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            if etag:
                self.send_header("ETag", etag)
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


def _source(name: str, listing_url: str) -> dict[str, object]:
    return {
        "name": name,
        "studio": name,
        "company": name,
        "adapter": "static",
        "pages": [listing_url],
        "id": f"static:listing_url:{listing_url}",
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


def _run(name: str, listing_url: str, rows: dict[str, Any]) -> dict[str, Any]:
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
        "clearHttpEtag": bool(detail.get("clearHttpEtag")),
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
