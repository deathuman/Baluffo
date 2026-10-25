"""Validators earned by the read, not only by the probe.

A full read answers the same question a conditional probe asks -- which copy of the page
the board serves now -- so the tag on its own response is worth keeping. Without that,
every board spent its first expired window on a plain HEAD that could only learn a tag the
read had already fetched, then waited another window before the tag could earn a 304.

These tests drive the real runner against a loopback board: the first read must store the
tag, the first window after it must already revalidate, a read that sees no tag must retire
the stored one, and a page that did not answer must keep what it had.
"""

from __future__ import annotations

import asyncio
import http.server
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock
from urllib.parse import urlparse

from src import jobs_fetcher as jf
from src.jobs import state_incremental as si
from src.jobs.adapters import static_listing
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
)
from src.jobs.state_source_state import update_source_state_rows
from src.jobs.transport import PROBE_RETIRE_RECHECK_S, default_fetch_text

ETAG = '"read-v1"'
SECOND_ETAG = '"read-v2"'
STALE_ETAG = '"read-v0"'
LAST_MODIFIED = "Wed, 21 Oct 2015 07:28:00 GMT"
# No anchors: the patched parser supplies the rows, and a link here would send the
# read on to a detail page (which is never asked conditionally).
LISTING = b"<html><body><h1>Careers</h1></body></html>"


@contextmanager
def _listing_server(
    *,
    etags: dict[str, str | None] | None = None,
    fail_paths: set[str] | None = None,
    last_modified: str | None = None,
    ignores_conditional: bool = False,
) -> Iterator[tuple[str, list[tuple[str, str, str]]]]:
    """A loopback board that publishes validators and (usually) honours them.

    ``ignores_conditional`` is the server defect the probe retirement exists for: it
    publishes the tag and then answers 200 in full even when asked about that very tag.
    """
    requests: list[tuple[str, str, str]] = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def _respond(self, *, with_body: bool) -> None:
            path = urlparse(self.path).path
            etag = (etags or {}).get(path, ETAG)
            conditional = (self.headers.get("If-None-Match") or "").strip()
            requests.append((self.command, path, conditional))
            if fail_paths and path in fail_paths:
                self.send_response(500)
                self.end_headers()
                return
            since = (self.headers.get("If-Modified-Since") or "").strip()
            if etag and conditional == etag and not ignores_conditional:
                self.send_response(304)
                self.send_header("ETag", etag)
                self.end_headers()
                return
            if last_modified and not etag and since == last_modified and not ignores_conditional:
                self.send_response(304)
                self.send_header("Last-Modified", last_modified)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            if etag:
                self.send_header("ETag", etag)
            if last_modified:
                self.send_header("Last-Modified", last_modified)
            self.send_header("Content-Length", str(len(LISTING)))
            self.end_headers()
            if with_body:
                self.wfile.write(LISTING)

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
        "sourceJobId": "static:read-seed:qa",
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


def _long_ago() -> str:
    return (datetime.now(UTC) - timedelta(days=20)).isoformat()


def _stored_row(listing_url: str, *, kept: int) -> dict[str, Any]:
    stamp = _long_ago()
    return {
        "lastStatus": "ok",
        "lastAdapter": "static",
        "listingUrl": listing_url,
        "lastKeptCount": kept,
        "lastJobsFound": kept,
        "consecutiveZeroKept": 0,
        "lastHttpEtag": STALE_ETAG,
        "lastHttpStatus": 200,
        "lastSuccessAt": stamp,
        "lastCheckedAt": stamp,
        "nextEligibleCheckAt": stamp,
    }


def _run(
    name: str,
    listing_url: str | list[str],
    rows: dict[str, Any],
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Run the real static source with a request-aware fetch, so captures reach the report."""
    body_fetches: list[str] = []

    def fetch_text(url: str, timeout_s: int, request: Any = None) -> str:
        body_fetches.append(url)
        return default_fetch_text(url, timeout_s, request=request)

    jf.SOURCE_DIAGNOSTICS.clear()
    kept = jf.run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        sources=[_source(name, listing_url)],
        source_state_rows=rows,
        force_refresh_all=force_refresh,
    )
    detail = ((jf.SOURCE_DIAGNOSTICS.get("static_studio_pages") or {}).get("details") or [{}])[0]
    return {
        "name": name,
        "adapter": "static",
        "status": detail.get("status") or "",
        "keptCount": len(kept or []),
        "bodyFetches": body_fetches,
        "cacheDecision": detail.get("cacheDecision") or "",
        "exclusionReason": detail.get("exclusionReason") or "",
        "httpEtag": detail.get("httpEtag") or "",
        "httpLastModified": detail.get("httpLastModified") or "",
        "clearHttpEtag": bool(detail.get("clearHttpEtag")),
        "httpPageValidators": detail.get("httpPageValidators"),
        "httpPageSeedAt": detail.get("httpPageSeedAt"),
        "httpProbeEchoAt": detail.get("httpProbeEchoAt") or "",
        "clearHttpProbeEcho": bool(detail.get("clearHttpProbeEcho")),
        "clearHttpProbeIgnored": bool(detail.get("clearHttpProbeIgnored")),
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
    then = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
    rows[name]["lastSuccessAt"] = then
    rows[name]["lastCheckedAt"] = then
    rows[name]["nextEligibleCheckAt"] = then


def _expire_listing_window(rows: dict[str, Any], name: str) -> None:
    _move_clock_back(rows, name, minutes=si.DEFAULT_INCREMENTAL_STATIC_LISTING_MINUTES + 10)


def test_a_full_read_stores_the_etag_so_the_first_window_already_revalidates() -> None:
    """read -> tag stored -> the next window is one conditional HEAD, not a blind one."""
    name = "Read Seed Studio"
    with _listing_server() as (base, requests):
        listing_url = f"{base}/careers"
        rows: dict[str, Any] = {name: {}}
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            first = _run(name, listing_url, rows)

        assert requests == [("GET", "/careers", "")], "the first run is a plain read"
        assert first["status"] == "ok"
        assert first["httpEtag"] == ETAG, "the read's own response must seed the validator"
        rows = _apply(rows, first)
        assert rows[name]["lastHttpEtag"] == ETAG

        _expire_listing_window(rows, name)
        assert _decision(name, rows) == {
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "static_revalidate_due",
        }

        requests.clear()
        second = _run(name, listing_url, rows)
        assert requests == [
            ("HEAD", "/careers", ETAG),
        ], "the seeded tag must be sent on the first probe, with no blind HEAD first"
        assert second["exclusionReason"] == "cache_not_modified_304"
        assert second["bodyFetches"] == [], "a 304 must not download the listing"


def test_a_read_stores_last_modified_when_the_board_publishes_no_etag() -> None:
    """The other validator is captured the same way: its own response stores it."""
    name = "Last Modified Seed Studio"
    with _listing_server(etags={"/careers": None}, last_modified=LAST_MODIFIED) as (
        base,
        requests,
    ):
        listing_url = f"{base}/careers"
        rows: dict[str, Any] = {name: {}}
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            first = _run(name, listing_url, rows)

        assert first["httpEtag"] == "", "the board publishes no Etag"
        assert first["httpLastModified"] == LAST_MODIFIED
        rows = _apply(rows, first)
        assert rows[name]["lastHttpLastModified"] == LAST_MODIFIED

        _expire_listing_window(rows, name)
        requests.clear()
        second = _run(name, listing_url, rows)
        assert requests == [("HEAD", "/careers", "")], "one conditional HEAD, no read"
        assert second["exclusionReason"] == "cache_not_modified_304"
        assert second["bodyFetches"] == []


def test_a_multi_page_read_seeds_every_pages_own_tag() -> None:
    """Each configured page is asked later with the tag its own read response carried."""
    name = "Paged Read Seed Studio"
    with _listing_server(etags={"/one": ETAG, "/two": SECOND_ETAG}) as (base, requests):
        pages = [f"{base}/one", f"{base}/two"]
        rows: dict[str, Any] = {name: {}}
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            first = _run(name, pages, rows)

        assert sorted(requests) == [("GET", "/one", ""), ("GET", "/two", "")]
        assert first["httpPageValidators"] == {
            pages[0]: {"etag": ETAG, "lastModified": ""},
            pages[1]: {"etag": SECOND_ETAG, "lastModified": ""},
        }, "the read must seed each page's own validator, not page 1's for all of them"
        rows = _apply(rows, first)
        assert rows[name]["lastHttpPageValidators"] == first["httpPageValidators"]

        _expire_listing_window(rows, name)
        assert _decision(name, rows)["cacheDecision"] == "revalidate_only", (
            "the per-page map must earn the probe without a seed sweep"
        )

        requests.clear()
        second = _run(name, pages, rows)
        assert requests == [
            ("HEAD", "/one", ETAG),
            ("HEAD", "/two", SECOND_ETAG),
        ], "each page is asked with its own seeded tag"
        assert second["exclusionReason"] == "cache_not_modified_304"
        assert second["bodyFetches"] == []


def test_a_read_that_sees_no_tag_retires_the_stored_one_without_a_probe() -> None:
    """The full read is an observation too: a 200 without a tag drops the old value."""
    name = "Read Retirement Studio"
    with _listing_server(etags={"/careers": None}) as (base, requests):
        listing_url = f"{base}/careers"
        rows = {name: _stored_row(listing_url, kept=3)}
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            report = _run(name, listing_url, rows, force_refresh=True)

        assert requests == [("GET", "/careers", "")], (
            "force refresh takes the read directly, so only the read can retire the tag"
        )
        assert report["httpEtag"] == ""
        assert report["clearHttpEtag"] is True

        rows = _apply(rows, report)
        assert not rows[name].get("lastHttpEtag"), "the retired tag must not survive the read"

        _expire_listing_window(rows, name)
        requests.clear()
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            _run(name, listing_url, rows)
        assert requests == [("HEAD", "/careers", ""), ("GET", "/careers", "")], (
            "no retired tag is replayed"
        )


def test_a_page_that_did_not_answer_keeps_the_validator_it_had() -> None:
    """A failed page said nothing about itself, so its stored tag must survive the read."""
    name = "Partially Read Studio"
    with _listing_server(etags={"/one": ETAG, "/two": SECOND_ETAG}, fail_paths={"/two"}) as (
        base,
        requests,
    ):
        pages = [f"{base}/one", f"{base}/two"]
        rows = {name: _stored_row(pages[0], kept=5)}
        rows[name]["lastHttpPageValidators"] = {
            pages[0]: {"etag": STALE_ETAG, "lastModified": ""},
            pages[1]: {"etag": SECOND_ETAG, "lastModified": ""},
        }

        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(pages[0], name)],
        ):
            report = _run(name, pages, rows, force_refresh=True)

        assert sorted(requests) == [("GET", "/one", ""), ("GET", "/two", "")]
        assert report["httpPageValidators"] == {
            pages[0]: {"etag": ETAG, "lastModified": ""},
            pages[1]: {"etag": SECOND_ETAG, "lastModified": ""},
        }, "the answered page is updated; the failed page keeps the tag it had"

        rows = _apply(rows, report)
        assert rows[name]["lastHttpPageValidators"][pages[1]]["etag"] == SECOND_ETAG


# ---------------------------------------------------------------------------
# The async listing seam is what production rides; the capture must reach it.
# ---------------------------------------------------------------------------


def _runner_ctx(*, pages: list[str], listing_async_fetch: Any) -> StaticSourceContext:
    source_name = "Async Read Seed Studio"
    source = {
        "name": source_name,
        "company": source_name,
        "pages": pages,
        "antiBotBrowserRetry": False,
    }
    run_deps = StaticRunDeps(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
        listing_async_fetch=listing_async_fetch,
    )
    return StaticSourceContext(
        run_deps=run_deps,
        runtime_config=StaticSourceRuntimeConfig(
            static_profile="standard",
            static_detail_concurrency=1,
            static_source_time_budget_s=30,
            low_yield_detail_cap=10,
            very_low_yield_detail_cap=3,
            uncapped_deep_static=False,
            listing_only_hosts=[],
            default_path_tokens=[],
            default_query_keys=[],
        ),
        html_fetcher=StaticHtmlFetcher(
            fetch_text=run_deps.fetch_text,
            timeout_s=run_deps.timeout_s,
            retries=run_deps.retries,
            backoff_s=run_deps.backoff_s,
        ),
        source=source,
        source_name=source_name,
        company=source_name,
        pages=pages,
        entry_report=build_static_entry_report(
            source=source,
            source_name=source_name,
            pages=pages,
            company=source_name,
        ),
        state_entry={},
        selected_source_count=1,
        jobs=[],
        warnings=[],
        errors=[],
        details=[],
    )


def test_the_async_listing_seam_is_handed_a_capture_sink_and_seeds_the_report() -> None:
    page = "https://async-read.example.com/careers"
    received: dict[str, Any] = {}

    async def listing_async_fetch(
        _client: Any,
        _job: Any,
        url: str,
        _timeout: int,
        request: Any = None,
    ) -> str:
        received["url"] = url
        received["request"] = request
        sink = request.capture_response_headers
        sink.update({"status": "200", "etag": ETAG, "last-modified": ""})
        return "<html></html>"

    ctx = _runner_ctx(pages=[page], listing_async_fetch=listing_async_fetch)
    runner = static_listing.StaticFetchRunner(ctx)
    asyncio.run(runner._fetch_listing_job_async(None, {"url": page, "payload": {}}, page, 5))

    assert received["url"] == page
    assert received["request"] is not None
    assert received["request"].capture_response_headers is not None
    runner._apply_listing_read_validators()
    assert ctx.entry_report["httpEtag"] == ETAG


def test_the_plugin_listing_fetch_captures_only_configured_pages() -> None:
    """Plugin boards read through the cached fetch; that response seeds them too."""
    from src.jobs.adapters.static_listing_plugin import _capturing_fetch_html_cached

    page = "https://plugin-read.example.com/careers"
    other = "https://plugin-read.example.com/about"
    ctx = _runner_ctx(pages=[page], listing_async_fetch=None)
    captured: dict[str, dict[str, str]] = {}

    def fake_fetch(
        url: str,
        *,
        capture_headers: dict[str, str] | None = None,
        remaining_budget_s: float | None = None,
    ) -> tuple[str, bool]:
        del url, remaining_budget_s
        if capture_headers is not None:
            capture_headers.update({"status": "200", "etag": ETAG})
        return "<html></html>", False

    wrapped = _capturing_fetch_html_cached(ctx, fake_fetch, captured)
    wrapped(page)
    wrapped(other)
    assert captured == {page: {"status": "200", "etag": ETAG}}, (
        "only a configured listing page is watched; a plugin's other fetches are not"
    )

    def cached_fetch(
        url: str,
        *,
        capture_headers: dict[str, str] | None = None,
    ) -> tuple[str, bool]:
        del url, capture_headers
        return "<html></html>", True

    cached_capture: dict[str, dict[str, str]] = {}
    _capturing_fetch_html_cached(ctx, cached_fetch, cached_capture)(page)
    assert cached_capture == {}, "a cache hit saw no response this run, so it seeds nothing"

    def legacy_fetch(url: str) -> tuple[str, bool]:
        del url
        return "<html></html>", False

    assert _capturing_fetch_html_cached(ctx, legacy_fetch, {}) is legacy_fetch, (
        "a seam that cannot take a capture sink is left exactly as it was"
    )


def test_a_four_argument_async_seam_is_still_called_unchanged_and_learns_nothing() -> None:
    """A seam that cannot take a request is a fail-open: call it as before, capture nothing."""
    page = "https://legacy-read.example.com/careers"
    calls: list[tuple[str, int]] = []

    async def listing_async_fetch(_client: Any, _job: Any, url: str, timeout: int) -> str:
        calls.append((url, timeout))
        return "<html></html>"

    ctx = _runner_ctx(pages=[page], listing_async_fetch=listing_async_fetch)
    runner = static_listing.StaticFetchRunner(ctx)
    asyncio.run(runner._fetch_listing_job_async(None, {"url": page, "payload": {}}, page, 5))

    assert calls == [(page, 5)]
    assert runner._listing_read_validators == {}
    assert "httpEtag" not in ctx.entry_report


def _pipeline_report(source_name: str, entry: dict[str, Any]) -> dict[str, Any]:
    """The shape the pipeline publishes: one loader row aggregating a named entry."""
    return {
        "name": source_name,
        "adapter": "static",
        "status": entry.get("status") or "ok",
        "keptCount": int(entry.get("keptCount") or 0),
        "details": [entry],
    }


def test_a_loader_row_keeps_the_tag_its_only_entry_carried() -> None:
    """The decision looks up the loader name, so that row is where the tag must land.

    A static board is reported as one loader row whose entry -- keyed by the studio's name
    -- carries the listing response's headers. Persisting only the entry leaves the loader
    row tagless, so the tag the read just earned could never earn its 304.
    """
    source_name = "static_source::static:listing_url:https://aggregate.example/careers"
    entry: dict[str, Any] = {
        "name": "Aggregate Studio (sheet)",
        "adapter": "static",
        "status": "ok",
        "keptCount": 3,
        "httpStatus": 200,
        "httpEtag": ETAG,
        "httpLastModified": LAST_MODIFIED,
    }

    rows = _apply({}, _pipeline_report(source_name, entry))

    loader = rows[source_name]
    assert loader["lastHttpEtag"] == ETAG
    assert loader["lastHttpLastModified"] == LAST_MODIFIED
    assert loader["lastHttpStatus"] == 200
    assert rows["Aggregate Studio (sheet)"]["lastHttpEtag"] == ETAG, "the entry keeps its own"


def test_a_row_that_already_carries_an_http_answer_is_not_replaced_by_its_entry() -> None:
    """A probe's own answer is the newer one; the entry must not overwrite it."""
    source_name = "static_source::static:listing_url:https://probed.example/careers"
    report = _pipeline_report(
        source_name,
        {"name": "Probed Studio", "status": "ok", "httpStatus": 200, "httpEtag": '"entry-v1"'},
    )
    report.update(
        {
            "httpStatus": 200,
            "httpEtag": STALE_ETAG,
            "cacheDecision": "revalidate_only",
            "cacheDecisionReason": "static_revalidate_due",
        }
    )

    rows = _apply({}, report)

    assert rows[source_name]["lastHttpEtag"] == STALE_ETAG


def test_a_304_reported_on_the_entry_keeps_the_loader_row_asleep() -> None:
    """The exclusion lives on the entry; the loader row must still learn it was a 304.

    A 304 is an exclusion entry under a loader row that itself only knows the fetch did
    not raise. Persisting the aggregate alone records a successful run that kept nothing:
    the deadline slides, the reason is lost, and the next run re-downloads a page the
    board just said was unchanged.
    """
    name = "static_source::static:listing_url:https://sleepy.example/careers"
    listing_url = "https://sleepy.example/careers"
    rows: dict[str, Any] = {name: _stored_row(listing_url, kept=5)}
    rows[name]["cacheDecisionReason"] = "static_revalidate_due"
    # The aliases a real aged row carries: the health derive reads these first, so a 304
    # that refreshes only their fallbacks would keep the previous run's clocks.
    rows[name]["lastSuccessfulFetchAt"] = _long_ago()
    rows[name]["lastSeenInFetchAt"] = _long_ago()
    deadline_before = rows[name]["nextEligibleCheckAt"]
    report: dict[str, Any] = {
        "name": name,
        "adapter": "static",
        "status": "ok",
        "keptCount": 0,
        "details": [
            {
                "name": "Sleepy Studio (sheet)",
                "adapter": "static",
                "status": "excluded",
                "error": "not_modified_304",
                "exclusionReason": "cache_not_modified_304",
                "cacheDecision": "revalidate_only",
                "cacheDecisionReason": "not_modified_304",
                "httpStatus": 304,
                "httpLastModified": LAST_MODIFIED,
            }
        ],
    }

    rows = _apply(rows, report)

    loader = rows[name]
    assert loader["lastStatus"] == "excluded"
    assert loader["cacheDecisionReason"] == "not_modified_304"
    assert loader["lastHttpLastModified"] == LAST_MODIFIED
    assert loader["lastSuccessAt"] != _long_ago(), "a 304 is a successful answer to the probe"
    assert loader["lastSuccessfulFetchAt"] == loader["lastSuccessAt"]
    assert loader["lastCheckedAt"] != _long_ago(), "the probe ran this run, so the clock moved"
    assert loader["nextEligibleCheckAt"] == deadline_before, (
        "a 304 fetched nothing new, so it must not slide the deadline"
    )
    assert _decision(name, rows) == {
        "cacheDecision": "skip_fresh",
        "cacheDecisionReason": "static_revalidated_fresh",
    }, "the 304's own window is what keeps the board asleep"

    _expire_listing_window(rows, name)
    assert _decision(name, rows) == {
        "cacheDecision": "revalidate_only",
        "cacheDecisionReason": "static_revalidate_due",
    }


def test_a_board_that_ignores_its_validator_is_read_without_a_probe() -> None:
    """Two consecutive 200s echoing the tag retire the HEAD; the next window reads directly."""
    name = "Deaf Validator Studio"
    with _listing_server(ignores_conditional=True) as (base, requests):
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
        assert first["httpProbeEchoAt"], "the echoed tag is recorded"
        rows = _apply(rows, first)
        assert rows[name]["lastHttpProbeEchoAt"] == first["httpProbeEchoAt"]
        assert "lastHttpProbeIgnoredAt" not in rows[name], (
            "one echo can be a cache miss, so it is not yet a retirement"
        )

        _expire_listing_window(rows, name)
        requests.clear()
        with mock.patch(
            "src.jobs.adapters.static_listing.parse_jobpostings_from_html",
            return_value=[_parsed_row(listing_url, name)],
        ):
            second = _run(name, listing_url, rows)

        assert requests == [("HEAD", "/careers", ETAG), ("GET", "/careers", "")], (
            "an unconfirmed echo still asks, because the next ask is the confirmation"
        )
        assert second["httpProbeEchoAt"]
        rows = _apply(rows, second)
        assert rows[name]["lastHttpProbeIgnoredAt"] == second["httpProbeEchoAt"], (
            "the second consecutive echo is the proof"
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
        assert third["bodyFetches"] == [listing_url]
        assert third["status"] == "ok"


def test_the_retirement_is_rechecked_after_a_week_and_cleared_by_a_304() -> None:
    """The verdict is not permanent: a week later the board is asked once more."""
    name = "Recovered Validator Studio"
    with _listing_server() as (base, requests):
        listing_url = f"{base}/careers"
        rows = {name: _stored_row(listing_url, kept=12)}
        rows[name]["lastHttpEtag"] = ETAG
        rows[name]["lastHttpProbeIgnoredAt"] = (
            datetime.now(UTC) - timedelta(seconds=PROBE_RETIRE_RECHECK_S + 60)
        ).isoformat()
        _expire_listing_window(rows, name)

        report = _run(name, listing_url, rows)

        assert requests == [("HEAD", "/careers", ETAG)], "one recheck, and it 304s"
        assert report["exclusionReason"] == "cache_not_modified_304"
        assert report["clearHttpProbeIgnored"] is True
        assert report["clearHttpProbeEcho"] is True
        rows = _apply(rows, report)
        assert "lastHttpProbeIgnoredAt" not in rows[name], (
            "a board that honours the question again must get its probe back"
        )
        assert "lastHttpProbeEchoAt" not in rows[name], (
            "a 304 also resets the streak, so a later echo starts the count over"
        )


def test_a_multi_entry_report_does_not_borrow_one_entrys_tag() -> None:
    """Several entries have no single answer; guessing would ask with another board's tag."""
    source_name = "provider_sources"
    report: dict[str, Any] = {
        "name": source_name,
        "adapter": "greenhouse",
        "status": "ok",
        "details": [
            {"name": "Feed A", "status": "ok", "httpStatus": 200, "httpEtag": '"a"'},
            {"name": "Feed B", "status": "ok", "httpStatus": 200, "httpEtag": '"b"'},
        ],
    }

    rows = _apply({}, report)

    assert "lastHttpEtag" not in rows[source_name]
