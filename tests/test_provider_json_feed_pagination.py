"""A feed that caps its response must be followed to the end.

SmartRecruiters answers with `totalFound` postings and returns at most 100 however the
request is phrased, so a single GET saw a third of Ubisoft's board: `totalFound=332`
against 100 rows. The runtime issued one request and kept 12 game jobs where the board holds
46, and 175 of the catalogue's missing Ubisoft roles sat in pages nobody asked for.

The other JSON feeds return everything in one response, so this must not change them. That is
the property worth pinning alongside the fix: a pagination loop that fires everywhere is a
new way to hammer an API, and SmartRecruiters rate-limits.
"""

from __future__ import annotations

import json
from typing import Any

from src.jobs.adapters.plugins.provider_api.json_feed import (
    JSON_FEED_SPECS,
    _build_json_feed_url,
    _fetch_json_feed_payload,
    _merge_json_feed_page,
    _with_query,
)

_SPEC = JSON_FEED_SPECS["smartrecruiters"]


def _feed(*, total: int, page_size: int, offset: int) -> str:
    rows = [
        {"id": str(i), "name": f"Role {i}", "department": {"label": "Art"}}
        for i in range(offset, min(offset + page_size, total))
    ]
    return json.dumps({"content": rows, "totalFound": total})


def _capped_fetcher(total: int, page_size: int = 100) -> tuple[Any, list[str]]:
    calls: list[str] = []

    def fetch(url: str, timeout: int) -> str:
        calls.append(url)
        offset = 0
        if "offset=" in url:
            query = url.split("?", 1)[1]
            offset = int(next(p.split("=")[1] for p in query.split("&") if p.startswith("offset=")))
        return _feed(total=total, page_size=page_size, offset=offset)

    return fetch, calls


def _ids(payload: dict[str, Any]) -> list[str]:
    return [str(row["id"]) for row in payload["content"]]


def test_a_capped_feed_is_followed_to_the_end() -> None:
    fetch, calls = _capped_fetcher(total=250)
    payload = _fetch_json_feed_payload("https://api/x/postings", _SPEC, fetch, 30, 1, 0.0)
    assert len(calls) == 3, calls
    assert len(_ids(payload)) == 250
    assert payload["totalFound"] == 250


def test_the_pages_are_disjoint() -> None:
    fetch, _calls = _capped_fetcher(total=250)
    payload = _fetch_json_feed_payload("https://api/x/postings", _SPEC, fetch, 30, 1, 0.0)
    ids = _ids(payload)
    assert len(set(ids)) == len(ids), "a repeated page would double-count postings"


def test_a_feed_that_fits_one_page_issues_exactly_one_request() -> None:
    """The common case must not become N requests."""
    fetch, calls = _capped_fetcher(total=43)
    payload = _fetch_json_feed_payload("https://api/x/postings", _SPEC, fetch, 30, 1, 0.0)
    assert len(calls) == 1, calls
    assert len(_ids(payload)) == 43


def test_an_exact_multiple_does_not_request_an_empty_extra_page() -> None:
    fetch, calls = _capped_fetcher(total=200)
    payload = _fetch_json_feed_payload("https://api/x/postings", _SPEC, fetch, 30, 1, 0.0)
    assert len(_ids(payload)) == 200
    assert len(calls) == 2, calls


def test_a_failing_later_page_keeps_the_pages_already_fetched() -> None:
    """One bad page must not discard 100 good rows."""
    calls: list[str] = []

    def fetch(url: str, timeout: int) -> str:
        calls.append(url)
        if "offset=" in url:
            return "not json"
        return _feed(total=250, page_size=100, offset=0)

    payload = _fetch_json_feed_payload("https://api/x/postings", _SPEC, fetch, 30, 0, 0.0)
    assert len(_ids(payload)) == 100


def test_the_bare_endpoint_is_requested_first() -> None:
    """The conditional-revalidate path is keyed on that exact URL."""
    fetch, calls = _capped_fetcher(total=250)
    _fetch_json_feed_payload("https://api/x/postings", _SPEC, fetch, 30, 1, 0.0)
    assert calls[0] == "https://api/x/postings", calls


def _single_request_fetcher(spec: Any, calls: list[str]) -> Any:
    def fetch(url: str, timeout: int) -> str:
        calls.append(url)
        body: Any = [{"id": "1"}] if spec.list_payload else {"jobs": [{"id": "1"}]}
        return json.dumps(body)

    return fetch


def test_feeds_without_pagination_are_untouched() -> None:
    for name in ("lever", "workable", "recruitee", "pinpoint"):
        spec = JSON_FEED_SPECS[name]
        assert spec.max_pages == 1, name
        assert not spec.page_offset_param, name
        calls: list[str] = []
        payload = _fetch_json_feed_payload(
            "https://api/x", spec, _single_request_fetcher(spec, calls), 30, 1, 0.0
        )
        assert len(calls) == 1, name
        assert payload, name


def test_page_size_is_bounded() -> None:
    """Ubisoft is 332 today; a runaway board must not become unbounded requests."""
    assert _SPEC.max_pages <= 20
    assert _SPEC.page_size == 100


def test_query_parameters_respect_an_existing_query_string() -> None:
    assert _with_query("https://a/x", limit=100) == "https://a/x?limit=100"
    assert _with_query("https://a/x?mode=json", limit=100) == "https://a/x?mode=json&limit=100"


def test_list_payloads_merge_as_lists() -> None:
    spec = JSON_FEED_SPECS["lever"]
    merged = _merge_json_feed_page([{"id": "1"}], [{"id": "2"}], spec)
    assert merged == [{"id": "1"}, {"id": "2"}]


def test_the_endpoint_still_builds_from_company_id() -> None:
    url = _build_json_feed_url({"company_id": "ubisoft2"}, _SPEC)
    assert url == "https://api.smartrecruiters.com/v1/companies/ubisoft2/postings"
