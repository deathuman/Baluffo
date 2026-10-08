"""An Ashby board must never read as an empty board.

Ashby serves `jobs.ashbyhq.com/<slug>` as a client-rendered page with zero
`/job/` anchors in the server response, while the posting API at
`api.ashbyhq.com/posting-api/job-board/<slug>` returns every posting. Measured
live on 2026-10-08: voodoo, thatgamecompany and supercell all served **0**
detail links and 122, 40 and 32 jobs respectively from the API.

That gap is not cosmetic. Two independent readers were counting the rendered
page, and both read it wrong:

- the discovery probe counted anchors, reported `zero_jobs` three times, and
  quarantined Voodoo's board as empty -- so the board was never registered and
  its 122 openings, including job `13968523-e0f2-4cdb-81a1-4ac338bd5e0a`, never
  reached the feed on any release since v0.3.007;
- the fetch adapter parsed the same page, so registered boards such as supercell
  kept 0 against a board holding 32.

So the property worth pinning is that both readers resolve the same posting-API
URL from whatever shape a row carries. If they drift apart again, one of them
reports a live board as empty, and a recorded zero is indistinguishable from a
board that really has no jobs.
"""

from __future__ import annotations

import json
from typing import Any

from src.ashby_board_urls import (
    ashby_board_slug,
    ashby_posting_api_url,
    ashby_source_api_url,
)
from src.jobs.adapters.parsers.json_payloads import parse_ashby_jobs_from_payload
from src.jobs.adapters.plugins.provider_api.json_feed import (
    JSON_FEED_SPECS,
    _build_json_feed_url,
)
from src.source_discovery.probe import fallback_probe_urls, parse_probe_count
from src.source_discovery.provider_inference import _ashby_candidate

_TARGET = "13968523-e0f2-4cdb-81a1-4ac338bd5e0a"
_BOARD = "https://jobs.ashbyhq.com/voodoo"
_API = "https://api.ashbyhq.com/posting-api/job-board/voodoo"


def _payload(*jobs: dict[str, Any]) -> str:
    return json.dumps({"apiVersion": "1", "jobs": list(jobs)})


def _listing_job(**overrides: Any) -> dict[str, Any]:
    job = {
        "id": _TARGET,
        "title": "Technical Artist (AI) - Portfolio Games Team",
        "location": "Paris",
        "secondaryLocations": [{"location": "Warsaw"}],
        "employmentType": "FullTime",
        "isListed": True,
        "publishedAt": "2026-10-01T15:30:49.366+00:00",
        "jobUrl": f"{_BOARD}/{_TARGET}",
        "department": "Gaming",
    }
    job.update(overrides)
    return job


def test_the_board_page_shape_resolves_to_the_posting_api() -> None:
    for value in (_BOARD, f"{_BOARD}/jobs", f"{_BOARD}/jobs/", _API, "voodoo"):
        assert ashby_board_slug(value) == "voodoo", value
        assert ashby_posting_api_url(value) == _API, value


def test_an_unresolvable_value_yields_no_url() -> None:
    """An empty URL must stay empty; inventing one would probe a wrong host."""
    for value in ("", None, "https://jobs.ashbyhq.com/", "https://careers.example.com/jobs"):
        assert ashby_posting_api_url(value) == "", value


def test_a_row_carrying_only_a_board_url_is_still_readable() -> None:
    """The shape the live registry actually holds: `ashby:board_url:` and no api_url."""
    assert ashby_source_api_url({"board_url": _BOARD}) == _API


def test_a_row_keeps_an_api_url_it_already_carries() -> None:
    assert ashby_source_api_url({"api_url": _API, "board_url": _BOARD}) == _API
    assert ashby_source_api_url({}) == ""


def test_the_fetch_url_is_the_posting_api_not_the_board_page() -> None:
    spec = JSON_FEED_SPECS["ashby"]
    assert _build_json_feed_url({"board_url": _BOARD}, spec) == _API
    assert _build_json_feed_url({"api_url": _API}, spec) == _API
    assert _build_json_feed_url({"name": "Voodoo (Ashby)"}, spec) == ""


def test_the_probe_reads_the_api_so_a_live_board_is_not_empty() -> None:
    """The defect itself: the probe must ask the API, not the rendered page."""
    urls = fallback_probe_urls({"adapter": "ashby", "board_url": _BOARD})
    assert urls == [_API], urls


def test_a_candidate_carries_the_api_url_it_will_be_probed_and_fetched_by() -> None:
    candidate = _ashby_candidate(
        {},
        None,  # type: ignore[arg-type]
        "jobs.ashbyhq.com",
        "/voodoo",
        "Voodoo",
    )
    assert candidate is not None
    assert candidate["board_url"] == _BOARD
    assert candidate["api_url"] == _API


def test_the_api_payload_becomes_rows_and_the_target_job_is_among_them() -> None:
    jobs = parse_ashby_jobs_from_payload(json.loads(_payload(_listing_job())), _BOARD, "Voodoo")
    assert len(jobs) == 1
    row = jobs[0]
    assert row["sourceJobId"] == f"ashby:{_TARGET}"
    assert row["jobLink"] == f"{_BOARD}/{_TARGET}"
    assert row["company"] == "Voodoo"
    assert row["contractType"] == "Full Time"
    assert row["city"] == "Paris"
    assert row["postedAt"] == "2026-10-01T15:30:49.366+00:00"


def test_the_probe_counts_the_api_payload_it_now_fetches() -> None:
    """End of the chain: the JSON the probe requests is counted, not skipped."""
    body = _payload(_listing_job(), _listing_job(id="other", title="Gameplay Engineer"))
    assert parse_probe_count("ashby", body, base_url=_API) == 2


def test_an_unlisted_posting_is_not_published() -> None:
    jobs = parse_ashby_jobs_from_payload(
        json.loads(_payload(_listing_job(isListed=False))), _BOARD, "Voodoo"
    )
    assert jobs == []


def test_a_row_without_a_job_url_still_resolves_a_link() -> None:
    jobs = parse_ashby_jobs_from_payload(
        json.loads(_payload(_listing_job(jobUrl=""))), _BOARD, "Voodoo"
    )
    assert jobs[0]["jobLink"] == f"{_BOARD}/{_TARGET}"


def test_a_malformed_payload_is_empty_rather_than_an_error() -> None:
    for body in ('{"jobs": null}', "[]", "null", '{"jobs": [{"title": "No id"}]}'):
        assert parse_ashby_jobs_from_payload(json.loads(body), _BOARD, "Voodoo") == [], body
