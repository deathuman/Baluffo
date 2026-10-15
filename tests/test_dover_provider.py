"""The Dover provider adapter: slug resolution, payload parsing, and the two-step runner.

Dover boards are client-rendered and serve no job anchors, so the listing only exists
behind two API requests -- a slug endpoint that yields the ``client_id`` the jobs
endpoint is keyed on. Two things are pinned here:

**The slug shapes agree with the API.** Every URL form Dover appears in (board, apply
link, both API URLs, the bare slug) resolves to one slug, and the bare root resolves to
nothing rather than to a slug invented from an empty path.

**A board that cannot be read is an error, never a zero.** A slug the API does not know,
or a response that is the SPA shell instead of JSON, must raise. A silent zero there is
indistinguishable from a board with no openings, which is exactly how a dead adapter
stays hidden for months.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from src.dover_board_urls import (
    dover_board_slug,
    dover_jobs_api_url,
    dover_slug_api_url,
    dover_source_slug_url,
)
from src.exceptions import AdapterValidationError
from src.jobs.adapters.parsers.json_payloads import parse_dover_jobs_payload
from src.jobs.adapters.plugins.provider_api import dover_runner

# The audited board, verbatim from the live API on 2026-10-14.
_SLUG = "hatsulabs"
_CLIENT_ID = "d306a454-f8d9-47b7-8d65-08914d7b5644"
_JOB_ID = "8dbe43b3-3dc5-4812-bf95-fbb306d08e59"
_BOARD_URL = f"https://app.dover.com/jobs/{_SLUG}"
_APPLY_URL = f"https://app.dover.com/apply/{_SLUG}/{_JOB_ID}"
_SLUG_URL = f"https://app.dover.com/api/v1/careers-page-slug/{_SLUG}"
_JOBS_URL = f"https://app.dover.com/api/v1/careers-page/{_CLIENT_ID}/jobs"


def _result(job_id: str, title: str, **overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": job_id,
        "title": title,
        "locations": [],
        "workplace_type": "ONSITE",
        "is_published": True,
        "is_sample": False,
    }
    row.update(overrides)
    return row


def _jobs_payload(*rows: dict[str, Any]) -> dict[str, Any]:
    return {"count": len(rows), "next": None, "previous": None, "results": list(rows)}


# --- 1. Slug resolution ----------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        _BOARD_URL,
        _SLUG_URL,
        _APPLY_URL,
        f"https://app.dover.com/jobs/{_SLUG}/",
        _SLUG,
    ],
)
def test_every_url_shape_resolves_to_the_same_slug(value: str) -> None:
    assert dover_board_slug(value) == _SLUG, value


@pytest.mark.parametrize(
    "value",
    [
        "https://app.dover.com",
        "https://app.dover.com/",
        "",
        None,
        "https://careers.example.com/jobs",
    ],
)
def test_a_url_naming_no_board_resolves_to_nothing(value: object) -> None:
    """The bare root is the Backbone row's old value: no slug, so no board."""
    assert dover_board_slug(value) == ""
    assert dover_slug_api_url(value) == ""
    assert dover_source_slug_url({"listing_url": value}) == ""


def test_urls_are_built_without_a_trailing_slash_on_jobs() -> None:
    """``.../jobs/`` is routed to the SPA shell and answers HTML, not JSON."""
    assert dover_jobs_api_url(_CLIENT_ID) == _JOBS_URL
    assert not _JOBS_URL.endswith("/")
    assert dover_slug_api_url(_BOARD_URL) == _SLUG_URL
    assert dover_jobs_api_url("") == ""


def test_a_curated_row_resolves_via_its_api_url() -> None:
    """The curated base-row shape: ``board_url`` plus the slug endpoint as ``api_url``.

    ``api_url`` wins, and it is the slug endpoint rather than a jobs URL -- the runner
    reads it first to discover the ``client_id``. Reading it as a jobs URL directly
    would send the second request nowhere.
    """
    row = {
        "name": "Hatsu Labs (dover)",
        "studio": "Hatsu Labs",
        "adapter": "dover",
        "board_url": _BOARD_URL,
        "api_url": _SLUG_URL,
    }
    assert dover_source_slug_url(row) == _SLUG_URL
    assert dover_source_slug_url({"board_url": _BOARD_URL}) == _SLUG_URL
    assert dover_source_slug_url({}) == ""


# --- 2. Parser -------------------------------------------------------------


def test_two_published_results_become_two_jobs() -> None:
    payload = _jobs_payload(
        _result(_JOB_ID, "Senior Gameplay Engineer"),
        _result("job-2", "Technical Artist", workplace_type="REMOTE"),
    )
    rows = parse_dover_jobs_payload(payload, _SLUG, "Hatsu Labs")

    assert len(rows) == 2
    assert rows[0]["sourceJobId"] == f"dover:{_JOB_ID}"
    assert rows[1]["sourceJobId"] == "dover:job-2"
    assert rows[0]["title"] == "Senior Gameplay Engineer"
    assert rows[1]["title"] == "Technical Artist"


def test_hidden_and_sample_rows_are_dropped() -> None:
    """``is_published: false`` is Dover's own hidden posting; ``is_sample`` is a demo row."""

    payload = _jobs_payload(
        _result("keep", "Gameplay Engineer"),
        _result("hidden", "Unpublished Role", is_published=False),
        _result("sample", "Sample Role", is_sample=True),
    )
    rows = parse_dover_jobs_payload(payload, _SLUG, "Hatsu Labs")

    assert [row["sourceJobId"] for row in rows] == ["dover:keep"]


def test_the_job_link_carries_the_slug_and_the_job_id() -> None:
    rows = parse_dover_jobs_payload(
        _jobs_payload(_result(_JOB_ID, "Lead Game Designer")), _SLUG, "Hatsu"
    )

    assert rows[0]["jobLink"] == _APPLY_URL
    assert _SLUG in rows[0]["jobLink"]
    assert _JOB_ID in rows[0]["jobLink"]


def test_the_link_slug_is_recovered_from_a_board_url() -> None:
    """The runner hands the parser a registry row, not a bare slug."""
    rows = parse_dover_jobs_payload(
        _jobs_payload(_result(_JOB_ID, "Lead Game Designer")), {"board_url": _BOARD_URL}, "Hatsu"
    )

    assert rows[0]["jobLink"] == _APPLY_URL


def test_the_title_is_the_api_title_and_never_the_slug() -> None:
    rows = parse_dover_jobs_payload(
        _jobs_payload(_result(_JOB_ID, "Principal Gameplay Engineer")), _SLUG, "Hatsu"
    )

    assert rows[0]["title"] == "Principal Gameplay Engineer"
    assert _SLUG not in rows[0]["title"]


@pytest.mark.parametrize(
    "locations,workplace_type,expected_city",
    [
        ([{"name": "London, United Kingdom"}], "HYBRID", "London"),
        ([{"name": "Tokyo, Japan"}, {"name": "London, United Kingdom"}], "ONSITE", "Tokyo"),
        ([], "REMOTE", "Remote"),
    ],
)
def test_location_comes_from_the_primary_entry_or_the_workplace_type(
    locations: list[dict[str, Any]], workplace_type: str, expected_city: str
) -> None:
    rows = parse_dover_jobs_payload(
        _jobs_payload(
            _result(_JOB_ID, "Game Developer", locations=locations, workplace_type=workplace_type)
        ),
        _SLUG,
        "Hatsu",
    )

    assert rows[0]["city"] == expected_city


def test_work_type_is_the_raw_dover_workplace_type() -> None:
    """Held raw ("REMOTE"), as the Ashby and Pinpoint parsers hold theirs.

    The repo has no workplace-type normaliser, so the API's own casing is the value --
    and it stays the workplace type rather than a location string even when the row
    names no location at all.
    """
    rows = parse_dover_jobs_payload(
        _jobs_payload(_result(_JOB_ID, "Game Developer", locations=[], workplace_type="REMOTE")),
        _SLUG,
        "Hatsu",
    )

    assert rows[0]["workType"] == "REMOTE"
    assert rows[0]["city"] == "Remote"


def test_a_bare_list_payload_is_accepted() -> None:
    rows = parse_dover_jobs_payload([_result("job-1", "Gameplay Engineer")], _SLUG, "Hatsu")

    assert len(rows) == 1


def test_a_payload_with_no_results_yields_no_jobs() -> None:
    assert parse_dover_jobs_payload({"count": 0, "results": []}, _SLUG, "Hatsu") == []
    assert parse_dover_jobs_payload({}, _SLUG, "Hatsu") == []


def test_rows_without_an_id_or_title_are_skipped() -> None:
    payload = _jobs_payload(
        {"title": "No Id", "is_published": True},
        _result("", "Empty Id"),
        _result("no-title", ""),
    )

    assert parse_dover_jobs_payload(payload, _SLUG, "Hatsu") == []


def test_both_of_the_audited_boards_real_titles_survive() -> None:
    """The F-4 acceptance case, with the live titles verbatim.

    "Character Lighting & Look Development Artist" carries no game keyword, so a
    wide-feed game-keyword filter drops it -- and the board reports one opening
    where the studio has two. A board-scoped parser must not apply that filter.
    """
    payload = _jobs_payload(
        _result(
            "3a138bf4-1111-2222-3333-444455556666",
            "Character Lighting & Look Development Artist",
            locations=[{"name": "International"}],
            workplace_type="REMOTE",
        ),
        _result(
            _JOB_ID,
            "Technical Artist (Shaders & Performance)",
            locations=[{"name": "International"}],
            workplace_type="REMOTE",
        ),
    )
    rows = parse_dover_jobs_payload(payload, _SLUG, "Hatsu Labs")

    assert len(rows) == 2
    assert [row["title"] for row in rows] == [
        "Character Lighting & Look Development Artist",
        "Technical Artist (Shaders & Performance)",
    ]


def test_a_role_with_no_game_keyword_is_kept() -> None:
    """The general form of the case above: one studio's own board is all its openings."""
    for title in ("Senior Lighting Artist", "Character Artist", "Cinematic Director"):
        rows = parse_dover_jobs_payload(_jobs_payload(_result("job-1", title)), _SLUG, "Hatsu")

        assert [row["title"] for row in rows] == [title], title


# --- 3. Runner -------------------------------------------------------------


@pytest.fixture()
def dover_board(monkeypatch: pytest.MonkeyPatch):
    """One registered Dover board, with diagnostics captured instead of written."""
    rows = [
        {
            "name": "Hatsu Labs (Dover)",
            "studio": "Hatsu Labs",
            "adapter": "dover",
            "board_url": _BOARD_URL,
            "api_url": _SLUG_URL,
        }
    ]
    diagnostics: dict[str, dict[str, Any]] = {}
    monkeypatch.setattr(dover_runner, "registry_entries", lambda _key: [dict(r) for r in rows])
    monkeypatch.setattr(
        dover_runner,
        "set_source_diagnostics",
        lambda name, **kwargs: diagnostics.setdefault(name, kwargs),
    )
    return diagnostics


def _run(fetch_payload, **kwargs: Any):
    return dover_runner._run_dover_sources(
        fetch_text=lambda _url, _timeout: "",
        fetch_payload=fetch_payload,
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
        **kwargs,
    )


def test_the_runner_makes_two_requests_and_keeps_the_published_rows(dover_board) -> None:
    calls: list[str] = []

    def fetch(url: str) -> Any:
        calls.append(url)
        if url == _SLUG_URL:
            return {"id": _CLIENT_ID, "slug": _SLUG, "name": "Hatsu Labs"}
        if url == _JOBS_URL:
            return _jobs_payload(
                _result(_JOB_ID, "Senior Gameplay Engineer"),
                _result("job-2", "Technical Artist"),
                _result("hidden", "Unpublished Role", is_published=False),
            )
        raise AssertionError(f"unexpected url: {url}")

    rows = _run(fetch)

    assert calls == [_SLUG_URL, _JOBS_URL]
    assert len(rows) == 2
    assert {row["adapter"] for row in rows} == {"dover"}
    assert {row["studio"] for row in rows} == {"Hatsu Labs"}
    assert {row["sourceJobId"] for row in rows} == {f"dover:{_JOB_ID}", "dover:job-2"}


def test_the_entry_report_names_the_jobs_url_and_counts_both_sides(dover_board) -> None:
    def fetch(url: str) -> Any:
        if url == _SLUG_URL:
            return {"id": _CLIENT_ID}
        return _jobs_payload(
            _result(_JOB_ID, "Senior Gameplay Engineer"),
            _result("hidden", "Unpublished Role", is_published=False),
        )

    _run(fetch)
    detail = dover_board["dover_sources"]["details"][0]

    assert detail["providerUrl"] == _JOBS_URL
    assert detail["fetchedCount"] == 2
    assert detail["keptCount"] == 1
    assert detail["status"] == "ok"
    assert detail["error"] == ""


def test_a_slug_response_without_a_client_id_is_an_error_not_a_zero(dover_board) -> None:
    """The failure mode this adapter exists to avoid: a live board read as empty."""
    with pytest.raises(AdapterValidationError):
        _run(lambda _url: {"slug": _SLUG, "name": "No Client Id Here"})


def test_an_html_response_is_an_error_not_a_zero(dover_board) -> None:
    """``.../jobs/`` with a trailing slash serves the SPA shell instead of JSON."""
    html = "<!DOCTYPE html><html><head><title>Hatsu Labs</title></head></html>"

    with pytest.raises(AdapterValidationError):
        _run(lambda _url: html)


def test_an_unreadable_board_reports_the_reason_in_its_diagnostics(dover_board) -> None:
    with pytest.raises(AdapterValidationError):
        _run(lambda _url: {"slug": _SLUG})

    detail = dover_board["dover_sources"]["details"][0]
    assert detail["status"] == "error"
    assert detail["error"]
    assert detail["keptCount"] == 0


def test_a_board_with_no_slug_is_reported_rather_than_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row whose URL names no board must read as an error, not as a quiet ok."""
    diagnostics: dict[str, dict[str, Any]] = {}
    monkeypatch.setattr(
        dover_runner,
        "registry_entries",
        lambda _key: [{"name": "Ghost", "studio": "Ghost", "listing_url": "https://app.dover.com"}],
    )
    monkeypatch.setattr(
        dover_runner,
        "set_source_diagnostics",
        lambda name, **kwargs: diagnostics.setdefault(name, kwargs),
    )

    with pytest.raises(AdapterValidationError):
        _run(lambda _url: {})

    assert "missing board_url/slug" in str(diagnostics["dover_sources"]["details"][0]["error"])


def test_a_json_body_that_is_not_json_is_decoded_as_an_error() -> None:
    with pytest.raises(AdapterValidationError):
        dover_runner._dover_payload_from_text("not json at all")
    assert dover_runner._dover_payload_from_text(json.dumps({"id": "x"})) == {"id": "x"}


def test_boards_are_fetched_concurrently_and_keep_registry_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slugs = {"alpha": "id-alpha", "beta": "id-beta", "gamma": "id-gamma"}
    monkeypatch.setattr(
        dover_runner,
        "registry_entries",
        lambda _key: [
            {
                "name": f"{slug} (Dover)",
                "studio": slug.title(),
                "board_url": f"https://app.dover.com/jobs/{slug}",
            }
            for slug in slugs
        ],
    )
    monkeypatch.setattr(dover_runner, "set_source_diagnostics", lambda *a, **k: None)

    def fetch(url: str) -> Any:
        if "/careers-page-slug/" in url:
            slug = url.rsplit("/", 1)[-1]
            return {"id": slugs[slug]}
        client_id = url.split("/careers-page/", 1)[1].split("/", 1)[0]
        return _jobs_payload(_result(f"job-{client_id}", f"Gameplay Engineer ({client_id})"))

    rows = _run(fetch)

    assert [row["studio"] for row in rows] == ["Alpha", "Beta", "Gamma"]
    assert len(rows) == 3
