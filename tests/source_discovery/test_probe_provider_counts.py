"""Auto-approval needs a job count, so the probe must be able to see one.

Discovery's probe fetches `endpoint_url(candidate)`, which prefers `api_url` over
`feed_url`, `board_url` and `listing_url`, and auto-approval requires a positive
`jobsFound`. Two defects combined to make that impossible for most boards, and both
were found by draining discovery rather than by reading code.

**Rows carried no `api_url`.** A curated row with only a `board_url` sends the probe
at the human-facing page, which for a single-page app ships no job links. The board
then reads as healthy with zero jobs, the gate believes the second number, and the row
sits in pending forever. Measured across the curated set: the only two adapters that
carried an `api_url` — smartrecruiters and recruitee — were the only provider adapters
where every board landed, while greenhouse, lever, ashby, workday, bamboohr and breezy
landed none, despite APIs returning 18-122 rows on a plain GET.

**Ashby and Breezy were missing from the probe's provider specs.** With no spec, the
count fell through to a branch that only ever matches HTML anchors, so a board whose
`api_url` returns JSON probed as zero. Ashby landed 0 of 39 while Greenhouse landed
40 of 44.

Together these moved delivered openings from 762 to 1,777 of 4,356 — 17% to 40%.
"""

from __future__ import annotations

from src.source_discovery.config import load_curated_coverage_boards
from src.source_discovery.io_runtime import endpoint_url
from src.source_discovery.probe import parse_probe_count

# Adapters whose curated rows must carry an api_url, because the probe reads
# endpoint_url() first and their board page renders through JavaScript.
_REQUIRES_API_URL = {"greenhouse", "lever", "ashby", "breezy"}


def test_a_row_with_only_a_board_url_probes_the_human_page_not_the_api() -> None:
    """This is the mechanism the api_url field exists to defeat."""
    assert endpoint_url(
        {"api_url": "https://api.example/x", "board_url": "https://b.example/"}
    ) == ("https://api.example/x")
    assert endpoint_url({"board_url": "https://b.example/"}) == "https://b.example/"


def test_every_api_backed_adaptable_row_carries_an_api_url() -> None:
    """A row missing its api_url silently returns to the pending-forever state."""
    missing = [
        row["studio"]
        for row in load_curated_coverage_boards()
        if str(row["adapter"]) in _REQUIRES_API_URL and not row.get("api_url")
    ]
    assert not missing, f"rows that would probe a JavaScript shell: {missing[:6]}"


def test_api_backed_rows_prefer_the_api_over_the_board_page() -> None:
    for row in load_curated_coverage_boards():
        if str(row["adapter"]) not in _REQUIRES_API_URL:
            continue
        assert endpoint_url(row) == row["api_url"], row["studio"]


def test_ashby_json_counts_rows() -> None:
    """Ashby returns {"jobs": [...]} and had no provider spec, so it counted zero."""
    payload = '{"jobs":[{"title":"a","id":1},{"title":"b","id":2},{"title":"c","id":3}]}'
    assert parse_probe_count("ashby", payload) == 3


def test_breezy_json_counts_rows() -> None:
    """Breezy's /json returns a top-level array, with no envelope key."""
    assert parse_probe_count("breezy", '[{"id":1},{"id":2},{"id":3}]') == 3


def test_an_empty_ashby_board_counts_zero_rather_than_falling_through() -> None:
    assert parse_probe_count("ashby", '{"jobs":[]}') == 0


def test_adding_a_spec_does_not_disturb_the_existing_ones() -> None:
    """Guard against the shared table being edited carelessly."""
    assert parse_probe_count("lever", '[{"text":"a"},{"text":"b"}]') == 2
    assert parse_probe_count("recruitee", '{"offers":[{"id":1},{"id":2}]}') == 2
    assert parse_probe_count("workable", '{"jobs":[{"id":1},{"id":2}]}') == 2
    assert parse_probe_count("smartrecruiters", '{"totalFound":9,"content":[]}') == 9


def test_workday_rows_still_carry_no_api_url() -> None:
    """Workday's CXS endpoint is a POST needing a certifi-anchored context.

    A GET api_url would not fix it, and pretending otherwise would move the failure
    from "no api_url" to "api_url returns nothing" without improving anything.
    """
    workday = [r for r in load_curated_coverage_boards() if r["adapter"] == "workday"]
    assert workday, "workday rows are expected in the catalogue"
    assert all(not row.get("api_url") for row in workday)
