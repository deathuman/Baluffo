"""Rows from the JSON a page's own app fetched while rendering.

A single-page app's openings are not in its markup. Feishu's board answers
``POST /api/v1/search/job_post/count``; the rendered page holds no job links, which is why 826
recorded openings sat unreachable and why registering those boards as static rows would have
been a false green -- the delivery metric would have called them landed while they produced
nothing.

The browser already fetches that response, so the lane reads it. These tests pin the three
things that make that honest: the posting-shape guard is the *same* one the embedded lane uses
(the payload is a different container, not a different rule), junk payloads are not mistaken for
listings, and a capture that yields nothing is not an error.
"""

from __future__ import annotations

from src.jobs.adapters.plugins.static.rendered_json import (
    describe_capture,
    extract_rendered_json_rows,
    payload_is_worth_walking,
)

# The shape Feishu's count endpoint answers: a total plus posting objects under an envelope.
FEISHU_LIKE = """
{"total": 2, "job_post_list": [
  {"id": "abc", "title": "Gameplay Engineer", "job_post_url": "https://kurogame.jobs.feishu.cn/position/abc/detail",
   "city": "Tokyo", "department": "Engineering", "job_type": "Full-time"},
  {"id": "def", "title": "Level Designer", "job_post_url": "https://kurogame.jobs.feishu.cn/position/def/detail",
   "city": "Osaka"}
]}
"""

GREENHOUSE_LIKE = """
{"jobs": [{"id": 1, "title": "Senior Game Designer", "absolute_url": "https://job-boards.greenhouse.io/studio/jobs/1",
           "location": {"name": "Berlin, DE"}, "company_name": "Studio"}], "meta": {"total": 1}}
"""


def test_rows_come_out_of_a_listings_payload() -> None:
    rows = extract_rendered_json_rows(
        [("https://kurogame.jobs.feishu.cn/api/v1/search/job_post/count", FEISHU_LIKE)],
        board_url="https://kurogame.jobs.feishu.cn/index",
        fallback_company="Kuro Game",
    )
    titles = sorted(row["title"] for row in rows)
    assert titles == ["Gameplay Engineer", "Level Designer"]
    assert rows[0]["company"] == "Kuro Game"


def test_the_posting_link_is_absolute_and_fetchable() -> None:
    rows = extract_rendered_json_rows(
        [("https://studio.example/api/jobs", FEISHU_LIKE)],
        board_url="https://studio.example/index",
    )
    for row in rows:
        assert row["jobLink"].startswith("https://")
        assert row["title"]


def test_a_relative_link_is_resolved_against_the_board_url() -> None:
    payload = '{"jobs": [{"title": "Gameplay Programmer", "url": "/jobs/1"}]}'
    rows = extract_rendered_json_rows(
        [("https://studio.example/api/jobs", payload)],
        board_url="https://studio.example/careers",
    )
    assert rows[0]["jobLink"] == "https://studio.example/jobs/1"


def test_the_same_shape_rule_as_the_embedded_lane_applies() -> None:
    """CMS assets and nav entries carry title+url too. One rule, not two."""
    payload = """
    {"items": [
      {"title": "Hero image", "url": "https://cdn.example/hero.png", "fileName": "hero.png",
       "contentType": "image/png", "width": 1200},
      {"name": "Jobs", "url": "/jobs"},
      {"@type": "Organization", "title": "Studio", "url": "https://studio.example"}
    ]}
    """
    rows = extract_rendered_json_rows(
        [("https://studio.example/api/config", payload)],
        board_url="https://studio.example/",
    )
    assert rows == []


TENCENT_LIKE = """
{"Code": 200, "Data": {"Count": 2240, "Posts": [
  {"Id": 0, "PostId": "2079889243404156928", "RecruitPostId": 1277386805258723328,
   "RecruitPostName": "Senior Gameplay Engineer", "CountryName": "英国", "LocationName": "伦敦",
   "BGName": "IEG", "CategoryName": "技术",
   "PostURL": "https://tencent.wd1.myworkdayjobs.com/Tencent_Careers/job/United-Kingdom/London"},
  {"Id": 0, "PostId": "2079889243404156929", "RecruitPostId": 1277386805258723329,
   "RecruitPostName": "Level Designer", "CountryName": "英国", "LocationName": "伦敦",
   "PostURL": "https://tencent.wd1.myworkdayjobs.com/Tencent_Careers/job/United-Kingdom/London-2"}
]}}
"""


def test_a_platform_that_names_its_fields_differently_still_reads() -> None:
    """Tencent's `/tencentcareer/api/post/Query`: RecruitPostName / PostURL / LocationName.

    A host-agnostic lane cannot know those names ahead of time, so the key sets carry the ones
    boards have actually been measured sending. Guessing one platform's API and calling it
    general would be the per-platform parser this avoids.
    """
    rows = extract_rendered_json_rows(
        [("https://careers.tencent.com/tencentcareer/api/post/Query", TENCENT_LIKE)],
        board_url="https://careers.tencent.com/search.html",
        fallback_company="Tencent",
    )
    assert sorted(row["title"] for row in rows) == [
        "Level Designer",
        "Senior Gameplay Engineer",
    ]
    assert rows[0]["jobLink"].startswith("https://tencent.wd1.myworkdayjobs.com/")
    assert rows[0]["company"] == "Tencent"


def test_rows_from_one_board_get_distinct_ids() -> None:
    """Tencent answers `Id: 0` on every post. Accepting it would dedupe the board into one row."""
    rows = extract_rendered_json_rows(
        [("https://careers.tencent.com/tencentcareer/api/post/Query", TENCENT_LIKE)],
        board_url="https://careers.tencent.com/search.html",
    )
    ids = {row["sourceJobId"] for row in rows}
    assert len(ids) == len(rows) == 2
    assert "embedded:0" not in ids


def test_a_greenhouse_shaped_payload_works_the_same_way() -> None:
    rows = extract_rendered_json_rows(
        [("https://boards-api.greenhouse.io/v1/boards/studio/jobs", GREENHOUSE_LIKE)],
        board_url="https://studio.example/careers",
    )
    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://job-boards.greenhouse.io/studio/jobs/1"
    assert rows[0]["company"] == "Studio"
    assert rows[0]["city"] == "Berlin"


def test_the_payload_url_is_recorded_for_evidence() -> None:
    """Which endpoint a board's rows came from is what makes a zero diagnosable."""
    url = "https://studio.example/api/v1/search/job_post/count"
    rows = extract_rendered_json_rows(
        [(url, FEISHU_LIKE)], board_url="https://studio.example/index"
    )
    assert rows[0]["renderedJsonPayloadUrl"] == url


def test_a_board_with_rows_in_two_payloads_is_not_double_counted() -> None:
    rows = extract_rendered_json_rows(
        [
            ("https://studio.example/api/jobs", FEISHU_LIKE),
            ("https://studio.example/api/footer", FEISHU_LIKE),
        ],
        board_url="https://studio.example/index",
    )
    assert len(rows) == 2, "the same two postings, once each"


def test_the_first_payload_to_claim_a_link_keeps_it() -> None:
    """A footer widget repeating the board's own rows must not replace their provenance."""
    first = "https://studio.example/api/jobs"
    rows = extract_rendered_json_rows(
        [
            (first, GREENHOUSE_LIKE),
            ("https://cdn.example/widget.json", GREENHOUSE_LIKE),
        ],
        board_url="https://studio.example/careers",
    )
    assert len(rows) == 1
    assert rows[0]["renderedJsonPayloadUrl"] == first


def test_a_non_json_payload_contributes_nothing_and_is_not_an_error() -> None:
    rows = extract_rendered_json_rows(
        [("https://studio.example/api/thing", "<html>not json</html>")],
        board_url="https://studio.example/",
    )
    assert rows == []


def test_empty_and_blank_payloads_are_handled() -> None:
    assert extract_rendered_json_rows([], board_url="https://studio.example/") == []
    assert extract_rendered_json_rows([("", "")], board_url="https://studio.example/") == []
    assert (
        extract_rendered_json_rows([("https://x.test/a", "")], board_url="https://studio.example/")
        == []
    )


# --- the payload filter is a filter, not a whitelist ------------------------------------


def test_telemetry_and_config_payloads_are_skipped() -> None:
    for url in (
        "https://studio.example/analytics/collect",
        "https://studio.example/gtm.js/x",
        "https://studio.example/api/events",
        "https://studio.example/sentry/1",
    ):
        assert payload_is_worth_walking(url) is False, url


def test_a_listings_payload_on_an_ordinary_url_is_walked() -> None:
    """A board that serves openings from an unremarkable URL must not be filtered away."""
    for url in (
        "https://studio.example/api/v1/search/job_post/count",
        "https://studio.example/data",
        "https://api.ashbyhq.com/posting-api/job-board/studio",
    ):
        assert payload_is_worth_walking(url) is True, url


def test_the_capture_summary_separates_captured_from_walked() -> None:
    payloads = [
        ("https://studio.example/api/jobs", "{}"),
        ("https://studio.example/analytics/collect", "{}"),
    ]
    assert describe_capture(payloads) == "2 captured payloads, 1 walked"
    assert describe_capture([]) == ""
