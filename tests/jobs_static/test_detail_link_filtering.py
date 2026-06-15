import pytest

from ._helpers import Counter, process_detail_link, static_helpers


@pytest.mark.parametrize(
    "candidate_url",
    [
        "https://www.comeet.com/jobs/ludeo/{{company.website}}",
        "https://careers.beenox.com/us/en/cvdHrefText",
        "javascript:void(0)",
        "https://example.com/careers",
    ],
)
def test_add_detail_link_rejects_template_and_self_links_before_fetch(candidate_url: str) -> None:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    seen_links: set[str] = set()
    link_rejections: Counter[str] = Counter()

    static_helpers.add_detail_link(
        detail_links,
        detail_seen,
        seen_links,
        link_rejections,
        candidate_url=candidate_url,
        anchor_text="Senior Artist",
        enforce_heuristics=False,
        page_url="https://example.com/careers",
        source={"company": "Example"},
        default_path_tokens=[],
        default_query_keys=[],
    )

    assert detail_links == []
    assert link_rejections["dead_listing_page"] == 1


def test_process_detail_link_rejects_template_url_without_fetching() -> None:
    def fail_fetch(*args: object, **kwargs: object) -> tuple[str, bool]:
        raise AssertionError(f"detail fetch should not run: {args} {kwargs}")

    result = process_detail_link(
        detail="https://www.comeet.com/jobs/ludeo/{{company.website}}",
        detail_title="Senior Artist",
        source_started=0.0,
        static_source_time_budget_s=10,
        fetch_html_cached=fail_fetch,
        timeout_s=5,
        detail_retries=0,
        company="Example",
        source_name="Example",
        source={"company": "Example"},
        ignored_link_titles=set(),
    )

    assert result["rows"] == []
    assert result["fetchMs"] == 0
    assert result["rejectedClassification"] == "dead_listing_page"


def test_add_detail_link_accepts_elevato_comma_job_paths() -> None:
    detail_links: list[tuple[str, str]] = []
    detail_seen: set[str] = set()
    seen_links: set[str] = set()
    link_rejections: Counter[str] = Counter()

    static_helpers.add_detail_link(
        detail_links,
        detail_seen,
        seen_links,
        link_rejections,
        candidate_url="/en/technical-artist,j,240?source=10",
        anchor_text="Technical Artist",
        enforce_heuristics=True,
        page_url="https://qloc.elevato.net/en/",
        source={"company": "QLOC"},
        default_path_tokens=[],
        default_query_keys=[],
    )

    assert detail_links == [
        ("https://qloc.elevato.net/en/technical-artist,j,240", "Technical Artist")
    ]
    assert not link_rejections
