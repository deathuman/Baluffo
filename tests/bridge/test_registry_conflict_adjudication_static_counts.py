from src.bridge import registry_conflict_adjudication
from src.bridge.registry_conflict_adjudication import _parse_jobs, _probe_row


def test_conflict_adjudication_provider_probe_reconstructs_compact_source_id(
    monkeypatch,
) -> None:
    payload = """
    {
      "content": [
        {
          "id": "744000000000001",
          "name": "Gameplay Designer",
          "location": {"city": "Warsaw", "country": "PL"},
          "department": "Game Design",
          "releasedDate": "2026-05-01T00:00:00.000Z"
        }
      ]
    }
    """

    captured_urls: list[str] = []

    def fake_fetch(url: str, _timeout_s: int) -> tuple[int, str, str, str]:
        captured_urls.append(url)
        return 200, url, payload, ""

    monkeypatch.setattr(registry_conflict_adjudication, "_fetch_url", fake_fetch)

    probe = _probe_row(
        {
            "id": "smartrecruiters:company_id:cdprojektred",
            "adapter": "smartrecruiters",
            "name": "CD PROJEKT RED (SmartRecruiters)",
        },
        5,
    )

    assert captured_urls == ["https://api.smartrecruiters.com/v1/companies/cdprojektred/postings"]
    assert probe["ok"] is True
    assert probe["jobsFound"] == 1


def test_conflict_adjudication_static_probe_counts_same_listing_detail_pages() -> None:
    html = """
    <a href="/work-with-us/4023614009/">Systems Engineer</a>
    <a href="/work-with-us/4023591009/">Lead Environment Artist</a>
    <a href="/work-with-us/94b98a86-d14e-49e5-b117-5b40bce17c9d/">HR Business Partner</a>
    <a href="/work-with-us/#benefits">Work settings</a>
    """

    valid, jobs = _parse_jobs(
        {
            "id": "static:listing_url:https://studio.example/work-with-us",
            "adapter": "static",
            "listing_url": "https://studio.example/work-with-us",
            "name": "Studio",
        },
        html,
        "https://studio.example/work-with-us",
    )

    assert valid is True
    assert len(jobs) == 3


def test_conflict_adjudication_static_probe_uses_browser_fallback_for_rendered_jobs(
    monkeypatch,
) -> None:
    raw_html = "<main><h1>Work with us</h1><div id='app'></div></main>"
    rendered_html = """
    <a href="/work-with-us/4023614009/">Systems Engineer</a>
    <a href="/work-with-us/4023591009/">Lead Environment Artist</a>
    """

    monkeypatch.setattr(
        registry_conflict_adjudication,
        "_fetch_url",
        lambda *_args: (200, "https://studio.example/work-with-us", raw_html, ""),
    )
    monkeypatch.setattr(
        registry_conflict_adjudication,
        "try_fetch_with_playwright",
        lambda *_args: (rendered_html, ""),
    )

    probe = _probe_row(
        {
            "id": "static:listing_url:https://studio.example/work-with-us",
            "adapter": "static",
            "listing_url": "https://studio.example/work-with-us",
            "name": "Studio",
        },
        5,
    )

    assert probe["ok"] is True
    assert probe["jobsFound"] == 2
