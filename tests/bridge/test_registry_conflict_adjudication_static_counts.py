from src.bridge import registry_conflict_adjudication
from src.bridge.registry_conflict_adjudication import _parse_jobs, _probe_row
from src.bridge.source_probe_evidence import ProbeFetchResponse, probe_source_evidence


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

    def fake_fetch(url: str, _timeout_s: int, **_kwargs) -> ProbeFetchResponse:
        captured_urls.append(url)
        return ProbeFetchResponse(200, url, payload)

    monkeypatch.setattr(
        registry_conflict_adjudication,
        "probe_source_evidence",
        lambda row, timeout_s, **_kwargs: probe_source_evidence(row, timeout_s, fetcher=fake_fetch),
    )

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


def test_conflict_adjudication_greenhouse_open_application_board_is_not_positive(
    monkeypatch,
) -> None:
    payload = """
    {
      "jobs": [
        {
          "id": 4345814007,
          "title": "Open Applications",
          "absolute_url": "https://job-boards.greenhouse.io/azragamesoa/jobs/4345814007"
        }
      ]
    }
    """

    monkeypatch.setattr(
        registry_conflict_adjudication,
        "probe_source_evidence",
        lambda row, timeout_s, **_kwargs: probe_source_evidence(
            row,
            timeout_s,
            fetcher=lambda url, _timeout_s, **_fetch_kwargs: ProbeFetchResponse(200, url, payload),
        ),
    )

    probe = _probe_row(
        {
            "id": "greenhouse:slug:azragamesoa",
            "adapter": "greenhouse",
            "slug": "azragamesoa",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/azragamesoa/jobs?content=true",
            "name": "Azra Games (Greenhouse)",
        },
        5,
    )

    assert probe["ok"] is True
    assert probe["jobsFound"] == 0
    assert probe["sampleJobs"] == []
    assert probe["_jobIds"] == []


def test_conflict_adjudication_smartrecruiters_count_uses_provider_total(
    monkeypatch,
) -> None:
    payload = """
    {
      "totalFound": 2,
      "content": [
        {
          "id": "744000000000001",
          "name": "Senior Technical Artist",
          "location": {"city": "Warsaw", "country": "PL"}
        },
        {
          "id": "744000000000002",
          "name": "Accountant",
          "location": {"city": "Warsaw", "country": "PL"}
        }
      ]
    }
    """

    def fake_fetch(url: str, _timeout_s: int, **_kwargs) -> ProbeFetchResponse:
        return ProbeFetchResponse(200, url, payload)

    monkeypatch.setattr(
        registry_conflict_adjudication,
        "probe_source_evidence",
        lambda row, timeout_s, **_kwargs: probe_source_evidence(row, timeout_s, fetcher=fake_fetch),
    )

    probe = _probe_row(
        {
            "id": "smartrecruiters:company_id:peoplecanfly",
            "adapter": "smartrecruiters",
            "name": "People can Fly Studio (SmartRecruiters)",
        },
        5,
    )

    assert probe["ok"] is True
    assert probe["jobsFound"] == 2
    assert len(probe["sampleJobs"]) == 1
    assert probe["sampleJobs"][0]["title"] == "Senior Technical Artist"


def test_conflict_adjudication_jazzhr_probe_reconstructs_compact_board_url(
    monkeypatch,
) -> None:
    payload = """
    <html>
      <body>
        <a href="https://nextlevelgames.applytojob.com/apply/ABC123/IT-Manager">
          IT Manager
        </a>
        <a href="https://nextlevelgames.applytojob.com/apply/DEF456/UI-Artist">
          UI Artist
        </a>
      </body>
    </html>
    """

    captured_urls: list[str] = []

    def fake_fetch(url: str, _timeout_s: int, **_kwargs) -> ProbeFetchResponse:
        captured_urls.append(url)
        return ProbeFetchResponse(200, url, payload)

    monkeypatch.setattr(
        registry_conflict_adjudication,
        "probe_source_evidence",
        lambda row, timeout_s, **_kwargs: probe_source_evidence(row, timeout_s, fetcher=fake_fetch),
    )

    probe = _probe_row(
        {
            "id": "jazzhr:board_url:https://nextlevelgames.applytojob.com/apply",
            "adapter": "jazzhr",
            "name": "Next Level Games (JazzHR)",
        },
        5,
    )

    assert captured_urls == ["https://nextlevelgames.applytojob.com/apply"]
    assert probe["ok"] is True
    assert probe["jobsFound"] == 2
    assert probe["finalUrl"] == "https://nextlevelgames.applytojob.com/apply"


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
        "probe_source_evidence",
        lambda row, timeout_s, **kwargs: probe_source_evidence(
            row,
            timeout_s,
            fetcher=lambda *_args, **_kwargs: ProbeFetchResponse(
                200, "https://studio.example/work-with-us", raw_html
            ),
            try_playwright=kwargs.get("try_playwright"),
        ),
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


def test_conflict_adjudication_static_probe_counts_embedded_lever_board(
    monkeypatch,
) -> None:
    page_html = """
    <h1>Open Positions</h1>
    <ul class="list"></ul>
    <div id="lever-no-results" style="display: none;">No results</div>
    <script>window.leverJobsOptions = {accountName: 'skyboxlabs'};</script>
    """
    lever_payload = """
    [
      {
        "id": "abc123",
        "text": "Senior Gameplay Programmer",
        "hostedUrl": "https://jobs.lever.co/skyboxlabs/abc123",
        "categories": {"team": "Engineering", "location": "Remote"}
      }
    ]
    """

    def fake_fetch(url: str, _timeout_s: int, **_kwargs) -> ProbeFetchResponse:
        if "api.lever.co" in url:
            return ProbeFetchResponse(200, url, lever_payload)
        return ProbeFetchResponse(200, url, page_html)

    monkeypatch.setattr(
        registry_conflict_adjudication,
        "probe_source_evidence",
        lambda row, timeout_s, **_kwargs: probe_source_evidence(
            row,
            timeout_s,
            fetcher=fake_fetch,
        ),
    )

    probe = _probe_row(
        {
            "id": "static:listing_url:https://skyboxlabs.com/jobs/",
            "adapter": "static",
            "listing_url": "https://skyboxlabs.com/jobs/",
            "name": "SkyBox Labs (GameDevMap)",
        },
        5,
    )

    assert probe["ok"] is True
    assert probe["adapter"] == "static"
    assert probe["jobsFound"] == 1
    assert probe["countReason"] == "provider_embed:lever"
    assert probe["sampleJobs"][0]["title"] == "Senior Gameplay Programmer"
    assert probe["_jobIds"] == ["lever:skyboxlabs:abc123"]


def test_conflict_adjudication_static_probe_counts_ubisoft_algolia_search(
    monkeypatch,
) -> None:
    page_html = """
    <section id="jobsSearch"></section>
    <script>
      window.__config = {
        "AlgoliaAppId": "AVCVYSEJS1",
        "AlgoliaApiKey": "d2ec5782c4eb549092cfa4ed5062599a"
      };
    </script>
    """
    algolia_payload = """
    {
      "nbHits": 136,
      "hits": [
        {
          "objectID": "job-1",
          "title": "Gameplay Programmer",
          "city": "Montreal",
          "countryCode": "ca",
          "link": "https://www.ubisoft.com/en-us/company/careers/search/744-job"
        }
      ]
    }
    """

    def fake_fetch(url: str, _timeout_s: int, **_kwargs) -> ProbeFetchResponse:
        if "algolia.net" in url:
            return ProbeFetchResponse(200, url, algolia_payload)
        return ProbeFetchResponse(200, url, page_html)

    monkeypatch.setattr(
        registry_conflict_adjudication,
        "probe_source_evidence",
        lambda row, timeout_s, **_kwargs: probe_source_evidence(
            row,
            timeout_s,
            fetcher=fake_fetch,
        ),
    )

    probe = _probe_row(
        {
            "id": "static:listing_url:https://www.ubisoft.com/en-us/company/careers/search",
            "adapter": "static",
            "listing_url": "https://www.ubisoft.com/en-us/company/careers/search",
            "name": "Ubisoft (Sheet)",
        },
        5,
    )

    assert probe["ok"] is True
    assert probe["adapter"] == "static"
    assert probe["jobsFound"] == 136
    assert probe["countReason"] == "provider_embed:ubisoft_algolia"
    assert probe["sampleJobs"][0]["title"] == "Gameplay Programmer"
    assert probe["_jobIds"] == ["ubisoft_algolia:job-1"]
