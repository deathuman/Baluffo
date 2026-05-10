import html
import json
from dataclasses import dataclass

import pytest

from src.source_registry import source_identity
from tests.helpers.bridge_api import build_admin_bridge_api


@dataclass(frozen=True)
class _LinkPatternCase:
    name: str
    source_url: str
    responses: dict[str, str]
    expected_jobs_found: int | None = None
    minimum_jobs_found: int | None = None


def _run_link_pattern_case(case: _LinkPatternCase, monkeypatch) -> None:
    api = build_admin_bridge_api()
    added = api.add_manual_source(case.source_url)
    source_id = str(added.get("sourceId") or "")
    assert source_id

    def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
        return case.responses.get(url, "<html></html>")

    monkeypatch.setattr("src.admin_bridge.discovery.fetch_text_with_retry", fake_fetch)
    result = api.trigger_source_check(source_id)
    assert result["started"]
    assert result["ok"]
    assert bool(result.get("weakSignal"))
    jobs_found = int(result["jobsFound"])
    if case.expected_jobs_found is not None:
        assert jobs_found == case.expected_jobs_found
    if case.minimum_jobs_found is not None:
        assert jobs_found >= case.minimum_jobs_found


LINK_PATTERN_CASES = [
    pytest.param(
        _LinkPatternCase(
            name="careers",
            source_url="https://www.naconstudiomilan.com/careers/",
            responses={
                "https://www.naconstudiomilan.com/careers": """
                <a href="/careers/">Careers</a>
                <a href="/careers-category/design/">Design category</a>
                <a href="/careers/gameplay-designer/">Gameplay Designer</a>
                <a href="/careers/gameplay-programmer/">Gameplay Programmer</a>
                <a href="/careers/ai-programmer/">AI Programmer</a>
                """,
            },
            expected_jobs_found=3,
        ),
        id="careers",
    ),
    pytest.param(
        _LinkPatternCase(
            name="join",
            source_url="https://www.guerrilla-games.com/join",
            responses={
                "https://www.guerrilla-games.com/join": """
                <a href="/join/senior-technical-animator/5778235004">Senior Technical Animator</a>
                <a href="/join?page=2#postings">Pager</a>
                """,
            },
            expected_jobs_found=1,
        ),
        id="join",
    ),
    pytest.param(
        _LinkPatternCase(
            name="open-positions",
            source_url="https://www.rovio.com/careers/",
            responses={
                "https://www.rovio.com/careers": '<a href="/open-positions/">Open Positions</a>',
                "https://www.rovio.com/open-positions": '<a href="/open-positions/game-developer-abc/">Game Developer</a>',
            },
            minimum_jobs_found=1,
        ),
        id="open-positions",
    ),
    pytest.param(
        _LinkPatternCase(
            name="job-offers",
            source_url="https://techland.net/job-offers",
            responses={
                "https://techland.net/job-offers": '<a href="/job-offers/senior-engine-programmer">Senior Engine Programmer</a>',
            },
            minimum_jobs_found=1,
        ),
        id="job-offers",
    ),
    pytest.param(
        _LinkPatternCase(
            name="vacancy",
            source_url="https://www.playground-games.com/careers",
            responses={
                "https://www.playground-games.com/careers": '<a href="/vacancy/25">Senior Animator</a>',
            },
            expected_jobs_found=1,
        ),
        id="vacancy",
    ),
    pytest.param(
        _LinkPatternCase(
            name="vacancies",
            source_url="https://careers.sega.co.uk/vacancies",
            responses={
                "https://careers.sega.co.uk/vacancies": """
                <a href="/vacancies">Vacancies</a>
                <a href="/vacancies/lead-environment-artist">Lead Environment Artist</a>
                """,
            },
            minimum_jobs_found=1,
        ),
        id="vacancies",
    ),
]


from collections.abc import Callable


@dataclass(frozen=True)
class _SourceCheckCase:
    name: str
    source_url: str
    fetch_handler: Callable[[str], str]
    browser_result: tuple[str, str] | None = None
    redirect_candidates: tuple[str, ...] = ()
    expected_ok: bool = True
    expected_jobs_found: int | None = None
    minimum_jobs_found: int | None = None
    expected_weak_signal: bool | None = True
    expected_error_code: str | None = None
    expected_error_fragment: str | None = None
    require_suggested_urls: bool = False
    expected_suggested_urls: tuple[str, ...] = ()
    expected_browser_fallback_attempted: bool | None = None
    expected_browser_fallback_used: bool | None = None


_GENERIC_SCRAPE_LISTING_HTML = """
<a href="/jobs/engine-programmer">Engine Programmer</a>
<script type="application/ld+json">
{"@context":"https://schema.org","@type":"JobPosting","title":"Technical Artist","url":"https://milestone.it/jobs/technical-artist"}
</script>
"""
_GENERIC_SCRAPE_DETAIL_HTML = """
<script type="application/ld+json">
{"@context":"https://schema.org","@type":"JobPosting","title":"Engine Programmer","url":"https://milestone.it/jobs/engine-programmer"}
</script>
"""
_EMBEDDED_JOB_OPENINGS_HTML = """
<script>
window.__NUXT__={state:{},data:[{body:[{slice_type:"job_openings_module"}]}]}
</script>
"""
_RENDERED_404_HTML = "<html><head><title>404 Not Found - Paradox Interactive</title></head><body>missing</body></html>"
_NO_JOBS_HTML = "<html><body>No jobs</body></html>"
_INTERVIEWEB_PAGE_HTML = """
<script src="https://cezanneondemand.intervieweb.it/integration/announces_js.php?lang=en&utype=0&k=abc123&LAC=milestone&d=milestone.it&annType=published&view=list&defgroup=name&gnavenable=1&desc=1&typeView=large"></script>
"""
_INTERVIEWEB_IFRAME_HTML = """
<a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=60982&lang=en">Job A</a>
<a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=61104&lang=en">Job B</a>
"""
_TEXTUAL_APPLY_HTML = """
<html><body>
<h1>We're hiring for multiple projects</h1>
<p>Senior Gameplay Programmer</p><button>Apply now</button>
<p>Lead Technical Artist</p><button>Apply now</button>
<p>Animation Programmer</p><button>Apply now</button>
<p>QA Tester</p><button>Apply now</button>
</body></html>
"""
_CRYTEK_HTML = """
<script>
self.__next_f.push([1,"{\\\"leverInitialData\\\":{\\\"postings\\\":[{\\\"hosted_url\\\":\\\"https://jobs.lever.co/crytek/abc123\\\"}]}}"]);
</script>
<a href="/career/posting/0cb503b8-53c9-4932-b0d1-8864e75deed8">Posting</a>
"""
_RELATIVE_CAREER_HTML = """
<script>
window.__PAGE_DATA__ = {"jobs":["/careers/senior-gameplay-programmer","/careers/lead-technical-artist"]};
</script>
"""
_SMARTRECRUITERS_HTML = """
<script>
var data = {"jobs":["https://jobs.smartrecruiters.com/CDPROJEKTRED/744000112115839-environment-artist"]};
</script>
"""
_PERSONIO_LISTING_HTML = """
<script src="https://assets.cdn.personio.de/jobs/v2/min/js/jobs_list.bed3abfdd85796686e20.js"></script>
<a href="https://yager.jobs.personio.de/">Jobs board</a>
"""
_PERSONIO_SEARCH_JSON = '{"data":[{"id":1},{"id":2}]}'
_JOBYLON_LISTING_HTML = """
<div id="jobylon-jobs-widget"></div>
<script>
var jbl_company_id = 2986;
var jbl_version = 'v2';
var jbl_page_size = 30;
var el = document.createElement('script');
el.src = 'https://cdn.jobylon.com/embedder.js';
</script>
"""
_JOBYLON_EMBED_HTML = "<html><body>Jobylon widget</body></html>"
_WORKABLE_LISTING_HTML = '<a href="https://apply.workable.com/team-17-digital/">Open roles</a>'
_WORKABLE_JSON = '{"jobs":[{"id":1},{"id":2},{"id":3}]}'
_JOB_FILTER_RAW_DATA = html.escape(
    json.dumps(
        {
            "jobs": [
                {
                    "id": 3472,
                    "title": "Associate DevOps Programmer",
                    "link": "https://careers-zenimax.icims.com/jobs/3472/associate-devops-programmer/job",
                },
                {
                    "id": 3479,
                    "title": "Development QA Manager",
                    "link": "https://careers-zenimax.icims.com/jobs/3479/development-qa-manager/job",
                },
                {
                    "id": 3488,
                    "title": "Senior Gameplay Programmer",
                    "link": "https://careers-zenimax.icims.com/jobs/3488/senior-gameplay-programmer/job",
                },
            ]
        }
    ),
    quote=True,
)
_JOB_FILTER_HTML = (
    '<script src="/cdn-cgi/challenge-platform/h/g/scripts/jsd/main.js"></script>'
    f'<job-filter :raw-data="{_JOB_FILTER_RAW_DATA}"></job-filter>'
)


def _equals(expected: str) -> Callable[[str], bool]:
    return lambda value: value == expected


def _contains(expected: str) -> Callable[[str], bool]:
    return lambda value: expected in value


def _fetch_routes(
    *routes: tuple[Callable[[str], bool], str], default: str = "<html></html>"
) -> Callable[[str], str]:
    def fake_fetch(url: str) -> str:
        for matches, response in routes:
            if matches(url):
                return response
        return default

    return fake_fetch


def _fetch_raises(message: str) -> Callable[[str], str]:
    def fake_fetch(_url: str) -> str:
        raise RuntimeError(message)

    return fake_fetch


def _fetch_generic_scrape(url: str) -> str:
    if url == "https://milestone.it/careers":
        return _GENERIC_SCRAPE_LISTING_HTML
    if url == "https://milestone.it/jobs/engine-programmer":
        return _GENERIC_SCRAPE_DETAIL_HTML
    raise RuntimeError(f"unexpected URL: {url}")


def _fetch_embedded_job_openings_module(_url: str) -> str:
    return _EMBEDDED_JOB_OPENINGS_HTML


def _fetch_rendered_404_page(_url: str) -> str:
    return _RENDERED_404_HTML


def _fetch_no_jobs(_url: str) -> str:
    return _NO_JOBS_HTML


def _fetch_intervieweb(url: str) -> str:
    if url == "https://milestone.it/careers":
        return _INTERVIEWEB_PAGE_HTML
    if "module=iframeAnnunci" in url and "act1=23" in url:
        return _INTERVIEWEB_IFRAME_HTML
    raise RuntimeError(f"unexpected URL: {url}")


def _fetch_textual_apply(url: str) -> str:
    if url == "https://www.4a-games.com.mt/careers":
        return _TEXTUAL_APPLY_HTML
    raise RuntimeError(f"unexpected URL: {url}")


def _fetch_crytek_like(url: str) -> str:
    if url == "https://www.crytek.com/career":
        return _CRYTEK_HTML
    raise RuntimeError(f"unexpected URL: {url}")


def _fetch_relative_career_links(url: str) -> str:
    if url == "https://www.4a-games.com.mt/careers":
        return _RELATIVE_CAREER_HTML
    raise RuntimeError(f"unexpected URL: {url}")


def _fetch_smartrecruiters(url: str) -> str:
    if url == "https://www.cdprojektred.com/en/jobs":
        return _SMARTRECRUITERS_HTML
    raise RuntimeError(f"unexpected URL: {url}")


def _fetch_personio(url: str) -> str:
    if url == "https://yager.de/careers":
        return _PERSONIO_LISTING_HTML
    if url == "https://yager.jobs.personio.de/search.json":
        return _PERSONIO_SEARCH_JSON
    if url == "https://yager.jobs.personio.de":
        return "<html>Personio Board</html>"
    return "<html></html>"


def _fetch_jobylon(url: str) -> str:
    if url == "https://www.remedygames.com/careers":
        return _JOBYLON_LISTING_HTML
    if "cdn.jobylon.com/jobs/companies/2986/embed/v2/" in url:
        return _JOBYLON_EMBED_HTML
    return "<html></html>"


def _fetch_workable(url: str) -> str:
    if url == "https://team17.com/careers":
        return _WORKABLE_LISTING_HTML
    if url == "https://apply.workable.com/api/v1/widget/accounts/team-17-digital?details=true":
        return _WORKABLE_JSON
    return "<html></html>"


def _fetch_404_then_alternate(
    initial_url: str, alternate_url: str, alternate_html: str
) -> Callable[[str], str]:
    def fake_fetch(url: str) -> str:
        if url == initial_url:
            raise RuntimeError("HTTP Error 404: Not Found")
        if url == alternate_url:
            return alternate_html
        raise RuntimeError(f"unexpected URL: {url}")

    return fake_fetch


def _run_source_check_case(case: _SourceCheckCase, monkeypatch) -> None:
    api = build_admin_bridge_api()
    added = api.add_manual_source(case.source_url)
    source_id = str(added.get("sourceId") or "")
    assert source_id

    def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
        return case.fetch_handler(url)

    def fake_browser(*_args, **_kwargs):  # noqa: ANN001
        if case.browser_result is None:
            raise AssertionError("unexpected browser fallback")
        return case.browser_result

    monkeypatch.setattr("src.admin_bridge.discovery.fetch_text_with_retry", fake_fetch)
    monkeypatch.setattr(
        "src.admin_bridge._source_check_http.try_fetch_with_playwright",
        fake_browser,
    )
    monkeypatch.setattr(
        "src.admin_bridge._source_check_http.discover_redirect_career_candidates",
        lambda *_args, **_kwargs: list(case.redirect_candidates),
    )
    result = api.trigger_source_check(source_id)
    assert result["started"]
    if case.expected_ok:
        assert result["ok"]
    else:
        assert not result["ok"]
    if case.expected_error_code is not None:
        assert str(result.get("errorCode") or "") == case.expected_error_code
    if case.expected_error_fragment is not None:
        assert case.expected_error_fragment in str(result.get("error") or "").lower()
    if case.require_suggested_urls:
        suggested = result.get("suggestedUrls") or []
        assert suggested
        for url in case.expected_suggested_urls:
            assert url in suggested
    if case.expected_browser_fallback_attempted is not None:
        assert (
            bool(result.get("browserFallbackAttempted")) is case.expected_browser_fallback_attempted
        )
    if case.expected_browser_fallback_used is not None:
        assert bool(result.get("browserFallbackUsed")) is case.expected_browser_fallback_used
    if case.expected_weak_signal is not None:
        assert bool(result.get("weakSignal")) is case.expected_weak_signal
    if case.expected_jobs_found is not None:
        assert int(result["jobsFound"]) == case.expected_jobs_found
    if case.minimum_jobs_found is not None:
        assert int(result["jobsFound"]) >= case.minimum_jobs_found


SOURCE_CHECK_SUCCESS_CASES = [
    pytest.param(
        _SourceCheckCase(
            name="generic-scrape",
            source_url="https://milestone.it/careers/",
            fetch_handler=_fetch_generic_scrape,
            expected_jobs_found=2,
            expected_weak_signal=None,
        ),
        id="generic-scrape",
    ),
    pytest.param(
        _SourceCheckCase(
            name="embedded-job-openings-module",
            source_url="https://www.avalanchestudios.com/careers",
            fetch_handler=_fetch_embedded_job_openings_module,
            minimum_jobs_found=1,
        ),
        id="embedded-job-openings-module",
    ),
    pytest.param(
        _SourceCheckCase(
            name="job-filter-payload",
            source_url="https://jobs.zenimax.com/jobs",
            fetch_handler=lambda _url: _JOB_FILTER_HTML,
            expected_jobs_found=3,
            expected_weak_signal=False,
        ),
        id="job-filter-payload",
    ),
    pytest.param(
        _SourceCheckCase(
            name="intervieweb-links",
            source_url="https://milestone.it/careers/",
            fetch_handler=_fetch_intervieweb,
            expected_jobs_found=2,
        ),
        id="intervieweb-links",
    ),
    pytest.param(
        _SourceCheckCase(
            name="textual-apply-signals",
            source_url="https://www.4a-games.com.mt/careers",
            fetch_handler=_fetch_textual_apply,
            minimum_jobs_found=1,
        ),
        id="textual-apply-signals",
    ),
    pytest.param(
        _SourceCheckCase(
            name="crytek-like-links",
            source_url="https://www.crytek.com/career",
            fetch_handler=_fetch_crytek_like,
            minimum_jobs_found=2,
        ),
        id="crytek-like-links",
    ),
    pytest.param(
        _SourceCheckCase(
            name="relative-career-links",
            source_url="https://www.4a-games.com.mt/careers",
            fetch_handler=_fetch_relative_career_links,
            minimum_jobs_found=2,
        ),
        id="relative-career-links",
    ),
    pytest.param(
        _SourceCheckCase(
            name="smartrecruiters-url",
            source_url="https://www.cdprojektred.com/en/jobs",
            fetch_handler=_fetch_smartrecruiters,
            minimum_jobs_found=1,
        ),
        id="smartrecruiters-url",
    ),
    pytest.param(
        _SourceCheckCase(
            name="personio-search-json",
            source_url="https://yager.de/careers/",
            fetch_handler=_fetch_personio,
            minimum_jobs_found=2,
        ),
        id="personio-search-json",
    ),
    pytest.param(
        _SourceCheckCase(
            name="jobylon-embed",
            source_url="https://www.remedygames.com/careers/",
            fetch_handler=_fetch_jobylon,
            minimum_jobs_found=1,
        ),
        id="jobylon-embed",
    ),
    pytest.param(
        _SourceCheckCase(
            name="workable-widget-jobs",
            source_url="https://team17.com/careers",
            fetch_handler=_fetch_workable,
            minimum_jobs_found=3,
        ),
        id="workable-widget-jobs",
    ),
    pytest.param(
        _SourceCheckCase(
            name="alternate-on-404",
            source_url="https://www.fatsharkgames.com/career",
            fetch_handler=_fetch_404_then_alternate(
                "https://www.fatsharkgames.com/career",
                "https://jobs.fatsharkgames.com",
                '<a href="https://jobs.fatsharkgames.com/jobs/senior-programmer">Role</a>',
            ),
            minimum_jobs_found=1,
        ),
        id="alternate-on-404",
        marks=pytest.mark.slow,
    ),
    pytest.param(
        _SourceCheckCase(
            name="parent-redirect-candidates-on-404",
            source_url="https://www.fatsharkgames.com/career",
            fetch_handler=_fetch_404_then_alternate(
                "https://www.fatsharkgames.com/career",
                "https://jobs.fatsharkgames.com",
                '<a href="https://jobs.fatsharkgames.com/jobs/network-programmer">Role</a>',
            ),
            redirect_candidates=("https://jobs.fatsharkgames.com",),
            minimum_jobs_found=1,
        ),
        id="parent-redirect-candidates-on-404",
    ),
]


SOURCE_CHECK_ERROR_CASES = [
    pytest.param(
        _SourceCheckCase(
            name="no-jobs",
            source_url="https://milestone.it/careers/",
            fetch_handler=_fetch_no_jobs,
            expected_ok=False,
            expected_weak_signal=None,
            expected_error_fragment="no job postings found",
        ),
        id="no-jobs",
    ),
    pytest.param(
        _SourceCheckCase(
            name="rendered-404-page",
            source_url="https://www.paradoxinteractive.com/careers",
            fetch_handler=_fetch_rendered_404_page,
            expected_ok=False,
            expected_weak_signal=None,
            expected_error_code="not_found",
            require_suggested_urls=True,
        ),
        id="rendered-404-page",
    ),
    pytest.param(
        _SourceCheckCase(
            name="404-hints",
            source_url="https://www.king.com/careers",
            fetch_handler=_fetch_404_then_alternate(
                "https://www.king.com/careers",
                "https://www.king.com/careers",
                "<html></html>",
            ),
            expected_ok=False,
            expected_weak_signal=None,
            expected_error_code="not_found",
            require_suggested_urls=True,
        ),
        id="404-hints",
    ),
    pytest.param(
        _SourceCheckCase(
            name="ssl-error",
            source_url="https://careers.11bitstudios.com/",
            fetch_handler=_fetch_raises("SSL: CERTIFICATE_VERIFY_FAILED hostname mismatch"),
            expected_ok=False,
            expected_weak_signal=None,
            expected_error_code="ssl_error",
        ),
        id="ssl-error",
    ),
]


SOURCE_CHECK_BROWSER_CASES = [
    pytest.param(
        _SourceCheckCase(
            name="browser-on-403",
            source_url="https://careers.rebellion.com/",
            fetch_handler=_fetch_raises("HTTP Error 403: Forbidden"),
            browser_result=(
                '<a href="/jobs/gameplay-programmer">Role</a>',
                "",
            ),
            minimum_jobs_found=1,
            expected_browser_fallback_attempted=True,
            expected_browser_fallback_used=True,
        ),
        id="browser-on-403",
    ),
    pytest.param(
        _SourceCheckCase(
            name="browser-on-challenge-page",
            source_url="https://jobs.zenimax.com/jobs",
            fetch_handler=_fetch_routes(
                (
                    _equals("https://jobs.zenimax.com/jobs"),
                    (
                        '<html><head><script src="/cdn-cgi/challenge-platform/h/g/scripts/jsd/main.js"></script></head>'
                        "<body>Just a moment...</body></html>"
                    ),
                )
            ),
            browser_result=(
                '<a href="/requisitions/view/3472">Associate DevOps Programmer</a>'
                '<a href="/requisitions/view/3479">Development QA Manager</a>',
                "",
            ),
            expected_jobs_found=2,
            expected_browser_fallback_attempted=True,
            expected_browser_fallback_used=True,
        ),
        id="browser-on-challenge-page",
    ),
    pytest.param(
        _SourceCheckCase(
            name="browser-unavailable",
            source_url="https://careers.rebellion.com/",
            fetch_handler=_fetch_raises("HTTP Error 403: Forbidden"),
            browser_result=(
                "",
                "browser fallback unavailable (playwright is not installed)",
            ),
            expected_ok=False,
            expected_weak_signal=None,
            expected_error_code="browser_fallback_unavailable",
            expected_browser_fallback_attempted=True,
            expected_browser_fallback_used=False,
        ),
        id="browser-unavailable",
    ),
]


@pytest.mark.parametrize("case", SOURCE_CHECK_SUCCESS_CASES, ids=lambda case: case.name)
def test_trigger_source_check_static_fallback_handles_success_cases(
    case: _SourceCheckCase,
    admin_bridge_entrypoint_root,
    monkeypatch,
) -> None:
    _run_source_check_case(case, monkeypatch)


@pytest.mark.parametrize("case", SOURCE_CHECK_ERROR_CASES, ids=lambda case: case.name)
def test_trigger_source_check_static_fallback_handles_error_cases(
    case: _SourceCheckCase,
    admin_bridge_entrypoint_root,
    monkeypatch,
) -> None:
    _run_source_check_case(case, monkeypatch)


@pytest.mark.parametrize("case", SOURCE_CHECK_BROWSER_CASES, ids=lambda case: case.name)
def test_trigger_source_check_static_fallback_handles_browser_cases(
    case: _SourceCheckCase,
    admin_bridge_entrypoint_root,
    monkeypatch,
) -> None:
    _run_source_check_case(case, monkeypatch)


@pytest.mark.parametrize("case", LINK_PATTERN_CASES, ids=lambda case: case.name)
def test_trigger_source_check_static_fallback_accepts_link_patterns(
    case: _LinkPatternCase,
    admin_bridge_entrypoint_root,
    monkeypatch,
) -> None:
    _run_link_pattern_case(case, monkeypatch)


def test_trigger_source_check_static_normalizes_placeholder_studio_name(
    admin_bridge_entrypoint_root,
    monkeypatch,
):
    api = build_admin_bridge_api()
    pending_row = {
        "name": "Www (Manual Website)",
        "studio": "Www",
        "company": "Www",
        "adapter": "static",
        "pages": ["https://www.naconstudiomilan.com/careers/"],
        "listing_url": "https://www.naconstudiomilan.com/careers/",
        "enabledByDefault": False,
        "id": "static:listing_url:https://www.naconstudiomilan.com/careers",
    }
    api.persist_state_and_auto_sync(
        {"active": [], "pending": [pending_row], "rejected": []},
        reason="unit_test_seed",
    )
    source_id = str(pending_row["id"])

    listing_html = '<a href="/careers/gameplay-designer/">Gameplay Designer</a>'
    monkeypatch.setattr(
        "src.admin_bridge.discovery.fetch_text_with_retry",
        lambda *_args, **_kwargs: listing_html,
    )
    result = api.trigger_source_check(source_id)
    assert result["started"]
    assert result["ok"]
    pending = api.load_state()["pending"]
    updated = next((row for row in pending if source_identity(row) == source_id), {})
    assert str(updated.get("studio") or "") == "Nacon Studio Milan"
