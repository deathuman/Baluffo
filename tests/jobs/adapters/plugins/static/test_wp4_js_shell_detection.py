from __future__ import annotations

from src.jobs.adapters.plugins.static import _heuristics
from src.jobs.common.taxonomy import (
    ClassificationContext,
    ZeroExtractDiagnosis,
    assess_zero_extract,
)


def test_react_next_spa_tokens_detect_shell() -> None:
    html = '<div id="root"><div class="site"></div></div><script>window.__DATA__</script>'
    assert _heuristics.detect_js_shell(html) is True


def test_legacy_ember_hydration_with_career_context_detects_shell() -> None:
    # jQuery-era Ember shell: handlebars template + career listing context.
    html = (
        '<script type="text/x-handlebars" id="jobs-list">{{#each jobs}}'
        '<div class="job-card"></div>{{/each}}</script>'
    )
    assert _heuristics.detect_js_shell(html) is True


def test_legacy_angularjs_boot_detects_shell() -> None:
    html = '<body ng-app="careersApp" ng-controller="JobListController">'
    assert _heuristics.detect_js_shell(html) is True


def test_hydrated_href_placeholder_with_job_context_detects_shell() -> None:
    html = '<a data-href="/job/openings" class="job-card">Engineer</a>'
    assert _heuristics.detect_js_shell(html) is True


def test_jquery_rehydrating_listing_containers_detects_shell() -> None:
    # jQuery era: a real jquery lib + a rehydrated job-list container.
    html = (
        '<script src="/jquery.min.js"></script><script>jQuery("#job-list").appendRows()</script>'
        '<div id="job-list" data-loading="true"></div>'
    )
    assert _heuristics.detect_js_shell(html) is True


def test_server_rendered_page_with_jquery_and_json_ld_stays_negative() -> None:
    # Plain server-rendered careers page that bundles jQuery but lists jobs in HTML.
    html = (
        '<script src="/jquery.js"></script>'
        '<a class="job-listing" href="/careers/engineer">Senior Engineer</a>'
        '<a class="job-listing" href="/careers/artist">Artist</a>'
    )
    assert _heuristics.detect_js_shell(html) is False


def test_server_rendered_wordpress_with_career_text_stays_negative() -> None:
    html = (
        "<h1>Careers</h1>"
        "<p>We are hiring for the following open positions across our teams.</p>"
        '<a href="/jobs/programmer">Programmer</a>'
    )
    assert _heuristics.detect_js_shell(html) is False


def test_handlebars_lib_only_without_templates_stays_negative() -> None:
    # Ships a handlebars library on a server-rendered page with no template blocks.
    html = '<script src="/handlebars.min.js"></script><h1>About our studio</h1>'
    assert _heuristics.detect_js_shell(html) is False


def test_empty_or_trivial_html_returns_false() -> None:
    assert _heuristics.detect_js_shell("") is False
    assert _heuristics.detect_js_shell("<html><body>Hello</body></html>") is False


def test_outbound_ats_links_detect_ultipro_and_paycom() -> None:
    # ATS-redirect careers pages (Konami Gaming class) must surface their external board links
    # as outbound ATS evidence so they classify as ATS-redirect pages, not job listings.
    html = (
        '<a href="https://recruiting.ultipro.com/KON1000/JobBoard/abc123">Click Here</a>'
        '<a href="https://www.paycomonline.net/v4/ats/web.php/jobs?clientkey=x">Apply</a>'
        '<a href="/careers">Careers</a>'
    )
    links = _heuristics.detect_outbound_ats_links(
        html, base_url="https://www.konamigaming.com/careers"
    )
    assert any("recruiting.ultipro.com" in link for link in links)
    assert any("paycomonline.net" in link for link in links)
    assert not any("konamigaming.com/careers" in link for link in links)


def test_js_shell_classifies_as_browser_eligible_js_required() -> None:
    # A lineup hit as a JS shell should diagnose as js_required (browser escalation).
    ctx = ClassificationContext(
        status="ok",
        error="no jobs extracted from source pages",
        classification="ok_no_jobs",
        extractor_hint="js_shell_detected",
    )
    assessment = assess_zero_extract(ctx)
    assert assessment.diagnosis == ZeroExtractDiagnosis.JS_REQUIRED
    assert assessment.browser_fallback_recommended is True
