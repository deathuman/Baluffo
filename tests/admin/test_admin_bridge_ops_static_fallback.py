import html
import json

from scripts import admin_bridge
from tests.admin.admin_bridge_ops_base import AdminBridgeOpsTestCase


class AdminBridgeOpsStaticFallbackTests(AdminBridgeOpsTestCase):
    def test_trigger_source_check_static_fallback_uses_generic_scrape(self):
        added = admin_bridge.add_manual_source("https://milestone.it/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <a href="/jobs/engine-programmer">Engine Programmer</a>
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"JobPosting","title":"Technical Artist","url":"https://milestone.it/jobs/technical-artist"}
        </script>
        """
        detail_html = """
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"JobPosting","title":"Engine Programmer","url":"https://milestone.it/jobs/engine-programmer"}
        </script>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://milestone.it/careers":
                    return listing_html
                if url == "https://milestone.it/jobs/engine-programmer":
                    return detail_html
                raise RuntimeError(f"unexpected URL: {url}")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertEqual(int(result["jobsFound"]), 2)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_returns_failure_when_no_jobs(self):
        added = admin_bridge.add_manual_source("https://milestone.it/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: "<html><body>No jobs</body></html>"
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertIn("no job postings found", str(result.get("error") or "").lower())
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_embedded_job_openings_module(self):
        added = admin_bridge.add_manual_source("https://www.avalanchestudios.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script>
        window.__NUXT__={state:{},data:[{body:[{slice_type:"job_openings_module"}]}]}
        </script>
        """
        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_classifies_rendered_404_page(self):
        added = admin_bridge.add_manual_source("https://www.paradoxinteractive.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = "<html><head><title>404 Not Found - Paradox Interactive</title></head><body>missing</body></html>"
        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertEqual(str(result.get("errorCode") or ""), "not_found")
            self.assertTrue(bool(result.get("suggestedUrls")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_attempts_browser_on_403(self):
        added = admin_bridge.add_manual_source("https://careers.rebellion.com/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        original_browser_fetch = admin_bridge._try_fetch_with_playwright
        try:
            def fake_fetch(_url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                raise RuntimeError("HTTP Error 403: Forbidden")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            admin_bridge._try_fetch_with_playwright = lambda *_args, **_kwargs: ('<a href="/jobs/gameplay-programmer">Role</a>', "")
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertEqual(int(result["jobsFound"]), 1)
            self.assertTrue(bool(result.get("browserFallbackAttempted")))
            self.assertTrue(bool(result.get("browserFallbackUsed")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch
            admin_bridge._try_fetch_with_playwright = original_browser_fetch

    def test_trigger_source_check_static_fallback_attempts_browser_on_challenge_page(self):
        added = admin_bridge.add_manual_source("https://jobs.zenimax.com/jobs")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        original_browser_fetch = admin_bridge._try_fetch_with_playwright
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: (
                '<html><head><script src="/cdn-cgi/challenge-platform/h/g/scripts/jsd/main.js"></script></head>'
                '<body>Just a moment...</body></html>'
            )
            admin_bridge._try_fetch_with_playwright = lambda *_args, **_kwargs: (
                '<a href="/requisitions/view/3472">Associate DevOps Programmer</a>'
                '<a href="/requisitions/view/3479">Development QA Manager</a>',
                "",
            )
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertEqual(int(result["jobsFound"]), 2)
            self.assertTrue(bool(result.get("browserFallbackAttempted")))
            self.assertTrue(bool(result.get("browserFallbackUsed")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch
            admin_bridge._try_fetch_with_playwright = original_browser_fetch

    def test_trigger_source_check_static_parses_embedded_job_filter_payload(self):
        added = admin_bridge.add_manual_source("https://jobs.zenimax.com/jobs")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        payload = {
            "jobs": [
                {"id": 3472, "title": "Associate DevOps Programmer", "link": "https://careers-zenimax.icims.com/jobs/3472/associate-devops-programmer/job"},
                {"id": 3479, "title": "Development QA Manager", "link": "https://careers-zenimax.icims.com/jobs/3479/development-qa-manager/job"},
                {"id": 3488, "title": "Senior Gameplay Programmer", "link": "https://careers-zenimax.icims.com/jobs/3488/senior-gameplay-programmer/job"},
            ]
        }
        raw_data = html.escape(json.dumps(payload), quote=True)
        listing_html = (
            '<script src="/cdn-cgi/challenge-platform/h/g/scripts/jsd/main.js"></script>'
            f'<job-filter :raw-data="{raw_data}"></job-filter>'
        )

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        original_browser_fetch = admin_bridge._try_fetch_with_playwright
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            admin_bridge._try_fetch_with_playwright = lambda *_args, **_kwargs: ("", "unexpected browser fallback")
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertFalse(bool(result.get("weakSignal")))
            self.assertEqual(int(result["jobsFound"]), 3)
            self.assertFalse(bool(result.get("browserFallbackAttempted")))
            self.assertFalse(bool(result.get("browserFallbackUsed")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch
            admin_bridge._try_fetch_with_playwright = original_browser_fetch

    def test_trigger_source_check_static_fallback_reports_unavailable_browser_fallback(self):
        added = admin_bridge.add_manual_source("https://careers.rebellion.com/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        original_browser_fetch = admin_bridge._try_fetch_with_playwright
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("HTTP Error 403: Forbidden"))
            admin_bridge._try_fetch_with_playwright = lambda *_args, **_kwargs: ("", "browser fallback unavailable (playwright is not installed)")
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertEqual(str(result.get("errorCode") or ""), "browser_fallback_unavailable")
            self.assertTrue(bool(result.get("browserFallbackAttempted")))
            self.assertFalse(bool(result.get("browserFallbackUsed")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch
            admin_bridge._try_fetch_with_playwright = original_browser_fetch

    def test_trigger_source_check_static_fallback_returns_404_hints(self):
        added = admin_bridge.add_manual_source("https://www.king.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("HTTP Error 404: Not Found"))
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertEqual(str(result.get("errorCode") or ""), "not_found")
            suggested = result.get("suggestedUrls") or []
            self.assertIn("https://careers.king.com", suggested)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_retries_suggested_alternate_on_404(self):
        added = admin_bridge.add_manual_source("https://www.fatsharkgames.com/career")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://www.fatsharkgames.com/career":
                    raise RuntimeError("HTTP Error 404: Not Found")
                if url == "https://jobs.fatsharkgames.com":
                    return '<a href="https://jobs.fatsharkgames.com/jobs/senior-programmer">Role</a>'
                raise RuntimeError(f"unexpected URL: {url}")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_uses_parent_redirect_candidates_on_404(self):
        added = admin_bridge.add_manual_source("https://www.fatsharkgames.com/career")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        original_redirect = admin_bridge._discover_redirect_career_candidates
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://www.fatsharkgames.com/career":
                    raise RuntimeError("HTTP Error 404: Not Found")
                if url == "https://jobs.fatsharkgames.com":
                    return '<a href="https://jobs.fatsharkgames.com/jobs/network-programmer">Role</a>'
                raise RuntimeError(f"unexpected URL: {url}")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            admin_bridge._discover_redirect_career_candidates = lambda *_args, **_kwargs: ["https://jobs.fatsharkgames.com"]
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch
            admin_bridge._discover_redirect_career_candidates = original_redirect

    def test_trigger_source_check_static_fallback_returns_ssl_error_code(self):
        added = admin_bridge.add_manual_source("https://careers.11bitstudios.com/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("SSL: CERTIFICATE_VERIFY_FAILED hostname mismatch")
            )
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertFalse(result["ok"])
            self.assertEqual(str(result.get("errorCode") or ""), "ssl_error")
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_extracts_intervieweb_links(self):
        added = admin_bridge.add_manual_source("https://milestone.it/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        page_html = """
        <script src="https://cezanneondemand.intervieweb.it/integration/announces_js.php?lang=en&utype=0&k=abc123&LAC=milestone&d=milestone.it&annType=published&view=list&defgroup=name&gnavenable=1&desc=1&typeView=large"></script>
        """
        iframe_html = """
        <a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=60982&lang=en">Job A</a>
        <a href="https://cezanneondemand.intervieweb.it/app.php?opmode=guest&module=iframeAnnunci&act1=1&IdAnnuncio=61104&lang=en">Job B</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://milestone.it/careers":
                    return page_html
                if "module=iframeAnnunci" in url and "act1=23" in url:
                    return iframe_html
                raise RuntimeError(f"unexpected URL: {url}")

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertEqual(int(result["jobsFound"]), 2)
            self.assertTrue(bool(result.get("weakSignal")))
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_careers_role_links(self):
        added = admin_bridge.add_manual_source("https://www.naconstudiomilan.com/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <a href="/careers/">Careers</a>
        <a href="/careers-category/design/">Design category</a>
        <a href="/careers/gameplay-designer/">Gameplay Designer</a>
        <a href="/careers/gameplay-programmer/">Gameplay Programmer</a>
        <a href="/careers/ai-programmer/">AI Programmer</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertEqual(int(result["jobsFound"]), 3)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_textual_apply_role_signals(self):
        added = admin_bridge.add_manual_source("https://www.4a-games.com.mt/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <html><body>
        <h1>We're hiring for multiple projects</h1>
        <p>Senior Gameplay Programmer</p><button>Apply now</button>
        <p>Lead Technical Artist</p><button>Apply now</button>
        <p>Animation Programmer</p><button>Apply now</button>
        <p>QA Tester</p><button>Apply now</button>
        </body></html>
        """
        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_crytek_like_embedded_links(self):
        added = admin_bridge.add_manual_source("https://www.crytek.com/career")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script>
        self.__next_f.push([1,"{\\"leverInitialData\\":{\\"postings\\":[{\\"hosted_url\\":\\"https://jobs.lever.co/crytek/abc123\\"}]}}"]);
        </script>
        <a href="/career/posting/0cb503b8-53c9-4932-b0d1-8864e75deed8">Posting</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 2)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_extracts_embedded_relative_career_links(self):
        added = admin_bridge.add_manual_source("https://www.4a-games.com.mt/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script>
        window.__PAGE_DATA__ = {"jobs":["/careers/senior-gameplay-programmer","/careers/lead-technical-artist"]};
        </script>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 2)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_smartrecruiters_embedded_url(self):
        added = admin_bridge.add_manual_source("https://www.cdprojektred.com/en/jobs")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script>
        var data = {"jobs":["https://jobs.smartrecruiters.com/CDPROJEKTRED/743999834254914-spontaneous-application"]};
        </script>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_counts_personio_search_json(self):
        added = admin_bridge.add_manual_source("https://yager.de/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <script src="https://assets.cdn.personio.de/jobs/v2/min/js/jobs_list.bed3abfdd85796686e20.js"></script>
        <a href="https://yager.jobs.personio.de/">Jobs board</a>
        """
        personio_search_json = '{"data":[{"id":1},{"id":2}]}'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://yager.de/careers":
                    return listing_html
                if url == "https://yager.jobs.personio.de/search.json":
                    return personio_search_json
                if url == "https://yager.jobs.personio.de":
                    return "<html>Personio Board</html>"
                return "<html></html>"

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 2)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_detects_jobylon_embed(self):
        added = admin_bridge.add_manual_source("https://www.remedygames.com/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <div id="jobylon-jobs-widget"></div>
        <script>
        var jbl_company_id = 2986;
        var jbl_version = 'v2';
        var jbl_page_size = 30;
        var el = document.createElement('script');
        el.src = 'https://cdn.jobylon.com/embedder.js';
        </script>
        """
        embed_html = "<html><body>Jobylon widget</body></html>"

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://www.remedygames.com/careers":
                    return listing_html
                if "cdn.jobylon.com/jobs/companies/2986/embed/v2/" in url:
                    return embed_html
                return "<html></html>"

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_join_role_links(self):
        added = admin_bridge.add_manual_source("https://www.guerrilla-games.com/join")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <a href="/join/senior-technical-animator/5778235004">Senior Technical Animator</a>
        <a href="/join?page=2#postings">Pager</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_open_positions_links(self):
        added = admin_bridge.add_manual_source("https://www.rovio.com/careers/")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        careers_html = '<a href="/open-positions/">Open Positions</a>'
        open_positions_html = '<a href="/open-positions/game-developer-abc/">Game Developer</a>'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://www.rovio.com/careers":
                    return careers_html
                if url == "https://www.rovio.com/open-positions":
                    return open_positions_html
                return "<html></html>"

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_job_offers_links(self):
        added = admin_bridge.add_manual_source("https://techland.net/job-offers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = '<a href="/job-offers/senior-engine-programmer">Senior Engine Programmer</a>'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_vacancy_links(self):
        added = admin_bridge.add_manual_source("https://www.playground-games.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = '<a href="/vacancy/25">Senior Animator</a>'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_accepts_vacancies_slug_links(self):
        added = admin_bridge.add_manual_source("https://careers.sega.co.uk/vacancies")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = """
        <a href="/vacancies">Vacancies</a>
        <a href="/vacancies/lead-environment-artist">Lead Environment Artist</a>
        """

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 1)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_fallback_counts_workable_widget_jobs(self):
        added = admin_bridge.add_manual_source("https://team17.com/careers")
        source_id = str(added.get("sourceId") or "")
        self.assertTrue(source_id)

        listing_html = '<a href="https://apply.workable.com/team-17-digital/">Open roles</a>'
        workable_json = '{"jobs":[{"id":1},{"id":2},{"id":3}]}'

        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
                if url == "https://team17.com/careers":
                    return listing_html
                if url == "https://apply.workable.com/api/v1/widget/accounts/team-17-digital?details=true":
                    return workable_json
                return "<html></html>"

            admin_bridge.discovery.fetch_text_with_retry = fake_fetch
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            self.assertTrue(bool(result.get("weakSignal")))
            self.assertGreaterEqual(int(result["jobsFound"]), 3)
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch

    def test_trigger_source_check_static_normalizes_placeholder_studio_name(self):
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
        admin_bridge.save_json_atomic(admin_bridge.PENDING_PATH, [pending_row])
        source_id = str(pending_row["id"])

        listing_html = '<a href="/careers/gameplay-designer/">Gameplay Designer</a>'
        original_fetch = admin_bridge.discovery.fetch_text_with_retry
        try:
            admin_bridge.discovery.fetch_text_with_retry = lambda *_args, **_kwargs: listing_html
            result = admin_bridge.trigger_source_check(source_id)
            self.assertTrue(result["started"])
            self.assertTrue(result["ok"])
            pending = admin_bridge.load_json_array(admin_bridge.PENDING_PATH, [])
            updated = next((row for row in pending if admin_bridge.source_identity(row) == source_id), {})
            self.assertEqual(str(updated.get("studio") or ""), "Nacon Studio Milan")
        finally:
            admin_bridge.discovery.fetch_text_with_retry = original_fetch