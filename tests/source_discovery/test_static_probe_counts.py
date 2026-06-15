from __future__ import annotations

from src.source_discovery import probe


def test_static_probe_counts_positive_static_job_signals() -> None:
    cases = [
        {
            "case_id": "generic-application-filtering",
            "candidate": {
                "adapter": "static",
                "listing_url": "https://azragames.com/careers/",
            },
            "html": """
            <a href="/careers/">Careers</a>
            <a href="/careers/#opening">Openings</a>
            <a href="https://azragames.com/careers/#jobs">Jobs</a>
            <a href="https://job-boards.greenhouse.io/azragames/jobs/4978306007">
              Senior Unity Gameplay Capture Artist
            </a>
            <a href="https://boards.greenhouse.io/azragamesoa/jobs/4345814007">
              Submit Your Application
            </a>
            """,
            "count": 1,
            "candidate_fields": {
                "lastProbeCountConfidence": "high",
                "lastReliableJobsFound": 1,
            },
        },
        {
            "case_id": "hidden-template-links",
            "candidate": {
                "adapter": "static",
                "listing_url": "https://studio.example/careers/",
            },
            "html": """
            <section hidden>
              <a href="/jobs/one">Gameplay Engineer</a>
              <a href="/jobs/two">Tools Engineer</a>
            </section>
            <div style="display:none">
              <a href="/jobs/three">Producer</a>
            </div>
            <a href="/jobs/live">Live Operations Engineer</a>
            """,
            "count": 1,
            "candidate_fields": {},
        },
        {
            "case_id": "same-listing-detail-links",
            "candidate": {
                "adapter": "static",
                "listing_url": "https://studio.example/work-with-us/index.html",
            },
            "html": """
            <a href="/work-with-us/4023614009/">Systems Engineer</a>
            <a href="/work-with-us/4023591009/">Lead Environment Artist</a>
            <a href="/work-with-us/94b98a86-d14e-49e5-b117-5b40bce17c9d/">HR Business Partner</a>
            <a href="/work-with-us/#benefits">Work settings</a>
            """,
            "count": 3,
            "candidate_fields": {
                "lastProbeCountReason": "detail_links",
                "lastReliableJobsFound": 3,
            },
        },
        {
            "case_id": "result-count-label",
            "candidate": {
                "adapter": "static",
                "listing_url": "https://www.krafton.com/careers/jobs/",
            },
            "html": """
            <main>
              <p>102 Results Found</p>
              <a href="/careers/jobs/?job_posting=1001">Gameplay Programmer</a>
              <a href="/careers/jobs/?job_posting=1002">Technical Artist</a>
              <a href="/careers/jobs/?var_page=2">Next</a>
              <a href="/careers/jobs/?search_keyword=engineer">Search</a>
            </main>
            """,
            "count": 102,
            "candidate_fields": {
                "lastProbeCountReason": "result_count_label",
                "lastReliableJobsFound": 102,
            },
        },
        {
            "case_id": "elevato-comma-job-paths",
            "candidate": {
                "adapter": "static",
                "listing_url": "https://qloc.elevato.net/en/",
            },
            "html": """
            <main>
              <a href="https://qloc.elevato.net/en/translator-proofreader,j,242">
                Translator / Proofreader
              </a>
              <a href="https://qloc.elevato.net/en/technical-artist,j,240?source=10">
                Technical Artist
              </a>
              <a href="https://q-loc.com/privacy-policy/personal-data-processing/">Privacy</a>
              <a href="https://qloc.elevato.net/en/job-offers,j">Show all job offers</a>
            </main>
            """,
            "count": 2,
            "candidate_fields": {
                "lastProbeCountReason": "detail_links",
                "lastReliableJobsFound": 2,
            },
        },
    ]

    for case in cases:
        case_id = str(case["case_id"])
        candidate = dict(case["candidate"])
        html = str(case["html"])

        ok, count, error = probe.probe_candidate(
            candidate,
            timeout_s=5,
            fetcher=lambda *_args, html=html: html,
        )

        assert ok, case_id
        assert count == case["count"], case_id
        assert error == "", case_id
        for key, expected in dict(case["candidate_fields"]).items():
            assert candidate[key] == expected, f"{case_id}:{key}"


def test_static_probe_suppresses_non_job_static_signals() -> None:
    cases = [
        {
            "case_id": "no-openings-jsonld-override",
            "candidate": {
                "adapter": "static",
                "listing_url": "https://studio.example/careers/",
            },
            "html": """
            <main>
              <h1>Careers</h1>
              <p>There are currently no open positions.</p>
              <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "JobPosting",
                "title": "Gameplay Engineer",
                "url": "https://studio.example/jobs/gameplay-engineer"
              }
              </script>
            </main>
            """,
            "candidate_fields": {
                "lastProbeWeakSignal": True,
                "lastProbeCountReason": "no_openings_overrides_jsonld",
                "lastReliableJobsFound": 0,
            },
        },
        {
            "case_id": "listing-navigation-only",
            "candidate": {
                "adapter": "static",
                "listing_url": "https://www.krafton.com/careers/",
            },
            "html": """
            <main>
              <a href="/careers/jobs/">Jobs</a>
              <a href="/careers/people/">People</a>
            </main>
            """,
            "candidate_fields": {"lastProbeCountReason": "no_jobs"},
        },
        {
            "case_id": "mojang-landing-navigation",
            "candidate": {
                "adapter": "static",
                "listing_url": "https://www.minecraft.net/mojang-careers",
            },
            "html": """
            <main>
              <h1>Mojang Studios Careers</h1>
              <a href="/en-us/store/minecraft-deluxe-collection-pc">Buy Minecraft</a>
              <a href="/en-us/community">Community</a>
              <a href="/en-us/mojang-careers">Mojang Studios Careers</a>
              <a href="https://www.youtube.com/minecraft">Follow Minecraft</a>
              <a href="https://discord.gg/minecraft">Official Minecraft Discord</a>
              <a href="https://help.minecraft.net/hc/en-us">Minecraft Help Center</a>
            </main>
            <footer>
              <a href="/en-us">English</a>
              <a href="/de-de">Deutsch</a>
              <a href="/fr-fr">Francais</a>
              <a href="/ja-jp">Japanese</a>
              <a href="https://go.microsoft.com/fwlink/?LinkId=521839">Privacy and Cookies</a>
            </footer>
            """,
            "candidate_fields": {"lastProbeCountReason": "no_jobs"},
        },
    ]

    for case in cases:
        case_id = str(case["case_id"])
        candidate = dict(case["candidate"])
        html = str(case["html"])

        ok, count, error = probe.probe_candidate(
            candidate,
            timeout_s=5,
            fetcher=lambda *_args, html=html: html,
        )

        assert ok, case_id
        assert count == 0, case_id
        assert error == "", case_id
        for key, expected in dict(case["candidate_fields"]).items():
            assert candidate[key] == expected, f"{case_id}:{key}"
