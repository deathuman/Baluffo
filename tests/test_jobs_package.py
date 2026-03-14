import unittest
from unittest import mock
from pathlib import Path

from scripts import jobs_fetcher as jf
from scripts.jobs import adapters, canonicalize, dedup, parsers, registry, transport
from scripts.jobs.models import CanonicalJob


class JobsPackageTests(unittest.TestCase):
    def test_transport_builds_headers_and_preserves_proxy_config(self) -> None:
        request = transport.default_request_config(
            timeout_s=9,
            headers={"X-Test": "1"},
            user_agent="Agent/2.0",
            proxy_url="http://proxy.internal:8080",
        )
        headers = transport.build_headers(request)
        self.assertEqual(request.timeout_s, 9)
        self.assertEqual(request.proxy_url, "http://proxy.internal:8080")
        self.assertEqual(headers["User-Agent"], "Agent/2.0")
        self.assertEqual(headers["X-Test"], "1")

    def test_registry_social_config_merges_defaults(self) -> None:
        config = registry.load_social_config(
            config_path=registry.DEFAULT_OUTPUT_DIR / "missing-social-config.json",
            enabled=True,
            lookback_minutes=45,
        )
        self.assertTrue(config["enabled"])
        self.assertEqual(int(config["lookbackMinutes"]), 45)
        self.assertIn("reddit", config)

    def test_canonicalize_returns_typed_job(self) -> None:
        job = canonicalize.canonicalize_job(
            {
                "title": "Technical Artist",
                "company": "Studio A",
                "city": "Amsterdam",
                "country": "NL",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/jobs/ta?utm_source=x",
                "sector": "Game",
            },
            source="unit",
            fetched_at="2026-03-13T10:00:00+00:00",
        )
        self.assertIsInstance(job, CanonicalJob)
        assert job is not None
        self.assertEqual(job.jobLink, "https://example.com/jobs/ta")
        self.assertEqual(job.profession, "technical-artist")

    def test_parsers_keep_extraction_raw_and_dedup_accepts_typed_records(self) -> None:
        rows = parsers.parse_jobpostings_from_html(
            """
            <script type="application/ld+json">
            {
              "@context": "https://schema.org",
              "@type": "JobPosting",
              "title": "Environment Artist",
              "url": "/jobs/env-artist",
              "hiringOrganization": {"name": "Studio B"},
              "employmentType": "Full-time",
              "jobLocation": {"address": {"addressLocality": "Utrecht", "addressCountry": "NL"}}
            }
            </script>
            """,
            base_url="https://example.com/careers",
        )
        self.assertEqual(len(rows), 1)
        self.assertNotIn("profession", rows[0])

        typed_rows = [
            canonicalize.canonicalize_job(
                row,
                source="unit",
                fetched_at="2026-03-13T10:00:00+00:00",
            )
            for row in rows
        ]
        typed_rows = [row for row in typed_rows if row is not None]
        merged, stats = dedup.deduplicate_jobs(typed_rows)
        self.assertEqual(len(merged), 1)
        self.assertEqual(int(stats["outputCount"]), 1)
        self.assertIsInstance(merged[0], CanonicalJob)

    def test_extracted_adapter_registry_exposes_moved_families(self) -> None:
        self.assertIn("google_sheets", adapters.EXTRACTED_ADAPTERS)
        self.assertIn("social_x", adapters.EXTRACTED_ADAPTERS)
        self.assertIn("greenhouse_boards", adapters.EXTRACTED_ADAPTERS)
        self.assertIn("scrapy_static_sources", adapters.EXTRACTED_ADAPTERS)
        self.assertIn("static_studio_pages", adapters.EXTRACTED_ADAPTERS)

    def test_social_adapter_uses_jobs_fetcher_urlopen_patch_surface(self) -> None:
        class _Headers:
            @staticmethod
            def get_content_charset() -> str:
                return "utf-8"

        class _Response:
            headers = _Headers()

            def read(self) -> bytes:
                return b'{"data": []}'

            def __enter__(self):  # noqa: ANN204
                return self

            def __exit__(self, exc_type, exc, tb):  # noqa: ANN001, ANN204
                return False

        with mock.patch.object(jf, "urlopen", return_value=_Response()) as patched:
            payload = adapters.social._request_json_with_headers("https://example.com/api", timeout_s=5)
        self.assertEqual(payload, {"data": []})
        patched.assert_called_once()

    def test_static_adapter_uses_jobs_fetcher_diagnostics_patch_surface(self) -> None:
        previous = list(jf.STUDIO_SOURCE_REGISTRY)
        jf.STUDIO_SOURCE_REGISTRY = []
        try:
            with mock.patch.object(jf, "set_source_diagnostics") as diag:
                rows = adapters.static.run_scrapy_static_source(
                    fetch_text=lambda _url, _timeout: "",
                    timeout_s=5,
                    retries=0,
                    backoff_s=0.0,
                )
            self.assertEqual(rows, [])
            diag.assert_called_once()
        finally:
            jf.STUDIO_SOURCE_REGISTRY = previous

    def test_package_modules_do_not_import_legacy_impl(self) -> None:
        package_root = Path(__file__).resolve().parents[1] / "scripts" / "jobs"
        targets = [
            package_root / "canonicalize.py",
            package_root / "dedup.py",
            package_root / "parsers.py",
            package_root / "registry.py",
            package_root / "reporting.py",
            package_root / "state.py",
            package_root / "transport.py",
            package_root / "pipeline.py",
        ]
        for target in targets:
            text = target.read_text(encoding="utf-8")
            self.assertNotIn("from scripts.jobs import legacy_impl", text, msg=str(target))
            self.assertNotIn("import scripts.jobs.legacy_impl", text, msg=str(target))

    def test_jobs_fetcher_exposes_curated_package_surface(self) -> None:
        self.assertTrue(callable(jf.run_pipeline))
        self.assertTrue(callable(jf.parse_args))
        self.assertTrue(callable(jf.main))
        self.assertTrue(callable(jf.default_source_loaders))
        self.assertTrue(callable(jf.set_source_diagnostics))
        self.assertTrue(callable(jf.build_redirect_resolver))
        self.assertTrue(callable(jf.parse_google_sheets_csv))
        self.assertTrue(callable(jf.canonicalize_job))
        self.assertTrue(callable(jf.deduplicate_jobs))
        self.assertIsInstance(jf.__all__, list)
        self.assertIn("run_pipeline", jf.__all__)
        self.assertIn("default_source_loaders", jf.__all__)
        self.assertIn("set_source_diagnostics", jf.__all__)


if __name__ == "__main__":
    unittest.main()
