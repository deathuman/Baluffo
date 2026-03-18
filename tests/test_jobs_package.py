from pathlib import Path
from unittest import mock

from src import jobs_fetcher as jf
from src.jobs import adapters, canonicalize, dedup, parsers, registry, transport
from src.jobs.models import CanonicalJob


def test_transport_builds_headers_and_preserves_proxy_config() -> None:
    request = transport.default_request_config(
        timeout_s=9,
        headers={"X-Test": "1"},
        user_agent="Agent/2.0",
        proxy_url="http://proxy.internal:8080",
    )
    headers = transport.build_headers(request)
    assert request.timeout_s == 9
    assert request.proxy_url == "http://proxy.internal:8080"
    assert headers["User-Agent"] == "Agent/2.0"
    assert headers["X-Test"] == "1"


def test_registry_social_config_merges_defaults() -> None:
    config = registry.load_social_config(
        config_path=registry.DEFAULT_OUTPUT_DIR / "missing-social-config.json",
        enabled=True,
        lookback_minutes=45,
    )
    assert config["enabled"]
    assert int(config["lookbackMinutes"]) == 45
    assert "reddit" in config


def test_canonicalize_returns_typed_job() -> None:
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
    assert isinstance(job, CanonicalJob)
    assert job is not None
    assert job.jobLink == "https://example.com/jobs/ta"
    assert job.profession == "technical-artist"


def test_parsers_keep_extraction_raw_and_dedup_accepts_typed_records() -> None:
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
    assert len(rows) == 1
    assert "profession" not in rows[0]

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
    assert len(merged) == 1
    assert int(stats["outputCount"]) == 1
    assert isinstance(merged[0], CanonicalJob)


def test_extracted_adapter_registry_exposes_moved_families() -> None:
    assert "google_sheets" in adapters.EXTRACTED_ADAPTERS
    assert "social_x" in adapters.EXTRACTED_ADAPTERS
    assert "greenhouse_boards" in adapters.EXTRACTED_ADAPTERS
    assert "scrapy_static_sources" in adapters.EXTRACTED_ADAPTERS
    assert "static_studio_pages" in adapters.EXTRACTED_ADAPTERS


def test_social_adapter_uses_jobs_fetcher_urlopen_patch_surface() -> None:
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
    assert payload == {"data": []}
    patched.assert_called_once()


def test_static_adapter_uses_jobs_fetcher_diagnostics_patch_surface() -> None:
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
        assert rows == []
        diag.assert_called_once()
    finally:
        jf.STUDIO_SOURCE_REGISTRY = previous


def test_package_modules_do_not_import_legacy_impl(repo_root: Path) -> None:
    package_root = repo_root / "src" / "jobs"
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
        assert "from src.jobs import legacy_impl" not in text, str(target)
        assert "import src.jobs.legacy_impl" not in text, str(target)


def test_pipeline_module_uses_package_private_helper_boundaries(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "pipeline.py"
    text = target.read_text(encoding="utf-8")
    assert "from src.jobs.pipeline_bootstrap import" in text
    assert "from src.jobs.pipeline_loader_selection import" in text
    assert "from src.jobs.pipeline_runtime import" in text


def test_static_adapter_uses_package_private_helper_boundary(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "adapters" / "static.py"
    text = target.read_text(encoding="utf-8")
    assert "from src.jobs.adapters.static_helpers import" in text


def test_jobs_fetcher_exposes_curated_package_surface() -> None:
    assert callable(jf.run_pipeline)
    assert callable(jf.parse_args)
    assert callable(jf.main)
    assert callable(jf.default_source_loaders)
    assert callable(jf.set_source_diagnostics)
    assert callable(jf.build_redirect_resolver)
    assert callable(jf.parse_google_sheets_csv)
    assert callable(jf.canonicalize_job)
    assert callable(jf.deduplicate_jobs)
    assert isinstance(jf.__all__, list)
    assert "run_pipeline" in jf.__all__
    assert "default_source_loaders" in jf.__all__
    assert "set_source_diagnostics" in jf.__all__
