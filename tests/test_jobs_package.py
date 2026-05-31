import importlib
import subprocess
import sys
from pathlib import Path
from unittest import mock

from src.jobs import adapters, canonicalize, dedup, registry, transport
from src.jobs import common as jobs_common
from src.jobs.adapters.html_parsers import parse_jobpostings_from_html
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
    reddit_config = config.get("reddit") or {}
    subreddits = reddit_config.get("subreddits") or []
    assert reddit_config.get("enabled") is False
    assert len(subreddits) == 0
    expected_subreddits = []
    assert subreddits == expected_subreddits


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


def test_canonical_job_from_mapping_collapses_sequence_text_fields() -> None:
    job = CanonicalJob.from_mapping(
        {
            "title": ["Lead Technical Designer", " House of How Games"],
            "company": ["House of How"],
            "description": ["Lead Technical Designer", "House of How Games"],
            "city": ["Stockholm"],
            "country": ["SE"],
            "jobLink": "https://www.houseofhow.com/job/lead-technical-designer",
            "source": ["scrapy_static_sources"],
            "sourceJobId": ["3fd3af46526a"],
            "locationSummary": ["Stockholm, SE"],
            "adapter": ["static"],
            "studio": ["House of How"],
        }
    )

    assert job.title == "Lead Technical Designer"
    assert job.company == "House of How"
    assert job.description == "Lead Technical Designer"
    assert job.city == "Stockholm"
    assert job.country == "SE"
    assert job.source == "scrapy_static_sources"
    assert job.sourceJobId == "3fd3af46526a"
    assert job.locationSummary == "Stockholm, SE"
    assert job.adapter == "static"
    assert job.studio == "House of How"


def test_parsers_keep_extraction_raw_and_dedup_accepts_typed_records() -> None:
    rows = parse_jobpostings_from_html(
        """
        <script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@type": "JobPosting",
          "title": "Environment Artist",
          "url": "/jobs/env-artist",
          "hiringOrganization": {"name": "Studio B"},
          "employmentType": "Full-time",
          "jobLocation": [
            {"address": {"addressLocality": "Utrecht", "addressCountry": "NL"}},
            {"address": {"addressLocality": "Guildford", "addressCountry": "UK"}}
          ]
        }
        </script>
        """,
        base_url="https://example.com/careers",
    )
    assert len(rows) == 1
    assert "profession" not in rows[0]
    assert rows[0]["locations"] == [
        {"city": "Utrecht", "country": "NL"},
        {"city": "Guildford", "country": "UK"},
    ]
    assert rows[0]["locationSummary"] == "Utrecht, NL | Guildford, UK"

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
    assert merged[0].locations == [
        {"city": "Utrecht", "country": "NL"},
        {"city": "Guildford", "country": "UK"},
    ]
    assert merged[0].locationSummary == "Utrecht, NL | Guildford, UK"


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

    with mock.patch.object(adapters.social, "urlopen", return_value=_Response()) as patched:
        payload = adapters.social._request_json_with_headers("https://example.com/api", timeout_s=5)
    assert payload == {"data": []}
    patched.assert_called_once()


def test_static_scrapy_adapter_uses_direct_registry_and_diagnostics_surface() -> None:
    with (
        mock.patch.object(adapters.static_scrapy, "registry_entries", return_value=[]),
        mock.patch.object(adapters.static_scrapy, "set_source_diagnostics") as diag,
    ):
        rows = adapters.static.run_scrapy_static_source(
            fetch_text=lambda _url, _timeout: "",
            timeout_s=5,
            retries=0,
            backoff_s=0.0,
        )
    assert rows == []
    diag.assert_called_once()


def test_generic_careers_import_is_clean_under_python_314(repo_root: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-W",
            "error::UserWarning",
            "-c",
            "import src.scrapers.spiders.generic_careers",
        ],
        capture_output=True,
        cwd=repo_root,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_jobs_common_root_package_is_minimal_and_leaf_modules_remain_importable() -> None:
    migration_map = {
        "registry_entries": ("src.jobs.registry", "registry_entries"),
        "fetch_with_retries": ("src.jobs.common.fetch", "fetch_with_retries"),
        "set_source_diagnostics": ("src.jobs.common.diagnostics", "set_source_diagnostics"),
        "default_fetch_text": ("src.jobs.common.http", "default_fetch_text"),
        "to_iso": ("src.jobs.common.datetime_utils", "to_iso"),
        "RawJob": ("src.jobs.models", "RawJob"),
        "SourceLoader": ("src.jobs.interfaces", "SourceLoader"),
    }

    assert "retired the root-symbol compatibility facade" in (jobs_common.__doc__ or "")
    assert not hasattr(jobs_common, "__all__")
    for root_name, (module_name, attr_name) in migration_map.items():
        assert not hasattr(jobs_common, root_name), root_name
        module = importlib.import_module(module_name)
        assert hasattr(module, attr_name), (module_name, attr_name)
