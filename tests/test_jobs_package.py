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


def test_pipeline_module_uses_package_private_helper_boundaries(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "pipeline.py"
    text = target.read_text(encoding="utf-8")
    assert "from src.jobs.pipeline_bootstrap import" in text
    assert "from src.jobs.pipeline_loader_selection import" in text
    assert "from src.jobs.pipeline_runtime import" in text
    assert "from src.jobs import common as common" not in text


def test_static_adapter_uses_package_private_helper_boundary(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "adapters" / "static.py"
    text = target.read_text(encoding="utf-8")
    assert "from src.jobs.adapters.static_helpers import" in text


def test_jobs_modules_avoid_new_broad_common_barrel_imports(repo_root: Path) -> None:
    targets = {
        repo_root / "src" / "jobs" / "transport.py": [],
        repo_root / "src" / "jobs" / "canonicalize.py": [],
        repo_root / "src" / "jobs" / "parsers.py": [],
        repo_root / "src" / "jobs" / "reporting.py": [],
        repo_root / "src" / "jobs" / "state.py": [],
        repo_root / "src" / "jobs" / "pipeline_stage_source_execution.py": [],
        repo_root / "src" / "jobs" / "adapters" / "provider_api.py": [],
        repo_root / "src" / "jobs" / "adapters" / "provider_parsers.py": [],
        repo_root / "src" / "jobs" / "adapters" / "social_parsers.py": [],
        repo_root / "src" / "jobs" / "adapters" / "plugins" / "provider_api" / "register.py": [],
        repo_root / "src" / "jobs" / "adapters" / "plugins" / "social" / "register.py": [],
        repo_root / "src" / "jobs" / "registry.py": [
            "from src.jobs.common import REDUNDANT_STATIC_IF_PROVIDER",
            "from src.jobs.common import DEFAULT_STUDIO_SOURCE_REGISTRY",
        ],
        repo_root / "src" / "jobs" / "adapters" / "__init__.py": [
            "from src.jobs.common import SOCIAL_SOURCE_NAMES, SOURCE_DIAGNOSTICS",
        ],
        repo_root / "src" / "jobs" / "adapters" / "social.py": [
            "from src.jobs.common import SOURCE_DIAGNOSTICS, set_source_diagnostics",
        ],
        repo_root / "src" / "jobs" / "adapters" / "static.py": [
            "from src.jobs.common import registry_entries, set_source_diagnostics",
        ],
        repo_root / "src" / "jobs" / "adapters" / "static_scrapy.py": [
            "from src.jobs.common import registry_entries, set_source_diagnostics",
            "from src.jobs.common import to_iso",
        ],
        repo_root / "src" / "jobs" / "adapters" / "community" / "__init__.py": [
            "from src.jobs.common import (",
        ],
        repo_root / "src" / "jobs" / "common" / "registry.py": [
            "from src.jobs.common import SCRAPY_BROWSER_QUEUE_PATH",
        ],
    }
    for target, forbidden_snippets in targets.items():
        text = target.read_text(encoding="utf-8")
        assert "from src.jobs import common as common" not in text, str(target)
        assert "import src.jobs.common as common" not in text, str(target)
        for snippet in forbidden_snippets:
            assert snippet not in text, str(target)


def test_jobs_common_migrated_modules_keep_direct_owning_imports(repo_root: Path) -> None:
    social_adapter = (repo_root / "src" / "jobs" / "adapters" / "social.py").read_text(
        encoding="utf-8"
    )
    registry_module = (repo_root / "src" / "jobs" / "registry.py").read_text(encoding="utf-8")
    static_adapter = (repo_root / "src" / "jobs" / "adapters" / "static.py").read_text(
        encoding="utf-8"
    )
    assert (
        "from src.jobs.common.diagnostics import SOURCE_DIAGNOSTICS, set_source_diagnostics"
        in social_adapter
    )
    assert (
        "DEFAULT_STUDIO_SOURCE_REGISTRY" in registry_module
        and "REDUNDANT_STATIC_IF_PROVIDER" in registry_module
    )
    assert "registry_entries as common_registry_entries" in registry_module
    assert "from src.jobs.common.diagnostics import set_source_diagnostics" in static_adapter
