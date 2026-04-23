import ast
import importlib
import subprocess
import sys
from pathlib import Path
from unittest import mock

from src.jobs import adapters, canonicalize, dedup, parsers, registry, transport
from src.jobs import common as jobs_common
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


def test_pipeline_module_uses_package_private_helper_boundaries(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "pipeline.py"
    text = target.read_text(encoding="utf-8")
    assert "from . import pipeline_run_setup as pipeline_run_setup_mod" in text
    assert "from . import pipeline_execution_flow as pipeline_execution_flow_mod" in text
    assert "from src.jobs import common as common" not in text


def test_state_module_uses_package_private_helper_boundaries(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "state.py"
    text = target.read_text(encoding="utf-8")
    assert "from . import state_incremental as state_incremental_mod" in text
    assert "from . import state_lifecycle as state_lifecycle_mod" in text
    assert "from . import state_source_state as state_source_state_mod" in text
    assert "def normalize_source_state_payload(" not in text
    assert "def apply_job_lifecycle_state(" not in text


def test_pipeline_stage_execution_module_uses_package_private_helper_boundaries(
    repo_root: Path,
) -> None:
    target = repo_root / "src" / "jobs" / "pipeline_stage_source_execution.py"
    text = target.read_text(encoding="utf-8")
    assert "from . import pipeline_source_loop as pipeline_source_loop_mod" in text
    assert "from . import pipeline_source_progress as pipeline_source_progress_mod" in text
    assert "from . import pipeline_source_results as pipeline_source_results_mod" in text
    assert "pipeline_source_loop_mod.root = sys.modules[__name__]" in text
    assert "pipeline_source_progress_mod.root = sys.modules[__name__]" in text
    assert "pipeline_source_results_mod.root = sys.modules[__name__]" in text
    assert "def emit_progress_line(" not in text
    assert "def mark_task_started(" not in text
    assert "def execute_loader(" not in text


def test_pipeline_runtime_module_uses_package_private_helper_boundaries(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "pipeline_runtime.py"
    text = target.read_text(encoding="utf-8")
    assert "from .pipeline_runtime_summary import (" in text
    assert "from .pipeline_runtime_writers import (" in text
    assert "def initialize_task_runtime(" not in text
    assert "def build_active_pipeline_summary(" not in text


def test_state_source_state_module_uses_package_private_helper_boundaries(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "state_source_state.py"
    text = target.read_text(encoding="utf-8")
    assert "from .state_source_browser import (" in text
    assert "from .state_source_migration import (" in text
    assert "from .state_source_records import (" in text
    assert "def normalize_source_state_payload(" not in text
    assert "def apply_successful_source_state(" not in text
    assert "def apply_browser_escalation_state(" not in text


def test_jobs_contracts_module_uses_package_private_helper_boundaries(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "common" / "contracts.py"
    text = target.read_text(encoding="utf-8")
    assert "from .contracts_fetch_report import normalize_fetch_report_payload" in text
    assert "from .contracts_runtime import normalize_runtime_payload" in text
    assert "from .contracts_source_reports import normalize_source_report_row" in text
    assert "from .contracts_task_state import normalize_task_state_payload" in text
    assert "def normalize_runtime_payload(" not in text
    assert "def normalize_source_report_row(" not in text
    assert "def normalize_task_state_payload(" not in text
    assert "def normalize_fetch_report_payload(" not in text


def test_jobs_reporting_module_uses_package_private_helper_boundaries(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "reporting.py"
    text = target.read_text(encoding="utf-8")
    assert "from .reporting_breakdowns import (" in text
    assert "from .reporting_queues import (" in text
    assert "from .reporting_social import (" in text
    assert "from .reporting_summary import build_pipeline_summary, format_source_error" in text
    assert "def build_pipeline_summary(" not in text
    assert "def build_browser_fallback_queue(" not in text
    assert "def build_parser_regression_queue(" not in text
    assert "def build_social_experiment_review_sample(" not in text


def test_static_adapter_uses_package_private_helper_boundary(repo_root: Path) -> None:
    target = repo_root / "src" / "jobs" / "adapters" / "static.py"
    text = target.read_text(encoding="utf-8")
    assert "from src.jobs.adapters.static_helpers import" in text


def test_jobs_modules_avoid_new_broad_common_barrel_imports(repo_root: Path) -> None:
    allowed_submodules = {
        "config",
        "contracts",
        "datetime_utils",
        "diagnostics",
        "fetch",
        "health",
        "heuristics",
        "numbers",
        "registry",
        "registry_defaults",
        "social",
        "sources",
        "taxonomy",
        "url",
    }
    offenders: list[str] = []
    for root_name in ("src", "tests"):
        for target in (repo_root / root_name).rglob("*.py"):
            if target.resolve() == Path(__file__).resolve():
                continue
            text = target.read_text(encoding="utf-8")
            tree = ast.parse(text, filename=str(target))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name == "src.jobs.common":
                            offenders.append(str(target.relative_to(repo_root)))
                if isinstance(node, ast.ImportFrom):
                    if node.module == "src.jobs":
                        if any(alias.name == "common" for alias in node.names):
                            offenders.append(str(target.relative_to(repo_root)))
                    if node.module == "src.jobs.common":
                        for alias in node.names:
                            if alias.name not in allowed_submodules:
                                offenders.append(str(target.relative_to(repo_root)))
    assert not offenders, "Found retired broad src.jobs.common imports:\n- " + "\n- ".join(
        offenders
    )


def test_legacy_runners_module_is_retired(repo_root: Path) -> None:
    legacy_module = repo_root / "src" / "jobs" / "common" / "legacy_runners.py"
    assert not legacy_module.exists()

    offenders: list[str] = []
    for root_name in ("src", "tests"):
        for target in (repo_root / root_name).rglob("*.py"):
            text = target.read_text(encoding="utf-8")
            tree = ast.parse(text, filename=str(target))
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.ImportFrom)
                    and node.module == "src.jobs.common.legacy_runners"
                ):
                    offenders.append(str(target.relative_to(repo_root)))
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name == "src.jobs.common.legacy_runners":
                            offenders.append(str(target.relative_to(repo_root)))
    assert not offenders, "Found retired src.jobs.common.legacy_runners imports:\n- " + "\n- ".join(
        offenders
    )


def test_fetcher_test_helpers_do_not_reintroduce_helper_barrel_patterns(repo_root: Path) -> None:
    helper_path = repo_root / "tests" / "jobs_fetcher_helpers.py"
    helper_text = helper_path.read_text(encoding="utf-8")
    for retired_export in (
        '"subprocess"',
        '"sys"',
        '"threading"',
        '"time"',
        '"json"',
        '"os"',
        '"mock"',
        '"pytest"',
        '"Path"',
    ):
        assert retired_export not in helper_text.split("__all__ = [", 1)[1].split("]", 1)[0]

    offenders: list[str] = []
    for target in (repo_root / "tests").rglob("*.py"):
        if target.resolve() == Path(__file__).resolve():
            continue
        text = target.read_text(encoding="utf-8")
        if "from tests.jobs_fetcher_helpers import *" in text:
            offenders.append(str(target.relative_to(repo_root)))
    assert not offenders, (
        "Found retired tests.jobs_fetcher_helpers star imports:\n- " + "\n- ".join(offenders)
    )

    jobs_static_helper = repo_root / "tests" / "jobs_static" / "_helpers.py"
    jobs_static_helper_text = jobs_static_helper.read_text(encoding="utf-8")
    assert "__all__ = [name for name in globals()" not in jobs_static_helper_text


def test_jobs_common_migrated_modules_keep_direct_owning_imports(repo_root: Path) -> None:
    social_adapter = (repo_root / "src" / "jobs" / "adapters" / "social.py").read_text(
        encoding="utf-8"
    )
    registry_module = (repo_root / "src" / "jobs" / "registry.py").read_text(encoding="utf-8")
    static_adapter = (repo_root / "src" / "jobs" / "adapters" / "static.py").read_text(
        encoding="utf-8"
    )
    static_scrapy_adapter = (
        repo_root / "src" / "jobs" / "adapters" / "static_scrapy.py"
    ).read_text(encoding="utf-8")
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
    assert "from src.jobs.registry import registry_entries" in static_scrapy_adapter
