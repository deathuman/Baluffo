import os
from unittest import mock

from src.source_discovery import config as discovery_config_module
from src.source_discovery import core as discovery_core
from src.source_discovery.core_scoring import probe_concurrency_defaults
from src.source_discovery.directory_fetch import directory_fetch_concurrency_defaults


def test_probe_concurrency_defaults_preserve_env_edge_case_fallbacks() -> None:
    with mock.patch.dict(
        os.environ,
        {
            "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL": "0",
            "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_STATIC": "",
            "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_PROVIDER": "invalid",
            "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TEAMTAILOR": "-5",
        },
        clear=False,
    ):
        assert probe_concurrency_defaults() == {
            "total": 1,
            "static": 16,
            "provider": 40,
            "teamtailor": 1,
        }


def test_compute_confidence_preserves_provider_and_job_thresholds() -> None:
    assert discovery_core.compute_confidence({"adapter": "lever", "evidenceScore": 39}, 0) == "low"
    assert (
        discovery_core.compute_confidence({"adapter": "lever", "evidenceScore": 40}, 0) == "medium"
    )
    assert (
        discovery_core.compute_confidence({"adapter": "static", "evidenceScore": 100}, 0) == "low"
    )
    assert (
        discovery_core.compute_confidence({"adapter": "unknown", "evidenceScore": 100}, 0) == "low"
    )
    assert (
        discovery_core.compute_confidence({"adapter": "static", "evidenceScore": 0}, 1) == "medium"
    )
    assert discovery_core.compute_confidence({"adapter": "static", "evidenceScore": 0}, 5) == "high"


def test_structured_batch_adapters_export_matches_config() -> None:
    assert (
        discovery_core.STRUCTURED_BATCH_ADAPTERS
        == discovery_config_module.STRUCTURED_BATCH_ADAPTERS
    )
    assert discovery_core.STRUCTURED_BATCH_ADAPTERS == frozenset({"greenhouse", "lever", "ashby"})


def test_directory_fetch_concurrency_defaults_preserve_env_edge_case_fallbacks() -> None:
    with mock.patch.dict(
        os.environ,
        {
            "BALUFFO_DISCOVERY_DIRECTORY_FETCH_CONCURRENCY_TOTAL": "0",
            "BALUFFO_DISCOVERY_DIRECTORY_FETCH_CONCURRENCY_PER_HOST": "invalid",
        },
        clear=False,
    ):
        assert directory_fetch_concurrency_defaults() == {"total": 1, "perHost": 2}
