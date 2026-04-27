import asyncio
import contextlib
import importlib
import json
import os
import sys
import threading
import time
from pathlib import Path
from unittest import mock

from src import source_discovery as sd
from src import source_registry as sr
from src.source_discovery import config as discovery_config_module
from src.source_discovery import gamesmap as gamesmap_adapter
from src.source_discovery import orchestrator as discovery_orchestrator
from src.source_discovery import url_patches as discovery_url_patches
from src.source_discovery.core import classify_probe_failure_stage
from src.source_discovery.schemas import DiscoveryReportSummarySchema
from src.source_discovery.web_search import async_fetch_text_httpx
from tests.helpers.discovery_runtime import (
    override_discovery_config,
    override_discovery_runtime,
)
from tests.helpers.temp_paths import workspace_tmpdir

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
GENERATOR_DISABLED_DISCOVERY_CONFIG = {
    "sheetDirectory": {"activeAuditEnabled": False},
    "webSearch": {"activeAuditEnabled": False},
    "gamesmap": {"enabled": False},
    "gameprog": {"enabled": False},
    "gamedevmap": {"enabled": False},
}


def _fixture_json(name: str):
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _fixture_text(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def _gamesmap_next_payload_html(companies: list[dict[str, object]]) -> str:
    payload = f'payload-start "companies":{json.dumps(companies, ensure_ascii=False)},"regions":[] payload-end'
    return (
        '<!DOCTYPE html><html lang="en"><body><script>'
        f"self.__next_f.push([1,{json.dumps(payload, ensure_ascii=False)}]);"
        "</script></body></html>"
    )


def discovery_config_without_generator_stages(**overrides: object) -> dict[str, object]:
    config = dict(GENERATOR_DISABLED_DISCOVERY_CONFIG)
    config.update(overrides)
    return config


@contextlib.contextmanager
def patch_empty_generator_stages(*, probe):
    with (
        mock.patch.object(
            discovery_orchestrator,
            "discover_game_studio_sheet_candidates",
            return_value=([], [], []),
        ),
        mock.patch.object(
            discovery_orchestrator,
            "discover_gamesmap_candidates",
            return_value=([], [], []),
        ),
        mock.patch.object(
            discovery_orchestrator,
            "discover_gameprog_candidates",
            return_value=([], [], []),
        ),
        mock.patch.object(
            discovery_orchestrator,
            "discover_web_search_candidates",
            return_value=([], []),
        ),
        mock.patch.object(
            discovery_orchestrator,
            "discover_seed_careers_page_candidates",
            return_value=([], [], []),
        ),
        mock.patch.object(discovery_orchestrator, "async_probe_candidate", side_effect=probe),
    ):
        yield


__all__ = [
    "DiscoveryReportSummarySchema",
    "FIXTURES_DIR",
    "GENERATOR_DISABLED_DISCOVERY_CONFIG",
    "Path",
    "_fixture_json",
    "_fixture_text",
    "_gamesmap_next_payload_html",
    "async_fetch_text_httpx",
    "asyncio",
    "classify_probe_failure_stage",
    "discovery_config_without_generator_stages",
    "discovery_config_module",
    "discovery_orchestrator",
    "discovery_url_patches",
    "gamesmap_adapter",
    "importlib",
    "json",
    "mock",
    "os",
    "override_discovery_config",
    "override_discovery_runtime",
    "patch_empty_generator_stages",
    "sd",
    "sr",
    "sys",
    "threading",
    "time",
    "workspace_tmpdir",
]
