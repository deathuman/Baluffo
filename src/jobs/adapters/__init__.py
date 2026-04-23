"""Adapter registry accessors for the jobs package."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from src.jobs.adapters import community, provider_api, social, static
from src.jobs.common.http import default_fetch_text as common_default_fetch_text
from src.jobs.interfaces import SourceLoader
from src.jobs.models import FetchContext, FetchResult, SourceDiagnostics
from src.jobs.text_utils import clean_text
from src.jobs_fetcher_registry import DEFAULT_SOURCE_LOADER_NAMES

from ..common import config as common_config
from ..common import social as common_social


def _run_loader_fetch_text(url: str, timeout_s: int) -> str:
    return common_default_fetch_text(url, timeout_s, headers={})


def default_source_loaders(
    *,
    social_enabled: bool = False,
    social_config: dict[str, Any] | None = None,
) -> list[tuple[str, SourceLoader]]:
    social_cfg = (
        social_config
        if isinstance(social_config, dict)
        else common_social.load_social_config(
            config_path=common_config.DEFAULT_SOCIAL_CONFIG_PATH,
            enabled=bool(social_enabled),
            lookback_minutes=common_config.DEFAULT_SOCIAL_LOOKBACK_MINUTES,
        )
    )

    google_sheet_loaders: dict[str, SourceLoader] = {}
    for source in community.GOOGLE_SHEETS_SOURCES:
        source_name = clean_text(source.get("name"))
        sheet_id = clean_text(source.get("sheetId"))
        gid = clean_text(source.get("gid") or "0")
        if not source_name or not sheet_id:
            continue

        def _loader(
            *,
            fetch_text: Callable[[str, int], str],
            timeout_s: int,
            retries: int,
            backoff_s: float,
            _sheet_id: str = sheet_id,
            _gid: str = gid,
            _source_name: str = source_name,
            heartbeat_callback: Callable[[], None] | None = None,
        ) -> list[dict[str, Any]]:
            return community.run_google_sheets_source(
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                sheet_id=_sheet_id,
                gid=_gid,
                diagnostics_name=_source_name,
                heartbeat_callback=heartbeat_callback,
            )

        google_sheet_loaders[source_name] = _loader

    def _run_social_reddit(**kwargs: Any) -> list[dict[str, Any]]:
        return social.run_social_reddit_source(**kwargs, social_config=social_cfg)

    def _run_social_x(**kwargs: Any) -> list[dict[str, Any]]:
        return social.run_social_x_source(**kwargs, social_config=social_cfg)

    def _run_social_mastodon(**kwargs: Any) -> list[dict[str, Any]]:
        return social.run_social_mastodon_source(**kwargs, social_config=social_cfg)

    available: dict[str, SourceLoader] = {
        **google_sheet_loaders,
        "remote_ok": community.run_remote_ok_source,
        "gamesindustry": community.run_gamesindustry_source,
        "gamejobs": community.run_gamejobs_source,
        "workwithindies": community.run_workwithindies_source,
        "8bitplay": community.run_8bitplay_source,
        "gracklehq": community.run_gracklehq_source,
        "epic_games_careers": community.run_epic_games_careers_source,
        "greenhouse_boards": provider_api.run_greenhouse_boards_source,
        "teamtailor_sources": provider_api.run_teamtailor_sources_source,
        "lever_sources": provider_api.run_lever_sources_source,
        "smartrecruiters_sources": provider_api.run_smartrecruiters_sources_source,
        "workable_sources": provider_api.run_workable_sources_source,
        "recruitee_sources": provider_api.run_recruitee_sources_source,
        "pinpoint_sources": provider_api.run_pinpoint_sources_source,
        "ashby_sources": provider_api.run_ashby_sources_source,
        "bamboohr_sources": provider_api.run_bamboohr_sources_source,
        "breezy_sources": provider_api.run_breezy_sources_source,
        "jazzhr_sources": provider_api.run_jazzhr_sources_source,
        "workday_sources": provider_api.run_workday_sources_source,
        "personio_sources": provider_api.run_personio_sources_source,
        "scrapy_static_sources": static.run_scrapy_static_source,
        "social_reddit": _run_social_reddit,
        "social_x": _run_social_x,
        "social_mastodon": _run_social_mastodon,
        "static_studio_pages_a_i": static.run_static_studio_pages_a_i_source,
        "static_studio_pages_j_r": static.run_static_studio_pages_j_r_source,
        "static_studio_pages_s_z": static.run_static_studio_pages_s_z_source,
        "static_studio_pages": static.run_static_studio_pages_source,
    }
    base_loaders = [
        (name, available[name]) for name in DEFAULT_SOURCE_LOADER_NAMES if name in available
    ]
    base_loaders = [
        (name, loader)
        for name, loader in base_loaders
        if name
        not in {
            "static_studio_pages",
            "static_studio_pages_a_i",
            "static_studio_pages_j_r",
            "static_studio_pages_s_z",
        }
    ]
    if not bool(social_cfg.get("enabled")):
        base_loaders = [
            (name, loader)
            for name, loader in base_loaders
            if name not in common_social.SOCIAL_SOURCE_NAMES
        ]
    return base_loaders + static.build_static_source_loaders()


EXTRACTED_ADAPTERS = {
    "google_sheets": community.run_google_sheets_source,
    "remote_ok": community.run_remote_ok_source,
    "gamesindustry": community.run_gamesindustry_source,
    "gamejobs": community.run_gamejobs_source,
    "workwithindies": community.run_workwithindies_source,
    "8bitplay": community.run_8bitplay_source,
    "gracklehq": community.run_gracklehq_source,
    "epic_games_careers": community.run_epic_games_careers_source,
    "wellfound": community.run_wellfound_source,
    "social_reddit": social.run_social_reddit_source,
    "social_x": social.run_social_x_source,
    "social_mastodon": social.run_social_mastodon_source,
    "greenhouse_boards": provider_api.run_greenhouse_boards_source,
    "teamtailor_sources": provider_api.run_teamtailor_sources_source,
    "lever_sources": provider_api.run_lever_sources_source,
    "smartrecruiters_sources": provider_api.run_smartrecruiters_sources_source,
    "workable_sources": provider_api.run_workable_sources_source,
    "recruitee_sources": provider_api.run_recruitee_sources_source,
    "pinpoint_sources": provider_api.run_pinpoint_sources_source,
    "ashby_sources": provider_api.run_ashby_sources_source,
    "bamboohr_sources": provider_api.run_bamboohr_sources_source,
    "breezy_sources": provider_api.run_breezy_sources_source,
    "jazzhr_sources": provider_api.run_jazzhr_sources_source,
    "workday_sources": provider_api.run_workday_sources_source,
    "personio_sources": provider_api.run_personio_sources_source,
    "scrapy_static_sources": static.run_scrapy_static_source,
    "static_studio_pages": static.run_static_studio_pages_source,
    "static_studio_pages_a_i": static.run_static_studio_pages_a_i_source,
    "static_studio_pages_j_r": static.run_static_studio_pages_j_r_source,
    "static_studio_pages_s_z": static.run_static_studio_pages_s_z_source,
}


def run_loader(name: str, loader: SourceLoader, ctx: FetchContext) -> FetchResult:
    jobs = loader(
        fetch_text=_run_loader_fetch_text,
        timeout_s=ctx.request.timeout_s,
        retries=ctx.retries,
        backoff_s=ctx.backoff_s,
    )
    diagnostics_payload = common_config.SOURCE_DIAGNOSTICS.get(name)
    diagnostics = None
    if isinstance(diagnostics_payload, dict):
        diagnostics = SourceDiagnostics(
            adapter=str(diagnostics_payload.get("adapter") or "unknown"),
            studio=str(diagnostics_payload.get("studio") or "multiple"),
            details=[
                dict(item)
                for item in diagnostics_payload.get("details") or []
                if isinstance(item, dict)
            ],
            partial_errors=[str(item) for item in diagnostics_payload.get("partialErrors") or []],
            low_confidence_dropped=int(diagnostics_payload.get("lowConfidenceDropped") or 0),
        )
    return FetchResult(jobs=jobs, diagnostics=diagnostics)
