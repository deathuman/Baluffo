"""Adapter registry accessors for the jobs package."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from scripts.jobs import common
from scripts.jobs.adapters import community, provider_api, social, static
from scripts.jobs.models import FetchContext, FetchResult, SourceDiagnostics


def default_source_loaders(
    *,
    social_enabled: bool = False,
    social_config: Optional[Dict[str, Any]] = None,
) -> List[Tuple[str, common.SourceLoader]]:
    social_cfg = social_config if isinstance(social_config, dict) else common.load_social_config(
        config_path=common.DEFAULT_SOCIAL_CONFIG_PATH,
        enabled=bool(social_enabled),
        lookback_minutes=common.DEFAULT_SOCIAL_LOOKBACK_MINUTES,
    )

    google_sheet_loaders: Dict[str, common.SourceLoader] = {}
    for source in common.GOOGLE_SHEETS_SOURCES:
        source_name = common.clean_text(source.get("name"))
        sheet_id = common.clean_text(source.get("sheetId"))
        gid = common.clean_text(source.get("gid") or "0")
        if not source_name or not sheet_id:
            continue

        def _loader(
            *,
            fetch_text,
            timeout_s: int,
            retries: int,
            backoff_s: float,
            _sheet_id: str = sheet_id,
            _gid: str = gid,
            _source_name: str = source_name,
        ):
            return community.run_google_sheets_source(
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                sheet_id=_sheet_id,
                gid=_gid,
                diagnostics_name=_source_name,
            )

        google_sheet_loaders[source_name] = _loader

    available: Dict[str, common.SourceLoader] = {
        **google_sheet_loaders,
        "remote_ok": community.run_remote_ok_source,
        "gamesindustry": community.run_gamesindustry_source,
        "epic_games_careers": community.run_epic_games_careers_source,
        "greenhouse_boards": provider_api.run_greenhouse_boards_source,
        "teamtailor_sources": provider_api.run_teamtailor_sources_source,
        "lever_sources": provider_api.run_lever_sources_source,
        "smartrecruiters_sources": provider_api.run_smartrecruiters_sources_source,
        "workable_sources": provider_api.run_workable_sources_source,
        "ashby_sources": provider_api.run_ashby_sources_source,
        "personio_sources": provider_api.run_personio_sources_source,
        "scrapy_static_sources": static.run_scrapy_static_source,
        "social_reddit": lambda **kwargs: social.run_social_reddit_source(**kwargs, social_config=social_cfg),
        "social_x": lambda **kwargs: social.run_social_x_source(**kwargs, social_config=social_cfg),
        "social_mastodon": lambda **kwargs: social.run_social_mastodon_source(**kwargs, social_config=social_cfg),
        "static_studio_pages_a_i": static.run_static_studio_pages_a_i_source,
        "static_studio_pages_j_r": static.run_static_studio_pages_j_r_source,
        "static_studio_pages_s_z": static.run_static_studio_pages_s_z_source,
        "static_studio_pages": static.run_static_studio_pages_source,
    }
    base_loaders = [(name, available[name]) for name in common.DEFAULT_SOURCE_LOADER_NAMES if name in available]
    base_loaders = [
        (name, loader)
        for name, loader in base_loaders
        if name not in {"static_studio_pages", "static_studio_pages_a_i", "static_studio_pages_j_r", "static_studio_pages_s_z"}
    ]
    if not bool(social_cfg.get("enabled")):
        base_loaders = [(name, loader) for name, loader in base_loaders if name not in common.SOCIAL_SOURCE_NAMES]
    return base_loaders + static.build_static_source_loaders()


EXTRACTED_ADAPTERS = {
    "google_sheets": community.run_google_sheets_source,
    "remote_ok": community.run_remote_ok_source,
    "gamesindustry": community.run_gamesindustry_source,
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
    "ashby_sources": provider_api.run_ashby_sources_source,
    "personio_sources": provider_api.run_personio_sources_source,
    "scrapy_static_sources": static.run_scrapy_static_source,
    "static_studio_pages": static.run_static_studio_pages_source,
    "static_studio_pages_a_i": static.run_static_studio_pages_a_i_source,
    "static_studio_pages_j_r": static.run_static_studio_pages_j_r_source,
    "static_studio_pages_s_z": static.run_static_studio_pages_s_z_source,
}


def run_loader(name: str, loader: common.SourceLoader, ctx: FetchContext) -> FetchResult:
    jobs = loader(
        fetch_text=common.default_fetch_text,
        timeout_s=ctx.request.timeout_s,
        retries=ctx.retries,
        backoff_s=ctx.backoff_s,
    )
    diagnostics_payload = common.SOURCE_DIAGNOSTICS.get(name)
    diagnostics = None
    if isinstance(diagnostics_payload, dict):
        diagnostics = SourceDiagnostics(
            adapter=str(diagnostics_payload.get("adapter") or "unknown"),
            studio=str(diagnostics_payload.get("studio") or "multiple"),
            details=[dict(item) for item in diagnostics_payload.get("details") or [] if isinstance(item, dict)],
            partial_errors=[str(item) for item in diagnostics_payload.get("partialErrors") or []],
            low_confidence_dropped=int(diagnostics_payload.get("lowConfidenceDropped") or 0),
        )
    return FetchResult(jobs=jobs, diagnostics=diagnostics)

