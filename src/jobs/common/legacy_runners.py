from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .config import DEFAULT_STATIC_DETAIL_CONCURRENCY

RawJob = dict[str, Any]


def _community_adapter():
    from src.jobs.adapters import community

    return community


def _social_adapter():
    from src.jobs.adapters import social

    return social


def _provider_api_adapter():
    from src.jobs.adapters import provider_api

    return provider_api


def _static_adapter():
    from src.jobs.adapters import static

    return static


def run_remote_ok_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _community_adapter().run_remote_ok_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def _request_json_with_headers(
    url: str, *, timeout_s: int, headers: dict[str, str] | None = None
) -> dict[str, Any]:
    return _social_adapter()._request_json_with_headers(url, timeout_s=timeout_s, headers=headers)


def run_social_reddit_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: dict[str, Any],
) -> list[RawJob]:
    return _social_adapter().run_social_reddit_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        social_config=social_config,
    )


def run_social_x_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: dict[str, Any],
) -> list[RawJob]:
    return _social_adapter().run_social_x_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        social_config=social_config,
    )


def run_social_mastodon_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: dict[str, Any],
) -> list[RawJob]:
    return _social_adapter().run_social_mastodon_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        social_config=social_config,
    )


def run_gamesindustry_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _community_adapter().run_gamesindustry_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_epic_games_careers_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _community_adapter().run_epic_games_careers_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_wellfound_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _community_adapter().run_wellfound_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_greenhouse_boards_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _provider_api_adapter().run_greenhouse_boards_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_teamtailor_sources_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _provider_api_adapter().run_teamtailor_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_scrapy_static_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> list[RawJob]:
    return _static_adapter().run_scrapy_static_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def static_source_shard(row: dict[str, Any]) -> str:
    return _static_adapter().static_source_shard(row)


def run_static_studio_pages_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    sources: list[dict[str, Any]] | None = None,
    shard: str | None = None,
    diagnostics_name: str = "static_studio_pages",
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
) -> list[RawJob]:
    return _static_adapter().run_static_studio_pages_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        sources=sources,
        shard=shard,
        diagnostics_name=diagnostics_name,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def run_static_source_entry_source(
    *,
    source_row: dict[str, Any],
    diagnostics_name: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
) -> list[RawJob]:
    return _static_adapter().run_static_source_entry_source(
        source_row=source_row,
        diagnostics_name=diagnostics_name,
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def run_static_studio_pages_a_i_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
) -> list[RawJob]:
    return _static_adapter().run_static_studio_pages_a_i_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def build_static_source_loaders() -> list[tuple[str, Any]]:
    return _static_adapter().build_static_source_loaders()


def run_static_studio_pages_j_r_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
) -> list[RawJob]:
    return _static_adapter().run_static_studio_pages_j_r_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def run_static_studio_pages_s_z_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    static_detail_concurrency: int = DEFAULT_STATIC_DETAIL_CONCURRENCY,
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    try_playwright: Callable[[str, int], tuple[str, str]] | None = None,
) -> list[RawJob]:
    return _static_adapter().run_static_studio_pages_s_z_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        static_detail_concurrency=static_detail_concurrency,
        source_state_rows=source_state_rows,
        try_playwright=try_playwright,
    )


def run_lever_sources_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _provider_api_adapter().run_lever_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_smartrecruiters_sources_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _provider_api_adapter().run_smartrecruiters_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_workable_sources_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _provider_api_adapter().run_workable_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_ashby_sources_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _provider_api_adapter().run_ashby_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_bamboohr_sources_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _provider_api_adapter().run_bamboohr_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_personio_sources_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _provider_api_adapter().run_personio_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


def run_workday_sources_source(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    return _provider_api_adapter().run_workday_sources_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )


__all__ = [
    "RawJob",
    "_request_json_with_headers",
    "build_static_source_loaders",
    "run_ashby_sources_source",
    "run_epic_games_careers_source",
    "run_gamesindustry_source",
    "run_greenhouse_boards_source",
    "run_lever_sources_source",
    "run_personio_sources_source",
    "run_remote_ok_source",
    "run_scrapy_static_source",
    "run_smartrecruiters_sources_source",
    "run_social_mastodon_source",
    "run_social_reddit_source",
    "run_social_x_source",
    "run_static_source_entry_source",
    "run_static_studio_pages_a_i_source",
    "run_static_studio_pages_j_r_source",
    "run_static_studio_pages_s_z_source",
    "run_static_studio_pages_source",
    "run_teamtailor_sources_source",
    "run_wellfound_source",
    "run_workable_sources_source",
    "static_source_shard",
]
