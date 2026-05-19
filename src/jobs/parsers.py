"""Extraction-only parsers for jobs content.

This module is a re-export shim. Provider parser functions are re-exported via
src.jobs.adapters.provider_parsers, which itself is a compat shim pointing to
the canonical location: src.jobs.adapters.parsers
"""

from __future__ import annotations

from typing import Any

from src.jobs.adapters import community
from src.jobs.adapters import html_parsers as _html_parsers
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.adapters import social_parsers as _social_parsers
from src.jobs.common.parsing import parse_remote_ok_payload as _parse_remote_ok_payload
from src.jobs.game_detection import looks_like_game_job
from src.jobs.models import RawJob

extract_json_ld_blocks = _html_parsers.extract_json_ld_blocks
strip_html_text = _html_parsers.strip_html_text
parse_gamesindustry_changed_date = _html_parsers.parse_gamesindustry_changed_date
iter_job_postings_from_jsonld = _html_parsers.iter_job_postings_from_jsonld
parse_jobposting_locations = _html_parsers.parse_jobposting_locations
parse_jobposting_company = _html_parsers.parse_jobposting_company
parse_jobposting_source_id = _html_parsers.parse_jobposting_source_id
maybe_fetch_kojima_job_listing_html = _html_parsers.maybe_fetch_kojima_job_listing_html
parse_teamtailor_listing_links = _html_parsers.parse_teamtailor_listing_links
parse_gamesindustry_html = _html_parsers.parse_gamesindustry_html
parse_wellfound_html = _html_parsers.parse_wellfound_html


def parse_jobpostings_from_html(
    html_text: str,
    *,
    base_url: str,
    fallback_company: str = "",
    fallback_source_id_prefix: str = "",
) -> list[RawJob]:
    return _html_parsers.parse_jobpostings_from_html(
        html_text,
        base_url=base_url,
        fallback_company=fallback_company,
        fallback_source_id_prefix=fallback_source_id_prefix,
    )


parse_google_sheets_csv = community.parse_google_sheets_csv
parse_gamejobs_html = community.parse_gamejobs_html
parse_workwithindies_html = community.parse_workwithindies_html
parse_8bitplay_html = community.parse_8bitplay_html
parse_gracklehq_html = community.parse_gracklehq_html
parse_reddit_json_payload = _social_parsers.parse_reddit_json_payload
parse_reddit_rss_payload = _social_parsers.parse_reddit_rss_payload
parse_x_payload = _social_parsers.parse_x_payload
parse_x_rss_payload = _social_parsers.parse_x_rss_payload
parse_mastodon_payload = _social_parsers.parse_mastodon_payload
parse_greenhouse_jobs_payload = _provider_parsers.parse_greenhouse_jobs_payload
parse_lever_jobs_payload = _provider_parsers.parse_lever_jobs_payload
parse_oracle_hcm_requisitions_payload = _provider_parsers.parse_oracle_hcm_requisitions_payload
parse_smartrecruiters_jobs_payload = _provider_parsers.parse_smartrecruiters_jobs_payload
parse_workable_jobs_payload = _provider_parsers.parse_workable_jobs_payload
parse_recruitee_jobs_payload = _provider_parsers.parse_recruitee_jobs_payload
parse_pinpoint_jobs_payload = _provider_parsers.parse_pinpoint_jobs_payload
parse_epic_games_jobs_payload = _provider_parsers.parse_epic_games_jobs_payload
parse_ashby_jobs_from_html = _provider_parsers.parse_ashby_jobs_from_html
parse_breezy_jobs_html = _provider_parsers.parse_breezy_jobs_html
parse_bamboohr_jobs_html = _provider_parsers.parse_bamboohr_jobs_html
parse_jazzhr_jobs_html = _provider_parsers.parse_jazzhr_jobs_html
parse_personio_feed_xml = _provider_parsers.parse_personio_feed_xml
parse_workday_jobs_html = _provider_parsers.parse_workday_jobs_html


def parse_remote_ok_payload(payload: Any) -> list[RawJob]:
    return _parse_remote_ok_payload(payload, looks_like_game_job=looks_like_game_job)
