"""Extraction-only parsers for jobs content."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from scripts.jobs import common
from scripts.jobs.models import RawJob

extract_json_ld_blocks = common.extract_json_ld_blocks
strip_html_text = common.strip_html_text
parse_gamesindustry_changed_date = common.parse_gamesindustry_changed_date
iter_job_postings_from_jsonld = common.iter_job_postings_from_jsonld
parse_jobposting_locations = common.parse_jobposting_locations
parse_jobposting_company = common.parse_jobposting_company
parse_jobposting_source_id = common.parse_jobposting_source_id
maybe_fetch_kojima_job_listing_html = common.maybe_fetch_kojima_job_listing_html
parse_teamtailor_listing_links = common.parse_teamtailor_listing_links
parse_gamesindustry_html = common.parse_gamesindustry_html
parse_google_sheets_csv = common.parse_google_sheets_csv
parse_remote_ok_payload = common.parse_remote_ok_payload
parse_reddit_json_payload = common.parse_reddit_json_payload
parse_reddit_rss_payload = common.parse_reddit_rss_payload
parse_x_payload = common.parse_x_payload
parse_x_rss_payload = common.parse_x_rss_payload
parse_mastodon_payload = common.parse_mastodon_payload
parse_greenhouse_jobs_payload = common.parse_greenhouse_jobs_payload
parse_lever_jobs_payload = common.parse_lever_jobs_payload
parse_smartrecruiters_jobs_payload = common.parse_smartrecruiters_jobs_payload
parse_workable_jobs_payload = common.parse_workable_jobs_payload
parse_epic_games_jobs_payload = common.parse_epic_games_jobs_payload
parse_ashby_jobs_from_html = common.parse_ashby_jobs_from_html
parse_personio_feed_xml = common.parse_personio_feed_xml


def parse_jobpostings_from_html(
    html_text: str,
    *,
    base_url: str,
    fallback_company: str = "",
    fallback_source_id_prefix: str = "",
) -> List[RawJob]:
    return common.parse_jobpostings_from_html(
        html_text,
        base_url=base_url,
        fallback_company=fallback_company,
        fallback_source_id_prefix=fallback_source_id_prefix,
    )
