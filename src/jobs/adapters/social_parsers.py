"""Compatibility exports for Reddit, X, and Mastodon social parsers.

AI boundary owns: social parser compatibility exports for legacy callers.
AI boundary implement in: this file for import compatibility only; parser behavior belongs in social_parser leaves.
AI boundary search before contracts: social adapter, social parser modules, and compatibility tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused social parser tests.
"""

from __future__ import annotations

from .social_parser.mastodon_parser import parse_mastodon_payload
from .social_parser.reddit_parser import (
    parse_reddit_html_payload,
    parse_reddit_json_payload,
    parse_reddit_rss_payload,
)
from .social_parser.signals import (
    SOCIAL_APPLY_HOST_HINTS,
    SOCIAL_APPLY_PATH_HINTS,
    SOCIAL_BLOCKED_HOSTS,
    SOCIAL_BLOCKED_PATH_HINTS,
    SOCIAL_CONTENT_ONLY_HOST_HINTS,
    SOCIAL_CONTENT_ONLY_PATH_HINTS,
    SOCIAL_DISCUSSION_PHRASES,
    SOCIAL_EXPLICIT_OPENING_PHRASES,
    SOCIAL_FOR_HIRE_KEYWORDS,
    SOCIAL_HIRING_KEYWORDS,
    SOCIAL_NEGATIVE_NOT_HIRING_PHRASES,
    _as_dict,
    _as_list,
    _clean_text,
    _increment_reason,
    _norm_text,
    social_compute_confidence,
    social_evaluate_post,
    social_extract_apply_url,
    social_extract_urls,
    social_has_explicit_opening_signal,
    social_has_negative_hiring_signal,
    social_has_social_repost_only,
    social_infer_company,
    social_is_content_only_url,
    social_is_job_destination_url,
    social_should_reject_non_job_reddit_post,
)
from .social_parser.x_parser import parse_x_payload, parse_x_rss_payload

__all__ = [
    "SOCIAL_APPLY_HOST_HINTS",
    "SOCIAL_APPLY_PATH_HINTS",
    "SOCIAL_BLOCKED_HOSTS",
    "SOCIAL_BLOCKED_PATH_HINTS",
    "SOCIAL_CONTENT_ONLY_HOST_HINTS",
    "SOCIAL_CONTENT_ONLY_PATH_HINTS",
    "SOCIAL_DISCUSSION_PHRASES",
    "SOCIAL_EXPLICIT_OPENING_PHRASES",
    "SOCIAL_FOR_HIRE_KEYWORDS",
    "SOCIAL_HIRING_KEYWORDS",
    "SOCIAL_NEGATIVE_NOT_HIRING_PHRASES",
    "_as_dict",
    "_as_list",
    "_clean_text",
    "_increment_reason",
    "_norm_text",
    "parse_mastodon_payload",
    "parse_reddit_html_payload",
    "parse_reddit_json_payload",
    "parse_reddit_rss_payload",
    "parse_x_payload",
    "parse_x_rss_payload",
    "social_compute_confidence",
    "social_evaluate_post",
    "social_extract_apply_url",
    "social_extract_urls",
    "social_has_explicit_opening_signal",
    "social_has_negative_hiring_signal",
    "social_has_social_repost_only",
    "social_infer_company",
    "social_is_content_only_url",
    "social_is_job_destination_url",
    "social_should_reject_non_job_reddit_post",
]
