"""Shared stdlib-only text helpers.

AI boundary owns: small text coercion helpers shared outside jobs-specific normalization.
AI boundary implement in: this file for generic text helpers; jobs text normalization belongs in src.jobs.text_utils.
AI boundary search before contracts: jobs text utilities, bridge route helpers, and callers that need stdlib-only text cleanup.
AI boundary verify: `npm run lint:repo-guardrails` plus focused text helper tests.
"""

from __future__ import annotations

from typing import Any


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def norm_text(value: Any) -> str:
    return clean_text(value).lower()
