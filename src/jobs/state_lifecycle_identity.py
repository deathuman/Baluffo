"""Lifecycle identity helpers.

Extracted from state_lifecycle.py as part of the lifecycle split.

AI boundary owns: lifecycle identity aliases and alias index helpers.
AI boundary implement in: this file for identity aliases; availability and normalization stay in sibling leaves.
AI boundary search before contracts: lifecycle state tests and pipeline finalization.
AI boundary verify: `npm run lint:repo-guardrails` plus focused lifecycle tests.
"""

from __future__ import annotations

import hashlib
from typing import Any

from src.jobs.common.url import fingerprint_url
from src.jobs.state_lifecycle_availability import _normalize_availability_aliases
from src.jobs.text_utils import clean_text


def job_identity_aliases(job: dict[str, Any]) -> list[str]:
    aliases: list[str] = []
    availability_id = clean_text(job.get("availabilityId"))
    if availability_id:
        aliases.append(f"availability:{availability_id}")
    source = clean_text(job.get("source"))
    source_job_id = clean_text(job.get("sourceJobId"))
    if source and source_job_id:
        aliases.append(f"source:{source.casefold()}:{source_job_id.casefold()}")
    link_fp = fingerprint_url(job.get("jobLink"))
    if link_fp:
        aliases.append(f"url:{link_fp}")
    return list(dict.fromkeys(aliases))


def availability_id_for_job(job: dict[str, Any]) -> str:
    existing = clean_text(job.get("availabilityId"))
    if existing:
        return existing
    aliases = job_identity_aliases(job)
    stable = next((item for item in aliases if item.startswith("source:")), "")
    stable = stable or next((item for item in aliases if item.startswith("url:")), "")
    # Dedup and title/company keys are neither canonical availability identity
    # nor lifecycle aliases. Rows without an exact identity stay unmonitored.
    # Rows without an exact source ID or canonical URL remain unmonitored.
    return (
        f"availability_{hashlib.sha256(stable.encode('utf-8')).hexdigest()[:32]}" if stable else ""
    )


def _job_identity_key(job: dict[str, Any]) -> str:
    aliases = job_identity_aliases(job)
    return aliases[0] if aliases else ""


def _lifecycle_alias_index(rows: dict[str, dict[str, Any]]) -> dict[str, str]:
    index: dict[str, str] = {}
    conflicts: set[str] = set()
    for key, entry in rows.items():
        aliases = [
            clean_text(key),
            *_normalize_availability_aliases(entry.get("availabilityAliases")),
            *job_identity_aliases(entry),
        ]
        availability_id = clean_text(entry.get("availabilityId"))
        if availability_id:
            aliases.append(f"availability:{availability_id}")
        for alias in aliases:
            if not alias:
                continue
            previous = index.get(alias)
            if previous and previous != key:
                conflicts.add(alias)
                continue
            index[alias] = key
    for alias in conflicts:
        index.pop(alias, None)
    return index


def _index_lifecycle_entry_aliases(
    alias_index: dict[str, str], key: str, entry: dict[str, Any]
) -> None:
    """Incrementally index one appended lifecycle entry.

    Same semantics as ``_lifecycle_alias_index`` for a last-appended entry:
    conflict-drop is immediate because the entry is always appended last, so
    no later entry in a full rebuild could re-add a dropped alias.
    """
    aliases = [
        clean_text(key),
        *_normalize_availability_aliases(entry.get("availabilityAliases")),
        *job_identity_aliases(entry),
    ]
    availability_id = clean_text(entry.get("availabilityId"))
    if availability_id:
        aliases.append(f"availability:{availability_id}")
    for alias in aliases:
        if not alias:
            continue
        previous = alias_index.get(alias)
        if previous and previous != key:
            alias_index.pop(alias, None)
            continue
        alias_index[alias] = key


def _resolve_lifecycle_key(
    job: dict[str, Any], rows: dict[str, dict[str, Any]], alias_index: dict[str, str]
) -> str:
    availability_id = clean_text(job.get("availabilityId"))
    if availability_id:
        availability_alias = f"availability:{availability_id}"
        return alias_index.get(availability_alias) or availability_alias
    for alias in job_identity_aliases(job):
        matched = alias_index.get(alias)
        if matched:
            return matched
    key = _job_identity_key(job)
    return key if key in rows or key else ""
