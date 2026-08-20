"""Registry conflict adjudication — row/url core helpers.

AI boundary owns: row accessors, url/adapter tokens, and universal coercion helpers for the conflict adjudication task.
AI boundary implement in: this registry_conflict_adjudication_core.py leaf.
AI boundary search before contracts: conflict adjudication routes, progress payloads, and adjudication tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry adjudication tests."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from src.source_registry import source_identity
from src.source_registry_identity import provider_fields_from_row_identity

ADJUDICATION_REASON = "registry_conflict_adjudication_auto_demote"

ADJUDICATION_PATH_NAME = "registry-conflict-adjudication.json"


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _artifact_path(api: Any) -> Any:
    return getattr(api, "REGISTRY_CONFLICT_ADJUDICATION_PATH", None) or (
        api.JOBS_FETCH_REPORT_PATH.with_name(ADJUDICATION_PATH_NAME)
    )


def load_registry_conflict_adjudication(api: Any) -> dict[str, Any]:
    return _as_dict(api.load_json_object(_artifact_path(api), {}))


def _row_id(row: dict[str, Any]) -> str:
    return _clean(row.get("id") or row.get("sourceId") or source_identity(row))


def _row_state(row: dict[str, Any]) -> str:
    return _clean(row.get("registryState") or row.get("candidateState")).lower()


def _row_adapter(row: dict[str, Any]) -> str:
    adapter = _clean(row.get("adapter") or row.get("sourceType")).lower()
    if adapter:
        return adapter
    row_id = _row_id(row).lower()
    return row_id.split(":", 1)[0] if ":" in row_id else ""


def _urls_from_row(row: dict[str, Any]) -> list[str]:
    values = [
        row.get(key)
        for key in (
            "api_url",
            "feed_url",
            "board_url",
            "listing_url",
            "careersUrl",
            "url",
            "sourceUrl",
            "id",
            "sourceId",
        )
    ]
    urls: list[str] = []
    for value in values:
        for match in re.findall(r"https?://[^\s|]+", _clean(value)):
            url = match.rstrip("),.;'\"")
            if url and url not in urls:
                urls.append(url)
    return urls


def _adapter_token(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _clean(row.get(key))
        if value:
            return value
    identity_fields = provider_fields_from_row_identity(row)
    for key in keys:
        value = _clean(identity_fields.get(key))
        if value:
            return value
    for url in _urls_from_row(row):
        host = urlparse(url).netloc.lower()
        if host:
            return host.split(".", 1)[0]
    return ""


def _endpoint_url(row: dict[str, Any]) -> str:
    for url in _urls_from_row(row):
        return url
    adapter = _row_adapter(row)
    if adapter == "greenhouse":
        slug = _adapter_token(row, "slug", "account", "company_id")
        return (
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true" if slug else ""
        )
    if adapter == "lever":
        account = _adapter_token(row, "account", "slug", "company_id")
        return f"https://api.lever.co/v0/postings/{account}?mode=json" if account else ""
    if adapter == "workable":
        account = _adapter_token(row, "account", "slug", "company_id")
        return (
            f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true"
            if account
            else ""
        )
    if adapter == "smartrecruiters":
        company_id = _adapter_token(row, "company_id", "account", "slug")
        return (
            f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings"
            if company_id
            else ""
        )
    if adapter == "jazzhr":
        board_url = _adapter_token(row, "board_url")
        return board_url if board_url else ""
    return ""
