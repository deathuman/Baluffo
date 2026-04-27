from __future__ import annotations

"""Shared candidate collection helpers for source-discovery audit artifacts."""

from typing import Any

from src.source_registry import unique_sources


def candidate_rows(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, dict)]


def unique_candidate_rows(value: Any) -> list[dict[str, Any]]:
    return unique_sources(candidate_rows(value))


def provider_static_rows_from_payload(
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return (
        unique_candidate_rows(payload.get("providerCandidates")),
        unique_candidate_rows(payload.get("staticCandidates")),
    )


def split_provider_static_rows(
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    provider_rows: list[dict[str, Any]] = []
    static_rows: list[dict[str, Any]] = []
    for row in candidate_rows(rows):
        adapter = str(row.get("adapter") or "").strip().lower()
        if adapter == "static":
            static_rows.append(row)
        elif adapter:
            provider_rows.append(row)
    return provider_rows, static_rows


def append_provider_static_rows(
    artifact: dict[str, Any],
    *,
    provider_rows: list[dict[str, Any]] | None = None,
    static_rows: list[dict[str, Any]] | None = None,
) -> None:
    artifact["providerCandidates"] = unique_sources(
        [
            *candidate_rows(artifact.get("providerCandidates")),
            *candidate_rows(provider_rows or []),
        ]
    )
    artifact["staticCandidates"] = unique_sources(
        [
            *candidate_rows(artifact.get("staticCandidates")),
            *candidate_rows(static_rows or []),
        ]
    )
