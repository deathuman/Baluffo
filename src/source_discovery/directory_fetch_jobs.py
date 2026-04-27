from __future__ import annotations

"""Shared fetch-job builders for source-discovery directory adapters."""

from typing import Any


def build_directory_fetch_job(
    *,
    url: str,
    payload: dict[str, Any],
    adapter: str,
    failure_stage: str,
) -> dict[str, Any]:
    normalized_url = str(url or "").strip()
    return {
        "url": normalized_url,
        "payload": payload,
        "name": normalized_url,
        "adapter": adapter,
        "failureStage": failure_stage,
    }


def build_directory_fetch_jobs(
    entries: list[dict[str, Any]],
    *,
    url_field: str,
    adapter: str,
    failure_stage: str,
    required_fields: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for entry in entries:
        url = str(entry.get(url_field) or "").strip()
        if not url:
            continue
        if any(not str(entry.get(field) or "").strip() for field in required_fields):
            continue
        jobs.append(
            build_directory_fetch_job(
                url=url,
                payload=entry,
                adapter=adapter,
                failure_stage=failure_stage,
            )
        )
    return jobs
