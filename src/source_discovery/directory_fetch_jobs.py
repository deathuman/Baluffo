from __future__ import annotations

"""Shared fetch-job builders for source-discovery directory adapters."""

from typing import Any


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
            {
                "url": url,
                "payload": entry,
                "name": url,
                "adapter": adapter,
                "failureStage": failure_stage,
            }
        )
    return jobs
