from __future__ import annotations

import hashlib
import time
from typing import Any


def dedup_row(**overrides):
    payload = {
        "id": "job-1",
        "dedupKey": "key-1",
        "title": "Senior Engineer",
        "company": "Studio One",
        "jobLink": "https://example.com/jobs/1",
        "locationSummary": "Amsterdam, NL",
        "sourceBundleCount": 1,
        "sourceBundle": [
            {
                "source": "greenhouse:slug:studio-one",
                "sourceJobId": "gh-1",
                "jobLink": "https://example.com/jobs/1",
                "adapter": "greenhouse",
            }
        ],
        "locations": [{"city": "Amsterdam", "country": "NL"}],
    }
    payload.update(overrides)
    return payload


def lifecycle_row(**overrides):
    payload = {
        "id": "job-identity",
        "dedupKey": "identity-key",
        "title": "Senior Engineer",
        "company": "Studio One",
        "jobLink": "https://example.com/jobs/1",
        "locationSummary": "Amsterdam, NL",
        "sourceBundleCount": 2,
        "sourceBundle": [],
        "locations": [{"city": "Amsterdam", "country": "NL"}],
    }
    payload.update(overrides)
    return payload


def source_row(plugin_name: str) -> dict[str, Any]:
    return {
        "id": plugin_name,
        "name": f"{plugin_name.title()} Careers",
        "studio": f"{plugin_name.title()} Studio",
        "company": f"{plugin_name.title()} Studio",
    }


def task_row() -> dict[str, object]:
    return {
        "status": "running",
        "startedAt": "2026-04-18T10:00:00Z",
        "finishedAt": "",
        "heartbeatAt": "2026-04-18T10:00:00Z",
        "durationMs": 0,
        "error": "",
        "_startedMonotonic": time.perf_counter(),
        "_slowWarned": False,
        "progress": {},
    }


def fingerprint_row(index: int, *, extra_chunks: int = 8) -> dict[str, str]:
    return {
        "id": f"static:listing_url:https://studio-{index:05d}.example/jobs",
        "adapter": "static",
        "name": f"Studio {index:05d}",
        "listing_url": f"https://studio-{index:05d}.example/jobs",
        "notes": "".join(
            hashlib.sha256(f"{index}:{chunk}".encode()).hexdigest() for chunk in range(extra_chunks)
        ),
    }


def canonical_feed_row(availability_id: str = "availability_1") -> dict:
    return {
        "id": "job-1",
        "title": "Engine Programmer",
        "company": "Studio",
        "city": "Rome",
        "country": "Italy",
        "workType": "Hybrid",
        "contractType": "Full-time",
        "jobLink": "https://example.com/jobs/1",
        "sector": "Games",
        "profession": "engine-programmer",
        "source": "fixture",
        "sourceJobId": "1",
        "availabilityId": availability_id,
        "availabilityStatus": "available",
        "sourceBundle": [],
    }


def excluded_report(name: str, reason: str) -> dict[str, object]:
    return {
        "name": name,
        "status": "excluded",
        "adapter": "custom",
        "fetchStrategy": "auto",
        "studio": "",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": reason,
        "exclusionReason": reason,
        "durationMs": 0,
    }


def sheet_csv() -> str:
    return """x,x,x,x
x,Studio,Hiring Location,Roles open,Link
x,Provider Studio,Remote,yes,https://boards.greenhouse.io/providerstudio
x,Static Studio,Remote,speculative,https://static.example.com/careers
"""
