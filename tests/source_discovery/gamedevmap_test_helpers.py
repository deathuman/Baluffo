from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.source_discovery import gamedevmap_active_dry_run as dry_run

from ._helpers import _fixture_text

CSV_URL = "https://www.gamedevmap.com/cmsdata/gamedevmapdata.csv"
INDEX_URL = "https://www.gamedevmap.com/index.php"


def gamedevmap_config(
    *,
    allowed_categories: list[str] | None = None,
    **overrides: Any,
) -> dict[str, object]:
    gamedevmap = {
        "enabled": True,
        "csvUrl": CSV_URL,
        "indexUrl": INDEX_URL,
        "allowedCategories": allowed_categories
        or ["Developer", "Developer and Publisher", "Publisher", "Mobile"],
        "blockedCategories": ["Organization"],
        "fetchConcurrency": 4,
        "perHostConcurrency": 2,
    }
    gamedevmap.update(overrides)
    return {"gamedevmap": gamedevmap}


def gamedevmap_payloads() -> dict[str, str]:
    return {
        CSV_URL: _fixture_text("gamedevmap_data.csv"),
        "https://homepage-provider.example.com": _fixture_text("gamedevmap_homepage_provider.html"),
        "https://homepage-static.example.com": _fixture_text("gamedevmap_homepage_static.html"),
        "https://duplicate.example.com": _fixture_text("gamedevmap_homepage_no_jobs.html"),
        "https://boards-api.greenhouse.io/v1/boards/providerfeedstudio/jobs?content=true": json.dumps(
            {
                "jobs": [
                    {
                        "id": 1,
                        "title": "Gameplay Engineer",
                        "absolute_url": "https://job-boards.greenhouse.io/providerfeedstudio/jobs/1",
                    },
                    {
                        "id": 2,
                        "title": "Technical Artist",
                        "absolute_url": "https://job-boards.greenhouse.io/providerfeedstudio/jobs/2",
                    },
                ]
            }
        ),
        "https://boards-api.greenhouse.io/v1/boards/homepageproviderstudio/jobs?content=true": json.dumps(
            {"jobs": []}
        ),
    }


def gamedevmap_fetcher(payloads: dict[str, str], calls: list[str] | None = None):
    def fake_fetch(url: str, _: int) -> str:
        if calls is not None:
            calls.append(url)
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    return fake_fetch


def gamedevmap_csv_row(studio: str, url: str) -> str:
    return (
        "Organization,URL,City,State/Province,Country/Region,Map Def,Category,Comments,"
        "Updated By,Bluesky,AI Response\n"
        f"{studio},{url},Rome,Lazio,Italy,Rome,Developer,Verified gaming studio.,,,"
        "Correct (Gaming)\n"
    )


def validated_static_candidate(
    name: str = "Validated Static",
    *,
    index: int | None = None,
    jobs_found: int = 2,
    recovered: bool = False,
) -> dict[str, object]:
    if index is None:
        url = "https://validated.example.com/jobs"
        source_entry = "https://www.gamedevmap.com/example"
        display_name = name
    else:
        url = f"https://validated-{index}.example{index}.com/jobs"
        source_entry = f"https://www.gamedevmap.com/?query={index}"
        display_name = f"{name} {index}"
    candidate = {
        "id": f"static:listing_url:{url}",
        "name": f"{display_name} (GameDevMap)",
        "studio": display_name,
        "company": display_name,
        "adapter": "static",
        "listing_url": url,
        "pages": [url],
        "careersUrl": url,
        "sourceDirectory": "gamedevmap",
        "sourceDirectoryEntryUrl": source_entry,
        "discoveryMethod": "gamedevmap",
        "discoveryStage": "generic_static",
        "evidenceScore": 44,
        "evidenceTypes": [
            "gamedevmap_directory",
            "gamedevmap_homepage_fetch",
            "structured_job_links",
        ],
        "probeStatus": "ok",
        "candidateState": "validated",
        "jobsFound": jobs_found,
        "sampleCount": jobs_found,
        "lastProbedAt": "2026-04-26T12:00:00+00:00",
        "weakSignal": False,
    }
    if recovered:
        candidate["gamedevmapRecovery"] = True
    return candidate


def validated_provider_candidate() -> dict[str, object]:
    return {
        "id": "greenhouse:slug:validatedstudio",
        "name": "Validated Studio (Greenhouse)",
        "studio": "Validated Studio",
        "company": "Validated Studio",
        "adapter": "greenhouse",
        "slug": "validatedstudio",
        "api_url": "https://boards-api.greenhouse.io/v1/boards/validatedstudio/jobs?content=true",
        "careersUrl": "https://boards.greenhouse.io/validatedstudio",
        "sourceDirectory": "gamedevmap",
        "discoveryMethod": "gamedevmap",
        "discoveryStage": "web_provider",
        "evidenceScore": 44,
        "evidenceTypes": ["gamedevmap_directory", "web_provider_url"],
        "probeStatus": "ok",
        "candidateState": "validated",
        "jobsFound": 1,
        "sampleCount": 1,
        "lastProbedAt": "2026-04-26T12:00:00+00:00",
    }


def write_gamedevmap_audit_artifact(
    path: Path,
    *,
    config: dict[str, object],
    active_candidates: list[dict[str, object]] | None = None,
    browser_candidates: list[dict[str, object]] | None = None,
    updated_at: str = "",
    include_default_rejections: bool = True,
) -> None:
    cfg_value = config.get("gamedevmap")
    cfg = dict(cfg_value) if isinstance(cfg_value, dict) else {}
    candidates = (
        active_candidates
        if active_candidates is not None
        else [validated_provider_candidate(), validated_static_candidate()]
    )
    browsers = browser_candidates or []
    timestamp = updated_at or dry_run.now_iso()
    payload = {
        "schemaVersion": dry_run.DRY_RUN_SCHEMA_VERSION,
        "updatedAt": timestamp,
        "finishedAt": timestamp,
        "runtime": {
            "configSignature": dry_run._gamedevmap_cache_signature(cfg),
            "artifactSizeBytes": 1234,
        },
        "progress": {"complete": True},
        "summary": {
            "activeCandidates": len(candidates),
            "activeAdapterCounts": {
                "static": sum(1 for row in candidates if row.get("adapter") == "static"),
                "greenhouse": sum(1 for row in candidates if row.get("adapter") == "greenhouse"),
            },
            "recoveredActiveCandidates": sum(
                1 for row in candidates if bool(row.get("gamedevmapRecovery"))
            ),
            "browserRecoveryCandidates": len(browsers),
            "artifactSizeBytes": 1234,
            "rejectedReasonDetailCounts": {"js_shell": len(browsers)},
        },
        "timings": {"totalsMs": {"totalMs": 123}},
        "activeCandidates": candidates,
        "zeroJobCandidates": [validated_static_candidate("Zero Jobs", jobs_found=0)],
        "browserRecoveryCandidates": browsers,
        "rejectedForActivation": [],
        "failures": [{"name": "Fetch Failed", "adapter": "gamedevmap", "stage": "x"}],
    }
    if include_default_rejections:
        payload["rejectedForActivation"] = [
            {"reason": "probe_failed", "candidate": validated_provider_candidate()}
        ]
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
