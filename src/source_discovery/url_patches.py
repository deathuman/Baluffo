from __future__ import annotations

"""Shared URL patch helpers for discovery and manual refresh tooling."""

import re
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from src.bridge.source_check_http import (
    discover_redirect_career_candidates,
    suggest_alternate_career_urls,
)
from src.source_registry import (
    URL_PATCH_MANIFEST_PATH,
    load_json_object,
    normalize_source_url,
    save_json_atomic,
)

GREENHOUSE_KNOWN_FIXES: dict[str, str] = {
    "niantic": "https://careers.nianticlabs.com/",
    "larian": "https://larian.com/careers",
    "supercell": "https://supercell.com/en/careors/",
    "ioi": "https://apply.ioi.dk/jobs",
    "io-interactive": "https://apply.ioi.dk/jobs",
    "guerrilla": "https://job-boards.greenhouse.io/guerrilla-games",
    "guerrilla-games": "https://job-boards.greenhouse.io/guerrilla-games",
    "remedy": "https://www.remedygames.com/careers/",
    "remedy-entertainment": "https://www.remedygames.com/careers/",
    "bandainamco": "https://job-boards.greenhouse.io/bandainamco",
    "playstation": "https://careers.playstation.com/",
    "playstation global": "https://careers.playstation.com/",
}

PATCHABLE_ERROR_TOKENS: tuple[str, ...] = (
    "redirect response",
    "redirect location",
    "http error 404",
    "404 not found",
    "http error 410",
    "410 gone",
)


def _default_manifest(patches: dict[str, str] | None = None) -> dict[str, Any]:
    return {
        "_version": "1.1",
        "_updated": datetime.now().strftime("%Y-%m-%d"),
        "_description": "URL patches for studios with redirect issues. Original URL -> Corrected URL.",
        "_stats": {
            "total_patches": len(patches or {}),
            "added": 0,
            "updated": 0,
            "reprobed": 0,
        },
        "patches": dict(patches or {}),
    }


def load_url_patch_manifest(path: Path | None = None) -> dict[str, Any]:
    manifest = load_json_object(path or URL_PATCH_MANIFEST_PATH, _default_manifest())
    patches_value = manifest.get("patches")
    patches = patches_value if isinstance(patches_value, dict) else {}
    normalized_patches: dict[str, str] = {}
    for raw_source, raw_target in patches.items():
        source = normalize_source_url(str(raw_source or ""))
        target = normalize_source_url(str(raw_target or ""))
        if source and target:
            normalized_patches[source] = target
    manifest.setdefault("_stats", {})
    manifest["patches"] = normalized_patches
    manifest["_stats"]["total_patches"] = len(normalized_patches)
    return manifest


def load_url_patches(path: Path | None = None) -> dict[str, str]:
    return dict(load_url_patch_manifest(path).get("patches") or {})


def save_url_patch_manifest(
    patches: dict[str, str],
    *,
    path: Path | None = None,
    added: int = 0,
    updated: int = 0,
    reprobed: int = 0,
) -> dict[str, Any]:
    manifest = _default_manifest(patches)
    manifest["_stats"].update(
        {
            "total_patches": len(patches),
            "added": int(max(0, added)),
            "updated": int(max(0, updated)),
            "reprobed": int(max(0, reprobed)),
        }
    )
    save_json_atomic(path or URL_PATCH_MANIFEST_PATH, manifest)
    return manifest


def merge_url_patches(
    existing_patches: dict[str, str], new_patches: dict[str, str]
) -> tuple[dict[str, str], int, int]:
    merged: dict[str, str] = dict(existing_patches or {})
    added = 0
    updated = 0
    for raw_source, raw_target in dict(new_patches or {}).items():
        source = normalize_source_url(str(raw_source or ""))
        target = normalize_source_url(str(raw_target or ""))
        if not source or not target:
            continue
        previous = merged.get(source)
        if previous is None:
            merged[source] = target
            added += 1
        elif previous != target:
            merged[source] = target
            updated += 1
    return merged, added, updated


def apply_url_patches_to_candidate(
    candidate: dict[str, Any], patches: dict[str, str]
) -> tuple[dict[str, Any], bool]:
    normalized_patches = dict(patches or {})
    if not normalized_patches:
        return dict(candidate), False

    updated = dict(candidate)
    changed = False
    for key in (
        "api_url",
        "feed_url",
        "board_url",
        "listing_url",
        "careersUrl",
        "sourceDirectoryEntryUrl",
    ):
        raw_value = str(updated.get(key) or "").strip()
        normalized = normalize_source_url(raw_value)
        patched = normalized_patches.get(normalized)
        if raw_value and patched and patched != normalized:
            updated[key] = patched
            changed = True

    pages = updated.get("pages")
    if isinstance(pages, list):
        new_pages: list[str] = []
        for raw_page in pages:
            text = str(raw_page or "").strip()
            normalized = normalize_source_url(text)
            patched = normalized_patches.get(normalized)
            if text and patched and patched != normalized:
                new_pages.append(patched)
                changed = True
            else:
                new_pages.append(text)
        updated["pages"] = new_pages

    if changed:
        updated["urlPatchApplied"] = True
    return updated, changed


def should_attempt_patch_recovery(error_text: str) -> bool:
    lower = str(error_text or "").lower()
    return any(token in lower for token in PATCHABLE_ERROR_TOKENS)


def extract_url_from_error(error_text: str) -> str:
    match = re.search(r"(https?://[^\s'\"]+)", str(error_text or ""))
    return normalize_source_url(match.group(1)) if match else ""


def extract_redirect_location(error_text: str) -> str:
    match = re.search(r"Redirect location:\s*'([^']+)'", str(error_text or ""), flags=re.I)
    return normalize_source_url(match.group(1)) if match else ""


def resolve_greenhouse_known(studio_name: str) -> str:
    name_lower = str(studio_name or "").lower()
    for key, url in GREENHOUSE_KNOWN_FIXES.items():
        if key in name_lower:
            return normalize_source_url(url)
    return ""


def resolve_patch_target(
    *,
    candidate: dict[str, Any],
    error_text: str,
    timeout_s: int,
) -> str:
    direct_redirect = extract_redirect_location(error_text)
    if direct_redirect:
        return direct_redirect

    original_url = normalize_source_url(
        extract_url_from_error(error_text) or str(candidate.get("careersUrl") or "")
    )
    if not original_url:
        original_url = normalize_source_url(
            str(
                candidate.get("listing_url")
                or candidate.get("board_url")
                or candidate.get("api_url")
                or ""
            )
        )
    if not original_url:
        return ""

    if "greenhouse" in original_url:
        known = resolve_greenhouse_known(
            str(candidate.get("studio") or candidate.get("name") or "")
        )
        if known:
            return known

    for suggested in suggest_alternate_career_urls(original_url):
        normalized = normalize_source_url(suggested)
        if normalized:
            return normalized

    for suggested in discover_redirect_career_candidates(original_url, timeout_s):
        normalized = normalize_source_url(suggested)
        if normalized:
            return normalized

    return ""


async def resolve_url(url: str, timeout: float = 10.0) -> tuple[str, int, list[str]]:
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            max_redirects=10,
        ) as client:
            response = await client.get(url)
            redirect_chain = [str(r.url) for r in response.history]
            redirect_chain.append(str(response.url))
            return str(response.url), response.status_code, redirect_chain
    except (httpx.HTTPError, ValueError):
        return "", 0, []


def extract_redirect_failures(report: dict[str, Any]) -> list[dict[str, str]]:
    failures = report.get("failures", [])
    redirects: list[dict[str, str]] = []
    for failure in failures if isinstance(failures, list) else []:
        error = str((failure or {}).get("error") or "")
        if not should_attempt_patch_recovery(error):
            continue
        url = extract_url_from_error(error)
        if not url:
            continue
        redirects.append(
            {
                "name": str((failure or {}).get("name") or "unknown"),
                "url": url,
                "adapter": str((failure or {}).get("adapter") or "unknown"),
                "original_error": error,
            }
        )
    return redirects


def summarize_url_patch_runtime(
    *,
    loaded: int,
    added: int,
    updated: int,
    reprobed: int,
) -> dict[str, int]:
    return {
        "loaded": int(max(0, loaded)),
        "added": int(max(0, added)),
        "updated": int(max(0, updated)),
        "reprobed": int(max(0, reprobed)),
    }
