from __future__ import annotations

from typing import Any

root: Any | None = None


def _require_root() -> Any:
    if root is None:
        raise RuntimeError("admin bridge root is not bound")
    return root


def normalize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    return _require_root()._get_registry_service().normalize_state(state)


def load_state() -> dict[str, list[dict[str, Any]]]:
    return _require_root()._get_registry_service().load_state()


def summarize_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, int]:
    return _require_root().RegistryService.summarize_state(state)


def persist_state(state: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    return _require_root()._get_registry_service().persist_state(state)


def persist_state_and_auto_sync(
    state: dict[str, list[dict[str, Any]]], *, reason: str
) -> dict[str, list[dict[str, Any]]]:
    root_mod = _require_root()
    return root_mod._registry_sync_flow.persist_state_and_auto_sync(
        state,
        reason=reason,
        persist_state=root_mod.persist_state,
        maybe_trigger_auto_sync_push=root_mod._maybe_trigger_auto_sync_push,
    )


def move_entries(
    pending: list[dict[str, Any]], selected_ids: list[str]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    return _require_root().RegistryService.move_entries(pending, selected_ids)


def build_manual_candidate(normalized_url: str) -> dict[str, Any] | None:
    root_mod = _require_root()
    if not normalized_url:
        return None
    studio = root_mod.infer_studio_name_from_host(normalized_url)
    inferred = root_mod.discovery.infer_web_candidate(
        normalized_url,
        studio,
        nl_priority=False,
    )
    if not isinstance(inferred, dict):
        fallback = {
            "name": f"{studio} (Manual Website)",
            "studio": studio,
            "company": studio,
            "adapter": "static",
            "pages": [normalized_url],
            "listing_url": normalized_url,
            "nlPriority": False,
            "enabledByDefault": False,
            "discoveryMethod": "manual",
            "discoveredAt": root_mod.now_iso(),
            "manualAddedAt": root_mod.now_iso(),
            "manualFallback": "generic_website",
        }
        return root_mod.ensure_source_id(fallback)
    row = root_mod.ensure_source_id(inferred)
    row["enabledByDefault"] = False
    row["discoveryMethod"] = "manual"
    row["discoveredAt"] = root_mod.now_iso()
    row["manualAddedAt"] = root_mod.now_iso()
    return row


def add_manual_source(raw_url: str) -> dict[str, Any]:
    root_mod = _require_root()
    normalized_url = root_mod.normalize_source_url(raw_url)
    if not normalized_url:
        return {"status": "invalid", "message": "Invalid URL. Use a full http(s) URL."}

    candidate = build_manual_candidate(normalized_url)
    if not candidate:
        return {
            "status": "invalid",
            "message": "URL is valid but provider is not supported for discovery checks.",
        }

    tombstones = root_mod.load_tombstones()
    if root_mod.is_tombstoned(candidate, tombstones):
        return {
            "status": "tombstoned",
            "sourceId": root_mod.source_identity(candidate),
            "source": root_mod.ensure_source_id(candidate),
            "message": "Source was deleted locally. Restore it before adding it again.",
        }

    state = root_mod.load_state()
    duplicate = root_mod.find_existing_source_by_url(state, normalized_url)
    if duplicate:
        return {
            "status": "duplicate",
            "sourceId": root_mod.source_identity(duplicate),
            "source": root_mod.ensure_source_id(duplicate),
            "message": "Source already exists.",
        }

    if str(candidate.get("adapter") or "").strip().lower() == "static":
        studio = str(candidate.get("studio") or "").strip()
        existing_match = root_mod.find_existing_static_source_by_studio_domain(
            state,
            studio=studio,
            normalized_url=normalized_url,
        )
        if existing_match is not None:
            bucket, idx, existing = existing_match
            updated = dict(existing)
            pages = (
                list(updated.get("pages") or []) if isinstance(updated.get("pages"), list) else []
            )
            normalized_pages = [root_mod.normalize_source_url(str(page or "")) for page in pages]
            normalized_pages = [page for page in normalized_pages if page]
            if normalized_url not in normalized_pages:
                normalized_pages.append(normalized_url)
            updated["pages"] = normalized_pages
            if not str(updated.get("listing_url") or "").strip():
                updated["listing_url"] = normalized_pages[0] if normalized_pages else normalized_url
            updated = root_mod.ensure_source_id(updated)
            state[bucket][idx] = updated
            state = root_mod.persist_state_and_auto_sync(
                state,
                reason=root_mod.REGISTRY_REASON_MANUAL_SOURCE_VARIANT,
            )
            return {
                "status": "duplicate",
                "sourceId": root_mod.source_identity(updated),
                "source": root_mod.ensure_source_id(updated),
                "summary": root_mod.summarize_state(state),
                "message": "Source already exists for this studio/domain. Added URL as page variant.",
            }

    state["pending"] = root_mod.unique_sources([candidate, *state["pending"]])
    state = root_mod.persist_state_and_auto_sync(
        state,
        reason=root_mod.REGISTRY_REASON_MANUAL_SOURCE,
    )
    added = next(
        (
            row
            for row in state["pending"]
            if root_mod.source_identity(row) == root_mod.source_identity(candidate)
        ),
        candidate,
    )
    return {
        "status": "added",
        "sourceId": root_mod.source_identity(added),
        "source": root_mod.ensure_source_id(added),
        "summary": root_mod.summarize_state(state),
        "message": "Manual source added with generic website scraping fallback."
        if str(added.get("adapter") or "").lower() == "static"
        else "Manual source added.",
    }


def fetch_html_with_fallback_bound(url: str, timeout_s: int) -> tuple[str, str, bool, bool]:
    root_mod = _require_root()
    return root_mod._source_check_fetch.fetch_html_with_fallback(
        url,
        timeout_s,
        fetch_text=lambda u, t: root_mod.discovery.fetch_text_with_retry(u, t, adapter="static"),
        looks_like_challenge=root_mod._source_check_http.looks_like_browser_challenge_page,
        has_extractable_job_data=lambda html, page_url: (
            root_mod._source_check_fetch.html_has_extractable_job_data(
                html,
                page_url,
                html_extractor=root_mod._html_extractor,
            )
        ),
        try_playwright=root_mod._source_check_http.try_fetch_with_playwright,
        is_http_forbidden=root_mod._source_check_http.is_http_forbidden_error,
    )


def fetch_static_page_with_alternates_bound(
    page_url: str, timeout_s: int
) -> tuple[str, str, bool, bool, str]:
    root_mod = _require_root()
    return root_mod._source_check_fetch.fetch_static_page_with_alternates(
        page_url,
        timeout_s,
        fetch_html_with_fallback_fn=fetch_html_with_fallback_bound,
        suggest_alternate_urls=root_mod._source_check_http.suggest_alternate_career_urls,
        discover_redirect_career_candidates=(
            root_mod._source_check_http.discover_redirect_career_candidates
        ),
        is_not_found_error_text=root_mod._source_check_http.is_not_found_error_text,
    )


def check_static_source(
    row: dict[str, Any], timeout_s: int = 12
) -> tuple[bool, int, str, bool, dict[str, Any]]:
    root_mod = _require_root()
    return root_mod._source_checker.check_static_source(
        row,
        timeout_s,
        fetch_page_with_alternates=fetch_static_page_with_alternates_bound,
        fetch_page=fetch_html_with_fallback_bound,
        fetch_text=lambda url, timeout: root_mod.discovery.fetch_text_with_retry(
            url,
            timeout,
            adapter="static",
        ),
        html_extractor=root_mod._html_extractor,
        parse_jobpostings_from_html=root_mod.parse_jobpostings_from_html,
        normalize_job_url=root_mod.normalize_job_url,
        source_identity=root_mod.source_identity,
        suggest_alternate_career_urls=root_mod._source_check_http.suggest_alternate_career_urls,
    )


def normalize_manual_static_studio_fields(row: dict[str, Any]) -> dict[str, Any]:
    root_mod = _require_root()
    return root_mod._source_check_api.normalize_manual_static_studio_fields(
        row,
        normalize_source_url=root_mod.normalize_source_url,
        infer_studio_name_from_host=root_mod.infer_studio_name_from_host,
    )


def trigger_source_check(source_id: str, timeout_s: int = 12) -> dict[str, Any]:
    root_mod = _require_root()
    return root_mod._source_check_api.trigger_source_check(
        source_id,
        timeout_s=timeout_s,
        load_state=root_mod.load_state,
        source_identity=root_mod.source_identity,
        normalize_manual_static_studio_fields_fn=root_mod.normalize_manual_static_studio_fields,
        check_static_source_fn=root_mod.check_static_source,
        now_iso=root_mod.now_iso,
        compute_candidate_score=root_mod.discovery.compute_candidate_score,
        normalize_candidate=root_mod.discovery.normalize_candidate,
        probe_candidate=root_mod.discovery.probe_candidate,
        persist_state_and_auto_sync=root_mod.persist_state_and_auto_sync,
        normalize_source_url=root_mod.normalize_source_url,
        build_check_failure_details=root_mod._source_check_http.build_check_failure_details,
    )
