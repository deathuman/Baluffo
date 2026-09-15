"""Origin-aware guest-view junk-class guard (hold-tail systemic option 3).

Fusebox's 41 ``linkedin.com/jobs/{slug}?trk=…`` verification_overdue rows and
Konami's "Community" nav-anchor row share a provenance class: LinkedIn
guest-view URLs harvested by static sources whose registry origin is NOT
LinkedIn. They can never re-verify for a logged-out fetcher (LinkedIn answers
bots with HTTP 999) and strand as floor rows forever. These tests pin the
predicate's origin-awareness (sanctioned LinkedIn-origin sources keep their
/jobs/view/ rows), its fail-open default, and both the inbound funnel drops
and the lifecycle drain that lets a failing source's stranded rows go.
"""

from __future__ import annotations

import pytest

from src.jobs.common import config as common_config
from src.jobs.common.origin_junk import (
    count_active_junk_class_rows,
    is_junk_class_row,
    is_junk_provenance_row,
    is_linkedin_guest_junk_url,
    is_nav_anchor_junk_row,
    source_identity_host,
    source_origin_is_linkedin,
)

# Real shapes from the 2026-09-13 Fusebox adjudication / lifecycle store.
FUSEBOX_GUEST_ROW = (
    "https://www.linkedin.com/jobs/production-specialist-jobs?trk=organization_guest_linkster_link"
)
SKYBOUND_GUEST_ROW = (
    "https://www.linkedin.com/jobs/view/director-of-franchise-development-at-empower-brands-"
    "4377268827?pageNum=0&position=2"
)
SANCTIONED_VIEW_ROW = (
    "https://bg.linkedin.com/jobs/view/front-of-house-administrator-at-sega-europe-4455769546"
)
KONAMI_COMMUNITY_ROW = "https://www.konami.com/games/us/en/pages/sns_account"
# Registry-form source ids (extraction-ctx spelling) for the monitor tests.
FUSEBOX_SOURCE_ID = "static:listing_url:https://fuseboxgames.com/careers/"
LINKEDIN_SOURCE_ID = "static:listing_url:https://www.linkedin.com/jobs/search/?geoId=103112868"


def _source(listing_url: str | None) -> dict[str, object]:
    if listing_url is None:
        return {"id": "plugin:custom", "name": "Custom Board (Plugin)"}
    return {"id": f"static_source::static:listing_url:{listing_url}", "name": "Studio (Sheet)"}


# --- URL-shape predicate ---------------------------------------------------


def test_fusebox_guest_search_url_is_junk() -> None:
    assert is_linkedin_guest_junk_url(FUSEBOX_GUEST_ROW)


def test_skybound_decorated_view_url_is_junk() -> None:
    assert is_linkedin_guest_junk_url(SKYBOUND_GUEST_ROW)


def test_linkedin_search_page_is_junk() -> None:
    assert is_linkedin_guest_junk_url(
        "https://www.linkedin.com/jobs/search?currentJobId=1&f_C=1245936"
    )


def test_bare_numeric_view_url_is_not_junk() -> None:
    assert not is_linkedin_guest_junk_url("https://www.linkedin.com/jobs/view/2483648202")


def test_non_linkedin_url_is_not_junk() -> None:
    assert not is_linkedin_guest_junk_url("https://www.konami.com/games/us/en/jobs/?trk=x")


def test_unparseable_url_fails_open() -> None:
    assert not is_linkedin_guest_junk_url("http://[cdn_template_directory]/images/x.jpg")


# --- nav-anchor (Konami) shape ---------------------------------------------


def test_konami_community_row_is_nav_junk() -> None:
    assert is_nav_anchor_junk_row(
        "Community",
        KONAMI_COMMUNITY_ROW,
        source_host="www.konami.com",
    )


def test_nav_junk_requires_exact_title() -> None:
    assert not is_nav_anchor_junk_row(
        "Community Manager",
        KONAMI_COMMUNITY_ROW,
        source_host="www.konami.com",
    )


def test_nav_junk_requires_same_host() -> None:
    assert not is_nav_anchor_junk_row(
        "Community",
        "https://games.example.com/community",
        source_host="www.konami.com",
    )


def test_nav_junk_never_hits_job_surface_urls() -> None:
    assert not is_nav_anchor_junk_row(
        "Jobs",
        "https://www.konami.com/games/us/en/jobs/",
        source_host="www.konami.com",
    )
    assert not is_nav_anchor_junk_row(
        "Contact",
        "https://www.example.com/apply?job_id=123",
        source_host="www.example.com",
    )


# --- origin-awareness -------------------------------------------------------


def test_linkedin_origin_source_is_sanctioned() -> None:
    source = _source("https://www.linkedin.com/jobs/search/?f_C=1245936")
    assert source_origin_is_linkedin(source)
    assert source_identity_host(source) == "www.linkedin.com"


def test_sanctioned_linkedin_origin_never_classifies_its_rows() -> None:
    source = _source("https://www.linkedin.com/jobs/search/?f_C=1245936")
    row = {"jobLink": SANCTIONED_VIEW_ROW, "title": "Front of House Administrator"}
    assert not is_junk_provenance_row(row, source=source)


def test_non_linkedin_static_origin_classifies_guest_rows() -> None:
    source = _source("https://fuseboxgames.com/careers/")
    assert is_junk_provenance_row(
        {"jobLink": FUSEBOX_GUEST_ROW, "title": "Production Specialist"}, source=source
    )


def test_registry_form_id_resolves_identity() -> None:
    """The 2026-09-14 Fusebox revival regression: extraction lanes carry the
    registry spelling (``static:listing_url:…``, no ``static_source::``
    prefix). The prefix-sensitive resolver returned an empty host and the
    guards fail-opened to no-ops in every funnel while all tests used the
    state spelling."""
    source = {"id": "static:listing_url:https://fuseboxgames.com/careers/"}
    assert source_identity_host(source) == "fuseboxgames.com"
    assert not source_origin_is_linkedin(source)
    assert is_junk_provenance_row(
        {"jobLink": FUSEBOX_GUEST_ROW, "title": "Production Specialist"}, source=source
    )


def test_registry_form_linkedin_origin_still_sanctioned() -> None:
    source = {"id": "static:listing_url:https://www.linkedin.com/jobs/search/?f_C=1245936"}
    assert source_origin_is_linkedin(source)
    assert not is_junk_provenance_row(
        {"jobLink": SANCTIONED_VIEW_ROW, "title": "Front of House Administrator"}, source=source
    )


def test_company_page_path_is_junk() -> None:
    """The rendered widget's "LinkedIn" link (Fusebox 09-14 render): a
    company/talent page, queryless or tracked, is never a per-job surface."""
    assert is_linkedin_guest_junk_url("https://www.linkedin.com/company/fusebox-games/jobs")
    assert is_linkedin_guest_junk_url(
        "https://uk.linkedin.com/company/fusebox-games?trk=organization_guest_main-feed-card_feed-actor-image"
    )


def test_sanctioned_origin_company_row_not_junk() -> None:
    source = _source("https://www.linkedin.com/jobs/search/?f_C=1245936")
    assert not is_junk_provenance_row(
        {"jobLink": "https://www.linkedin.com/company/ubisoft-montreal", "title": "Jobs"},
        source=source,
    )


def test_non_linkedin_static_origin_classifies_nav_rows() -> None:
    source = _source("https://www.konami.com/games/us/en/jobs/")
    assert is_junk_provenance_row(
        {"jobLink": KONAMI_COMMUNITY_ROW, "title": "Community"}, source=source
    )


def test_source_without_static_identity_fails_open() -> None:
    source = {"id": "google_sheets", "name": "Sheet"}
    assert not is_junk_provenance_row(
        {"jobLink": FUSEBOX_GUEST_ROW, "title": "Whatever"}, source=source
    )
    assert not is_junk_provenance_row(
        {"jobLink": FUSEBOX_GUEST_ROW, "title": "Whatever"}, source=None
    )


def test_row_without_link_fails_open() -> None:
    assert not is_junk_provenance_row({"title": "Engineer"}, source=_source("https://x.com/jobs"))
    assert not is_junk_provenance_row("not-a-dict", source=_source("https://x.com/jobs"))  # type: ignore[arg-type]


# --- kill switch -------------------------------------------------------------


def test_guard_disabled_by_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(common_config, "GUEST_JUNK_GUARD_ENABLED", False)
    source = _source("https://fuseboxgames.com/careers/")
    assert not is_junk_provenance_row(
        {"jobLink": FUSEBOX_GUEST_ROW, "title": "Production Specialist"}, source=source
    )


# --- post-pass active-junk monitor (2026-09-14 widget survey) -----------------


def _entry(status: str, source: str, url: str, title: str = "Engineer") -> dict[str, object]:
    return {"status": status, "source": source, "jobLink": url, "title": title}


def test_monitor_counts_active_linkedin_junk_rows() -> None:
    rows = [
        _entry("active", FUSEBOX_SOURCE_ID, FUSEBOX_GUEST_ROW),
        _entry("likely_removed", FUSEBOX_SOURCE_ID, FUSEBOX_GUEST_ROW),
        _entry("archived", FUSEBOX_SOURCE_ID, FUSEBOX_GUEST_ROW),
    ]
    assert count_active_junk_class_rows(rows) == 1


def test_monitor_counts_active_nav_anchor_rows_without_linkedin_url() -> None:
    # The Konami "Community" shape points at the source's own page — the
    # monitor must not prefilter on "linkedin in url" (the 2026-09-14 survey
    # did, and missed four stranded JOBS nav-anchor rows because of it).
    rows = [
        _entry(
            "active",
            "static:listing_url:https://metricminds.com/jobs/",
            "https://metricminds.com/jobs",
            "JOBS",
        )
    ]
    assert count_active_junk_class_rows(rows) == 1


def test_monitor_counts_with_state_prefix_and_when_guard_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        _entry("active", "static_source::" + FUSEBOX_SOURCE_ID, FUSEBOX_GUEST_ROW),
    ]
    assert count_active_junk_class_rows(rows) == 1
    monkeypatch.setattr(common_config, "GUEST_JUNK_GUARD_ENABLED", False)
    assert count_active_junk_class_rows(rows) == 1  # monitor outlives the kill switch


def test_monitor_ignores_sanctioned_and_unidentifiable_sources() -> None:
    rows = [
        _entry("active", LINKEDIN_SOURCE_ID, SANCTIONED_VIEW_ROW),
        _entry("active", "", FUSEBOX_GUEST_ROW),
        {"status": "active", "source": FUSEBOX_SOURCE_ID, "jobLink": "", "title": "Engineer"},
        _entry("active", FUSEBOX_SOURCE_ID, "https://fuseboxgames.com/careers/real-role"),
    ]
    assert count_active_junk_class_rows(rows) == 0


def test_monitor_counts_non_linkedin_origin_harvesting_guest_urls() -> None:
    # A non-LinkedIn origin carrying LinkedIn guest URLs IS the junk class
    # (the sheets-row Fusebox shape) — the monitor counts it even though the
    # origin host is not linkedin.com.
    rows = [_entry("active", "static:listing_url:https://sheets.example.com", FUSEBOX_GUEST_ROW)]
    assert count_active_junk_class_rows(rows) == 1


def test_monitor_tolerates_non_iterable_and_non_dict_entries() -> None:
    assert count_active_junk_class_rows(None) == 0
    assert count_active_junk_class_rows(["nope", 42, None]) == 0


def test_is_junk_class_row_matches_guard_but_ignores_kill_switch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _source("https://fuseboxgames.com/careers/")
    row = {"jobLink": FUSEBOX_GUEST_ROW, "title": "Production Specialist"}
    assert is_junk_class_row(row, source=source)
    monkeypatch.setattr(common_config, "GUEST_JUNK_GUARD_ENABLED", False)
    assert is_junk_class_row(row, source=source)
    assert not is_junk_provenance_row(row, source=source)
