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
