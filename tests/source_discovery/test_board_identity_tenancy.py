"""Board identity on multi-tenant providers: a platform is not a tenant.

Workday and BambooHR each host hundreds of unrelated studios on one registrable domain.
Before this, duplicate resolution keyed on the *adapter name*, so every board on either
platform hashed to a single lookup key, the index held one arbitrary entry, and each board
matched whichever tenant happened to be registered first.

Measured on the 2026-10-04 live run: 64 curated boards carrying 1,257 openings had
`existingProviderSourceId` pointing at a different board, and none pointed at itself.
NVIDIA's Workday board -- 2,000 openings, the single largest block in the catalogue -- was
recorded as a duplicate of *Aristocrat Gaming's* board and marked
`already_covered_by_provider`, so it was never registered. Every Workday and BambooHR
board is affected the same way.

These tests pin the invariant: identity is host + tenant. A shared platform domain is
never identity.
"""

from __future__ import annotations

from typing import Any

from src.source_discovery.core_identity import (
    board_identity_key,
    multi_tenant_provider,
    queue_family_key,
    tenant_host_label,
    tenant_path,
)
from src.source_discovery.provider_migration_advisory import (
    _existing_provider_match,
    _provider_registry_index,
    _row_serves_board,
)

WORKDAY = "myworkdayjobs.com"
BAMBOOHR = "bamboohr.com"


def _workday_row(host: str, site: str, *, adapter: str = "workday") -> dict[str, Any]:
    url = f"https://{host}.{WORKDAY}/{site}"
    return {
        "adapter": adapter,
        "listing_url": url,
        "studio": site,
        "name": f"{site} ({adapter})",
    }


def _registry_row(host: str, site: str, *, adapter: str = "static", state: str = "pending"):
    url = f"https://{host}.{WORKDAY}/{site.lower()}"
    return {
        "id": f"{adapter}:listing_url:{url}",
        "adapter": adapter,
        "listing_url": url,
        "registryState": state,
        "name": f"{site} ({adapter})",
    }


def _match(
    candidate: dict[str, Any],
    index: dict[tuple[str, str], tuple[str, str]],
    family: str = "workday",
):
    return _existing_provider_match(
        candidate, family=family, provider_id=family, provider_index=index
    )[0]


# --- the live defect ---------------------------------------------------------


def test_a_board_does_not_match_a_different_tenant_on_the_same_platform() -> None:
    """The exact 2026-10-04 failure: registry held Aristocrat, NVIDIA was suppressed.

    Both boards live on `*.myworkdayjobs.com`. Before, both resolved to the single key
    `('workday', 'workday')`, so NVIDIA resolved to Aristocrat's row and was marked
    already covered.
    """
    index = _provider_registry_index(
        [_registry_row("aristocrat.wd3", "AristocratExternalCareersSite")], []
    )

    assert _match(_workday_row("nvidia.wd5", "NVIDIAExternalCareerSite"), index) == ""
    assert _match(_workday_row("intel.wd1", "External"), index) == ""


def test_the_matching_tenant_still_matches() -> None:
    """The fix must not blind the index: the real owner is still recognised."""
    index = _provider_registry_index(
        [_registry_row("aristocrat.wd3", "AristocratExternalCareersSite")], []
    )

    matched = _match(_workday_row("aristocrat.wd3", "AristocratExternalCareersSite"), index)

    assert matched.endswith("aristocratexternalcareerssite")


def test_every_curated_workday_board_gets_its_own_key() -> None:
    """17 boards / 1,065 openings collapsed onto one key. Each must be distinct."""
    boards = {
        "nvidia.wd5": "NVIDIAExternalCareerSite",
        "aristocrat.wd3": "AristocratExternalCareersSite",
        "intel.wd1": "External",
        "tencent.wd1": "timi_careers",
        "unitytech.wd1": "Unity",
        "disney.wd5": "disneycareer",
        "lnw.wd5": "LightWonderExternalCareers",
    }
    keys = {queue_family_key(_workday_row(host, site)) for host, site in boards.items()}

    assert len(keys) == len(boards), sorted(keys)


def test_bamboohr_tenants_are_separate_too() -> None:
    """Same defect, same platform family: one subdomain per studio."""
    index = _provider_registry_index(
        [
            {
                "id": "static:listing_url:https://prismstudio.bamboohr.com/careers",
                "adapter": "static",
                "listing_url": "https://prismstudio.bamboohr.com/careers",
                "registryState": "active",
            }
        ],
        [],
    )

    def candidate(host: str) -> dict[str, Any]:
        return {
            "adapter": "bamboohr",
            "listing_url": f"https://{host}.{BAMBOOHR}/careers",
            "studio": host,
        }

    assert _match(candidate("prismstudio"), index, family="bamboohr") != ""
    assert _match(candidate("acmegames"), index, family="bamboohr") == ""


def test_path_tenant_platforms_get_their_own_queue_families() -> None:
    """Every greenhouse board shared one family, so `domain_cap` deferred all but a few.

    Measured 2026-10-05: all 43 deferred greenhouse candidates carried
    `deferReason: domain_cap`. The platform map was missing greenhouse and the other
    path-tenant platforms, so `queue_family_key` fell to `adapter:root_domain` and every
    studio's board on the platform became one family. The tenant on these platforms is
    the path slug.
    """
    rows = [
        {
            "adapter": "greenhouse",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/studio-a/jobs",
        },
        {
            "adapter": "greenhouse",
            "api_url": "https://boards-api.greenhouse.io/v1/boards/studio-b/jobs",
        },
        {
            "adapter": "ashby",
            "api_url": "https://api.ashbyhq.com/posting-api/job-board/studio-c",
        },
        {"adapter": "lever", "listing_url": "https://jobs.lever.co/studio-d"},
        {"adapter": "workable", "listing_url": "https://apply.workable.com/studio-e"},
        {
            "adapter": "smartrecruiters",
            "listing_url": "https://jobs.smartrecruiters.com/StudioF",
        },
    ]
    keys = [queue_family_key(row) for row in rows]
    assert len(set(keys)) == len(rows), keys


def test_subdomain_tenant_platforms_get_their_own_queue_families() -> None:
    """Same rule, host-label tenancy: one shared root domain must not collapse boards."""
    rows = [
        {"adapter": "teamtailor", "listing_url": "https://awaceb.teamtailor.com/"},
        {"adapter": "teamtailor", "listing_url": "https://axolotgamesab.teamtailor.com/"},
        {"adapter": "breezy", "listing_url": "https://flowplay-llc.breezy.hr/"},
        {"adapter": "recruitee", "listing_url": "https://11bitstudios.recruitee.com/"},
        {"adapter": "jazzhr", "listing_url": "https://gamedistrict.applytojob.com/apply"},
        {"adapter": "personio", "listing_url": "https://aesir.jobs.personio.com/"},
        {"adapter": "pinpoint", "listing_url": "https://gameplaygalaxy.pinpointhq.com/"},
    ]
    keys = [queue_family_key(row) for row in rows]
    assert len(set(keys)) == len(rows), keys


# --- the invariant, stated directly -----------------------------------------


def test_same_host_different_path_is_a_different_board() -> None:
    """Tenant in the path, host shared: the path still separates them."""
    site_a = _workday_row("careers.acme.com", "wd/SiteA")
    site_b = _workday_row("careers.acme.com", "wd/SiteB")

    assert board_identity_key(site_a) != board_identity_key(site_b)
    assert not _row_serves_board("workday:listing_url:https://acme.myworkdayjobs.com/SiteA", site_b)


def test_platform_family_comes_from_the_host_not_the_declared_adapter() -> None:
    """A Workday board is routinely registered as `static`.

    Letting the declared adapter decide the platform made a static row stop being
    recognised as tenant-scoped, which is how the tenant-scoped key went missing.
    """
    assert multi_tenant_provider(f"nvidia.wd5.{WORKDAY}") == "workday"
    assert multi_tenant_provider(f"prismstudio.{BAMBOOHR}") == "bamboohr"
    assert multi_tenant_provider("careers.ea.com") == ""
    # The path-tenant and subdomain-tenant platforms belong to the same rule: without
    # them every board on the platform shared one queue family (43 greenhouse
    # candidates deferred by `domain_cap`).
    assert multi_tenant_provider("boards-api.greenhouse.io") == "greenhouse"
    assert multi_tenant_provider("jobs.lever.co") == "lever"
    assert multi_tenant_provider("awaceb.teamtailor.com") == "teamtailor"
    assert multi_tenant_provider("gamedistrict.applytojob.com") == "jazzhr"
    # Same host, either declared adapter: still the same platform.
    assert multi_tenant_provider(f"nvidia.wd5.{WORKDAY}") == multi_tenant_provider(
        f"nvidia.wd5.{WORKDAY}"
    )


def test_tenant_label_and_path_are_the_two_halves_of_a_workday_tenant() -> None:
    assert tenant_host_label(f"nvidia.wd5.{WORKDAY}") == "nvidia.wd5"
    assert tenant_host_label(WORKDAY) == "", "a bare platform host carries no tenant"
    assert tenant_path(f"https://nvidia.wd5.{WORKDAY}/NVIDIAExternalCareerSite") == (
        "nvidiaexternalcareersite"
    )


def test_non_platform_boards_keep_their_previous_family_key() -> None:
    """The fix is scoped. EA and the Ubisoft subdomains are untouched by it."""
    assert queue_family_key(
        {"adapter": "static", "listing_url": "https://careers.ea.com/careers"}
    ) == ("static:ea.com")
    toronto = queue_family_key(
        {"adapter": "static", "listing_url": "https://toronto.ubisoft.com/jobs"}
    )
    berlin = queue_family_key(
        {"adapter": "static", "listing_url": "https://berlin.ubisoft.com/en-us/company"}
    )
    assert toronto == berlin == "static:ubisoft.com", (
        "Ubisoft's subdomains are one studio; only the provider platforms need tenancy"
    )


def test_a_provider_row_on_its_api_host_still_matches_its_board() -> None:
    """Greenhouse rows are keyed by slug on `job-boards.greenhouse.io`.

    The board is the studio's own careers host. Rejecting that as a host mismatch would
    silently stop finding real provider coverage, which is the failure the tenant check
    risks introducing.
    """
    candidate = {
        "adapter": "static",
        "pages": ["https://studio.example/careers"],
        "atsLinks": ["https://boards.greenhouse.io/staticstudio"],
    }

    assert _row_serves_board("greenhouse:slug:staticstudio", candidate)


def test_a_carried_over_existing_id_is_not_trusted_across_tenants() -> None:
    """`existingProviderSourceId` persists on a candidate row between runs.

    Reading it back without re-checking identity is how a cross-tenant match survives
    into the next run after the index stops producing it.
    """
    candidate = _workday_row("nvidia.wd5", "NVIDIAExternalCareerSite")
    candidate["existingProviderSourceId"] = _registry_row("aristocrat.wd3", "Aristocrat")["id"]

    assert _existing_provider_match(
        candidate, family="workday", provider_id="workday", provider_index={}
    ) == ("", "")
