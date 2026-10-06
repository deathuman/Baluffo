"""A refused cross-site redirect should say what it found, not just that it refused.

`_safe_redirect_url` raised `Unsafe static redirect from A to B` for four different situations
-- non-http scheme, credentials in the URL, cross-site, and an https downgrade -- and the fetch
report could not distinguish them. Measured on the live 0.3.008 run, the 135 rows carrying that
error split three ways:

| shape | rows | example |
|---|---:|---|
| cross-site to a known ATS host | **5** | `studiowildcard.com -> studiowildcard.bamboohr.com` |
| cross-site to an unknown host | 118 | `foxandsheep.com -> linkedin.com` (closed), `exozet.com -> endava.com` (acquired) |
| same-site https downgrade | 12 | `ninjatheory.com/careers -> http://...` |

Only the first is a platform migration, which is the Ubisoft shape: the board did not break, it
*moved*, and the fix is to register it against the platform rather than keep scraping a URL that
now redirects elsewhere. The 118 are closures, rebrands and acquisitions -- treating those as
migrations would point discovery at `linkedin.com`.

These tests pin the split, and pin that the guard still refuses: classification is a diagnosis
attached to a refusal, never a licence to follow the target.
"""

from __future__ import annotations

import pytest

from src.jobs.adapters.static_runtime_support import _static_redirect_error
from src.source_discovery.redirect_classification import (
    INSECURE_DOWNGRADE,
    MIGRATION,
    SITE_GONE,
    classify_cross_site_redirect,
)

# --- the five real migrations ----------------------------------------------------------


@pytest.mark.parametrize(
    ("source", "target", "adapter", "tenant"),
    [
        (
            "https://www.ubisoft.com/jobs",
            "https://jobs.smartrecruiters.com/Ubisoft2",
            "smartrecruiters",
            "ubisoft2",
        ),
        (
            "https://www.ghoststorygames.com/careers",
            "https://job-boards.greenhouse.io/ghoststorygames",
            "greenhouse",
            "ghoststorygames",
        ),
        (
            "https://www.naturalmotion.com",
            "https://job-boards.greenhouse.io/naturalmotion",
            "greenhouse",
            "naturalmotion",
        ),
        (
            "https://everplaygroupplc.com",
            "https://apply.workable.com/everplaygroupplc/",
            "workable",
            "everplaygroupplc",
        ),
    ],
)
def test_a_move_to_a_known_platform_is_a_migration(source, target, adapter, tenant):
    verdict = classify_cross_site_redirect(source, target)
    assert verdict.kind == MIGRATION
    assert verdict.kind == MIGRATION
    assert verdict.adapter == adapter
    assert verdict.tenant == tenant


def test_a_bamboohr_migration_names_the_adapter_without_inventing_a_tenant():
    """BambooHR puts the tenant on the *host*, not in the path.

    Reading a tenant off the path here would produce ``""`` at best and a wrong studio at
    worst, so it reports unknown and leaves tenancy to whoever registers the board.
    """
    verdict = classify_cross_site_redirect(
        "https://www.studiowildcard.com", "https://studiowildcard.bamboohr.com"
    )
    assert verdict.kind == MIGRATION
    assert verdict.adapter == "bamboohr"
    assert verdict.tenant == ""
    assert "tenant=unknown" in _static_redirect_error(
        "https://www.studiowildcard.com", "https://studiowildcard.bamboohr.com"
    )


# --- the 118 that are not migrations --------------------------------------------------


@pytest.mark.parametrize(
    ("source", "target", "note"),
    [
        (
            "https://www.foxandsheep.com/jobs",
            "https://www.linkedin.com/company/fox-and-sheep/jobs/",
            "studio closed",
        ),
        ("https://www.roblox.com/jobs", "https://corp.roblox.com/careers", "rebrand"),
        ("https://exozet.com", "https://endava.com/", "acquired"),
        ("https://game-labs.net", "https://stillfront.com", "acquired"),
        ("https://www.wildcardstudios.com", "https://wildcardmobile.com", "rebrand"),
    ],
)
def test_a_redirect_to_a_host_no_platform_owns_is_not_a_migration(source, target, note):
    """The dominant shape, and the one a looser classifier would get wrong.

    ``linkedin.com`` and ``corp.roblox.com`` are not applicant-tracking platforms. Calling
    either a migration would point discovery at a host that serves no board.
    """
    verdict = classify_cross_site_redirect(source, target)
    assert verdict.kind == SITE_GONE, note
    assert verdict.kind != MIGRATION
    assert verdict.adapter == ""


def test_a_rebrand_to_a_sibling_domain_is_still_not_a_migration():
    """`roblox.com -> corp.roblox.com` changes the host but not the organisation.

    Stripping `www.` is the only normalisation, so a subdomain rebrand stays cross-site --
    which is correct, because nothing here knows the two hosts are related.
    """
    verdict = classify_cross_site_redirect(
        "https://www.roblox.com/jobs", "https://corp.roblox.com/careers"
    )
    assert verdict.kind == SITE_GONE


# --- the 12 downgrades are a third thing ----------------------------------------------


def test_a_same_site_https_downgrade_is_not_a_move():
    verdict = classify_cross_site_redirect(
        "https://www.ninjatheory.com/careers", "http://www.ninjatheory.com/careers/"
    )
    assert verdict.kind == INSECURE_DOWNGRADE
    assert verdict.kind != MIGRATION


def test_a_www_prefix_does_not_make_a_same_site_redirect_cross_site():
    """`certainaffinity.com -> www.certainaffinity.com` is the same site, and a downgrade."""
    verdict = classify_cross_site_redirect(
        "https://certainaffinity.com/careers", "http://www.certainaffinity.com/careers/"
    )
    assert verdict.kind == INSECURE_DOWNGRADE


# --- the trailing semicolon --------------------------------------------------------------


def test_a_trailing_semicolon_is_stripped_from_the_target_url():
    """Servers emit it and it is legal under RFC 3986, so the URL parses fine either way.

    The harm is only downstream: copied verbatim into a registration URL it leaves a stray
    character on the end. Note `urlparse` puts the text before `;` in `path` and the rest in
    `params`, so stripping `path` is a no-op -- the character has to come off the raw string.
    """
    verdict = classify_cross_site_redirect(
        "https://www.roblox.com/jobs", "https://corp.roblox.com/careers;"
    )
    assert verdict.target_url == "https://corp.roblox.com/careers"


def test_the_semicolon_strip_keeps_query_and_fragment_intact():
    """A `;` that terminates the query or fragment is not the same defect."""
    query = classify_cross_site_redirect("https://a.com/x", "https://b.com/y?x=1;")
    assert query.target_url == "https://b.com/y?x=1;"
    fragment = classify_cross_site_redirect("https://a.com/x", "https://b.com/y#frag;")
    assert fragment.target_url == "https://b.com/y#frag;"


def test_a_url_without_a_semicolon_is_returned_unchanged():
    verdict = classify_cross_site_redirect("https://a.com/x", "https://b.com/y")
    assert verdict.target_url == "https://b.com/y"


# --- the guard still refuses ------------------------------------------------------------


def test_the_error_still_says_the_redirect_was_not_followed():
    """Classification is a diagnosis attached to a refusal, never a licence to follow.

    The word "unsafe" and the refusal itself must survive: this change adds a diagnosis to an
    existing error, and if it ever made the redirect *permitted* the guard would be gone.
    """
    message = _static_redirect_error(
        "https://www.roblox.com/jobs", "https://corp.roblox.com/careers"
    )
    assert message.startswith(
        "Unsafe static redirect from https://www.roblox.com/jobs to https://corp.roblox.com/careers"
    )
    assert "[site_gone_or_moved]" in message


def test_the_message_names_the_adapter_for_a_migration():
    message = _static_redirect_error(
        "https://www.ubisoft.com/jobs", "https://jobs.smartrecruiters.com/Ubisoft2"
    )
    assert "[platform_migration" in message
    assert "adapter=smartrecruiters" in message
    assert "tenant=ubisoft2" in message


def test_a_migration_message_still_refuses():
    """The most important negative: a recognised migration is still a refusal.

    Following a target the guard rejected is the thing this work must not reintroduce, and it
    would be easy to introduce by treating "platform_migration" as permission.
    """
    message = _static_redirect_error(
        "https://www.ubisoft.com/jobs", "https://jobs.smartrecruiters.com/Ubisoft2"
    )
    assert message.startswith("Unsafe static redirect")
    assert "followed" not in message.lower()


# --- host matching is suffix-based, not substring --------------------------------------


def test_platform_matching_is_suffix_based():
    """`job-boards.eu.greenhouse.io` is Greenhouse; `notgreenhouse.io` is not."""
    regional = classify_cross_site_redirect(
        "https://a.com/x", "https://job-boards.eu.greenhouse.io/acme"
    )
    assert regional.adapter == "greenhouse"
    impostor = classify_cross_site_redirect("https://a.com/x", "https://notgreenhouse.io/acme")
    assert impostor.kind == SITE_GONE


def test_an_empty_or_unusable_url_does_not_raise():
    """Hostile or malformed input is classified, not crashed on.

    `Location` is attacker-influenced in the general case, so this reads only the host and
    never resolves or fetches anything.
    """
    for source, target in (("", "https://b.com"), ("https://a.com", ""), ("not-a-url", "also-not")):
        verdict = classify_cross_site_redirect(source, target)
        assert verdict.kind in {MIGRATION, SITE_GONE, INSECURE_DOWNGRADE, "unparseable_redirect"}
        assert verdict.kind != MIGRATION or verdict.adapter
