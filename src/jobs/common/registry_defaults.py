"""Default registry rows and provider-redundancy rules for jobs sources."""

from __future__ import annotations

from typing import Any

DEFAULT_STUDIO_SOURCE_REGISTRY: list[dict[str, Any]] = [
    {
        "name": "Guerrilla Games",
        "studio": "Guerrilla Games",
        "adapter": "greenhouse",
        "slug": "guerrilla-games",
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "PlayStation Global",
        "studio": "PlayStation Global",
        "adapter": "greenhouse",
        "slug": "sonyinteractiveentertainmentglobal",
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "Paradox Careers",
        "studio": "Paradox Interactive",
        "adapter": "teamtailor",
        "listing_url": "https://career.paradoxplaza.com/jobs",
        "base_url": "https://career.paradoxplaza.com",
        "company": "Paradox Interactive",
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "Little Chicken",
        "studio": "Little Chicken",
        "adapter": "static",
        "company": "Little Chicken",
        "pages": [
            "https://www.littlechicken.nl/about-us/jobs/",
            "https://www.littlechicken.nl/job/",
        ],
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "Larian Studios",
        "studio": "Larian Studios",
        "adapter": "greenhouse",
        "slug": "larian-studios",
        "nlPriority": True,
        "enabledByDefault": False,
    },
    {
        "name": "Bandai Namco Entertainment America (Greenhouse)",
        "studio": "Bandai Namco Entertainment America Inc.",
        "adapter": "greenhouse",
        "slug": "bandainamco",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Jagex (Lever)",
        "studio": "Jagex",
        "adapter": "lever",
        "account": "jagex",
        "api_url": "https://api.lever.co/v0/postings/jagex?mode=json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Sandbox VR (Lever)",
        "studio": "Sandbox VR",
        "adapter": "lever",
        "account": "sandboxvr",
        "api_url": "https://api.lever.co/v0/postings/sandboxvr?mode=json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Voodoo (Lever)",
        "studio": "Voodoo",
        "adapter": "lever",
        "account": "voodoo",
        "api_url": "https://api.lever.co/v0/postings/voodoo?mode=json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "CD PROJEKT RED (SmartRecruiters)",
        "studio": "CD PROJEKT RED",
        "adapter": "smartrecruiters",
        "company_id": "CDPROJEKTRED",
        "api_url": "https://api.smartrecruiters.com/v1/companies/CDPROJEKTRED/postings",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Gameloft (SmartRecruiters)",
        "studio": "Gameloft",
        "adapter": "smartrecruiters",
        "company_id": "Gameloft",
        "api_url": "https://api.smartrecruiters.com/v1/companies/Gameloft/postings",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Hutch (Workable)",
        "studio": "Hutch",
        "adapter": "workable",
        "account": "hutch",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/hutch?details=true",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Wargaming (Workable)",
        "studio": "Wargaming",
        "adapter": "workable",
        "account": "wargaming",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/wargaming?details=true",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "CrazyGames (Recruitee)",
        "studio": "CrazyGames",
        "adapter": "recruitee",
        "subdomain": "jobs.crazygames.com",
        "api_url": "https://jobs.crazygames.com/api/offers/",
        "nlPriority": True,
        "enabledByDefault": True,
    },
    {
        "name": "Gameplay Galaxy (Pinpoint)",
        "studio": "Gameplay Galaxy",
        "adapter": "pinpoint",
        "subdomain": "gameplaygalaxy",
        "api_url": "https://gameplaygalaxy.pinpointhq.com/postings.json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "YallaPlay (Breezy)",
        "studio": "YallaPlay",
        "adapter": "breezy",
        "board_url": "https://yallaplay.breezy.hr/",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Lost Boys Interactive (JazzHR)",
        "studio": "Lost Boys Interactive",
        "adapter": "jazzhr",
        "board_url": "https://lostboysinteractive.applytojob.com/apply",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Next Level Games (JazzHR)",
        "studio": "Next Level Games",
        "adapter": "jazzhr",
        "board_url": "https://nextlevelgames.applytojob.com/apply",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Ubisoft (SmartRecruiters)",
        "studio": "Ubisoft",
        "adapter": "smartrecruiters",
        "company_id": "Ubisoft2",
        "api_url": "https://api.smartrecruiters.com/v1/companies/Ubisoft2/postings",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    # Reclassified 2026-11-18 from the 0.3.066 fetch report. Each row below was the
    # ATS a failing static careers page actually served, verified against the live
    # board API on that date (posting counts in the PR/commit body).
    {
        "name": "Endless Studios (BambooHR)",
        "studio": "Endless Studios",
        "adapter": "bamboohr",
        "listing_url": "https://endlessstudios.bamboohr.com/careers",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "LBC Studio (BambooHR)",
        "studio": "LBC Studio",
        "adapter": "bamboohr",
        "listing_url": "https://lbcstudiosca.bamboohr.com/careers",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "High Voltage Software (BambooHR)",
        "studio": "High Voltage Software",
        "adapter": "bamboohr",
        "listing_url": "https://highvoltagesoftware.bamboohr.com/careers",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Keen Software House (Greenhouse)",
        "studio": "Keen Software House",
        "adapter": "greenhouse",
        "slug": "kswh",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Neowiz (Lever)",
        "studio": "Neowiz",
        "adapter": "lever",
        "account": "neowiz",
        "api_url": "https://api.lever.co/v0/postings/neowiz?mode=json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Playco (Lever)",
        "studio": "Playco",
        "adapter": "lever",
        "account": "playco",
        "api_url": "https://api.lever.co/v0/postings/playco?mode=json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Skydance Interactive (Lever)",
        "studio": "Skydance Interactive",
        "adapter": "lever",
        "account": "skydance",
        "api_url": "https://api.lever.co/v0/postings/skydance?mode=json",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Don't Nod (SmartRecruiters)",
        "studio": "Don't Nod",
        "adapter": "smartrecruiters",
        "company_id": "dontnod",
        "api_url": "https://api.smartrecruiters.com/v1/companies/dontnod/postings",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Unchained Entertainment (SmartRecruiters)",
        "studio": "Unchained Entertainment",
        "adapter": "smartrecruiters",
        "company_id": "citystateentertainmentllc",
        "api_url": (
            "https://api.smartrecruiters.com/v1/companies/citystateentertainmentllc/postings"
        ),
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Anzu.io (Workable)",
        "studio": "Anzu.io",
        "adapter": "workable",
        "account": "anzu",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/anzu?details=true",
        "nlPriority": False,
        "enabledByDefault": True,
    },
    {
        "name": "Square Enix (Workable)",
        "studio": "Square Enix",
        "adapter": "workable",
        "account": "square-enix",
        "api_url": ("https://apply.workable.com/api/v1/widget/accounts/square-enix?details=true"),
        "nlPriority": False,
        "enabledByDefault": True,
    },
]

REDUNDANT_STATIC_IF_PROVIDER: list[dict[str, Any]] = [
    {
        "hosts": ["cdprojektred.com", "www.cdprojektred.com"],
        "adapter": "smartrecruiters",
        "provider_id_field": "company_id",
        "provider_id_value": "CDPROJEKTRED",
    },
    {
        # `*.ubisoft.com` is the load-bearing entry. Every Ubisoft careers board is a
        # regional subdomain (toronto, berlin, mainz, duesseldorf, saguenay, stockholm,
        # winnipeg); the apex and `www.` alone matched none of them, so no regional row was
        # ever recognised as provider-served. The static adapter then spent 2,760 s on
        # toronto.ubisoft.com to keep 1 job from a page that serves zero job links, while
        # the SmartRecruiters board holds 333 postings covering those same studios.
        "hosts": ["*.ubisoft.com", "ubisoft.com", "www.ubisoft.com"],
        "adapter": "smartrecruiters",
        "provider_id_field": "company_id",
        "provider_id_value": "Ubisoft2",
    },
    {
        "hosts": ["xsolla.com", "www.xsolla.com"],
        "adapter": "lever",
        "provider_id_field": "account",
        "provider_id_value": "xsolla",
    },
    {
        "hosts": ["bandainamcoent.com", "www.bandainamcoent.com"],
        "adapter": "greenhouse",
        "provider_id_field": "slug",
        "provider_id_value": "bandainamco",
    },
    {
        "hosts": ["*.bamboohr.com", "bamboohr.com"],
        "adapter": "bamboohr",
        "provider_id_field": "adapter",
        "provider_id_value": "bamboohr",
    },
    {
        "hosts": ["*.myworkdayjobs.com", "*.workday.com", "workday.com"],
        "adapter": "workday",
        "provider_id_field": "adapter",
        "provider_id_value": "workday",
    },
    {
        "hosts": ["nextlevelgames.com", "www.nextlevelgames.com"],
        "adapter": "jazzhr",
        "provider_id_field": "board_url",
        "provider_id_value": "https://nextlevelgames.applytojob.com/apply",
    },
    # Verified 2026-11-18 against the live board APIs. Only hosts whose ATS board
    # answered with the studio's own tenant are listed: 13 other signature matches
    # were aggregator pages, vendor demo tenants, or dead boards and are deliberately
    # absent, so their static rows keep running instead of being suppressed.
    # BambooHR rows carry the tenant in their listing URL, not in ``adapter``: using
    # ``adapter`` here would suppress a studio host whenever *any* BambooHR board is
    # registered, including an unrelated tenant.
    {
        "hosts": ["endlessstudios.com", "www.endlessstudios.com", "about.endlessstudios.com"],
        "adapter": "bamboohr",
        "provider_id_field": "listing_url",
        "provider_id_value": "https://endlessstudios.bamboohr.com/careers",
    },
    {
        "hosts": ["lbcstudios.ca", "www.lbcstudios.ca"],
        "adapter": "bamboohr",
        "provider_id_field": "listing_url",
        "provider_id_value": "https://lbcstudiosca.bamboohr.com/careers",
    },
    {
        "hosts": ["high-voltage.com", "www.high-voltage.com"],
        "adapter": "bamboohr",
        "provider_id_field": "listing_url",
        "provider_id_value": "https://highvoltagesoftware.bamboohr.com/careers",
    },
    {
        "hosts": ["keenswh.com", "www.keenswh.com"],
        "adapter": "greenhouse",
        "provider_id_field": "slug",
        "provider_id_value": "kswh",
    },
    {
        "hosts": ["neowiz.com", "www.neowiz.com"],
        "adapter": "lever",
        "provider_id_field": "account",
        "provider_id_value": "neowiz",
    },
    {
        "hosts": ["play.co", "www.play.co"],
        "adapter": "lever",
        "provider_id_field": "account",
        "provider_id_value": "playco",
    },
    {
        "hosts": ["skydance.com", "www.skydance.com"],
        "adapter": "lever",
        "provider_id_field": "account",
        "provider_id_value": "skydance",
    },
    {
        "hosts": ["dont-nod.com", "www.dont-nod.com"],
        "adapter": "smartrecruiters",
        "provider_id_field": "company_id",
        "provider_id_value": "dontnod",
    },
    {
        "hosts": ["unchained-entertainment.com", "www.unchained-entertainment.com"],
        "adapter": "smartrecruiters",
        "provider_id_field": "company_id",
        "provider_id_value": "citystateentertainmentllc",
    },
    {
        "hosts": ["anzu.io", "www.anzu.io"],
        "adapter": "workable",
        "provider_id_field": "account",
        "provider_id_value": "anzu",
    },
    {
        "hosts": ["square-enix-games.com", "www.square-enix-games.com"],
        "adapter": "workable",
        "provider_id_field": "account",
        "provider_id_value": "square-enix",
    },
]


__all__ = ["DEFAULT_STUDIO_SOURCE_REGISTRY", "REDUNDANT_STATIC_IF_PROVIDER"]
