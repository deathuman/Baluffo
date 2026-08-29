"""Tests for the shared Phenom \"phApp\" careers-platform static adapter.

WP17 (jobs-coverage plan): Activision, Blizzard, King, Treyarch, Raven,
Sledgehammer, Warner Bros. Games, Scopely, and more run the proprietary Phenom
\"CareerConnect\" platform. Their widget-API JSON is gated behind a tenant + CSRF
session, but every jobsite publishes an open per-locale sitemap that lists job
pages as `/job/{jobCode}/{slug}` URLs, and each detail page is server-rendered
with a stable `<title>`. These tests pin the sitemap-URL collection, the    title/location extraction (both the Blizzard "job in … | … jobs at" and King
    "in … | … at" forms), the kind-of-empty behavior, and that the shared plugin
    outranks the zero-yield per-host blizzard/activision plugins in the registry.
"""

from __future__ import annotations

from typing import Any, cast

from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.static import phapp

_PHA_HOST = "careers.blizzard.com"


def _ctx(host: str) -> Any:
    return cast(Any, type("Ctx", (), {"source_identity": host})())


# --- sitemap URL collection -------------------------------------------------


def test_sitemap_candidates_derives_locale_and_fallbacks() -> None:
    assert phapp.sitemap_candidates("https://careers.blizzard.com/global/en") == [
        "https://careers.blizzard.com/global/en/sitemap.xml",
        "https://careers.blizzard.com/sitemap.xml",
    ]
    assert phapp.sitemap_candidates("https://careers.king.com/") == [
        "https://careers.king.com/global/en/sitemap.xml",
        "https://careers.king.com/sitemap.xml",
    ]
    assert phapp.sitemap_candidates("") == []
    assert phapp.sitemap_candidates("ftp://careers.king.com") == []


def test_collect_job_urls_extracts_job_slugs_and_dedupes() -> None:
    sitemap = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc><![CDATA[https://careers.king.com/us/en/job/R027381/Senior-Principal-AI-ML-Engineer]]></loc></url>
      <url><loc>https://careers.king.com/us/en/job/R027884/QA-Lead-Minecraft-Blast</loc></url>
      <url><loc>https://careers.king.com/us/en/job/R027381/Senior-Principal-AI-ML-Engineer</loc></url>
    </urlset>"""
    urls = phapp.collect_job_urls(sitemap, "https://careers.king.com/")
    assert urls == [
        "https://careers.king.com/us/en/job/R027381/Senior-Principal-AI-ML-Engineer",
        "https://careers.king.com/us/en/job/R027884/QA-Lead-Minecraft-Blast",
    ]


def test_collect_job_urls_handles_sitemap_index_and_relative() -> None:
    sitemap = """<?xml version="1.0"?>
    <sitemapindex>
      <sitemap><loc>https://careers.king.com/us/en/sitemap-0.xml</loc></sitemap>
      <sitemap><loc>https://careers.king.com/us/en/sitemap-1.xml</loc></sitemap>
    </sitemapindex>"""
    # An index carries no /job/ slugs itself -> no job URLs, no crash.
    assert phapp.collect_job_urls(sitemap, "https://careers.king.com/") == []


# --- title/location extraction ----------------------------------------------

_BLIZZARD_DETAIL = """
<html><head>
<title>Senior Combat Designer, Systems - World of Warcraft | Irvine, CA job in Irvine, California, United States of America | Game Design jobs at Blizzard Entertainment</title>
</head><body></body></html>
"""

_KING_DETAIL = """
<html><head>
<title>Senior Principal AI/ML Engineer in Stockholm, Stockholm County, Sweden | Data, Analytics &amp; Strategy at King</title>
</head><body></body></html>
"""


def test_extract_meta_parses_blizzard_title_shape() -> None:
    meta = phapp.extract_phapp_job_meta(
        _BLIZZARD_DETAIL,
        job_url="https://careers.blizzard.com/global/en/job/R027923/Senior-Combat-Designer",
        fallback_company="Blizzard",
    )
    assert meta is not None
    assert meta["title"] == "Senior Combat Designer, Systems - World of Warcraft"
    assert meta["city"] == "Irvine"
    assert meta["country"] == "United States of America"
    assert meta["company"] == "Blizzard Entertainment"
    assert meta["sourceJobId"] == "phapp:R027923"


def test_extract_meta_parses_king_title_shape() -> None:
    meta = phapp.extract_phapp_job_meta(
        _KING_DETAIL,
        job_url="https://careers.king.com/us/en/job/R027381/Senior-Principal-AI-ML-Engineer",
        fallback_company="King",
    )
    assert meta is not None
    assert meta["title"] == "Senior Principal AI/ML Engineer"
    assert meta["city"] == "Stockholm"
    assert meta["country"] == "SE"  # generic location parser normalizes Sweden
    assert meta["company"] == "King"


def test_extract_meta_falls_back_to_url_slug() -> None:
    meta = phapp.extract_phapp_job_meta(
        "<html><body>nothing usable</body></html>",
        job_url="https://careers.king.com/us/en/job/R027884/QA-Lead-Minecraft-Blast",
        fallback_company="King",
    )
    assert meta is not None
    # Slug-derived title still carries a usable identity + job id.
    assert "QA" in meta["title"]
    assert meta["sourceJobId"] == "phapp:R027884"


def test_extract_meta_returns_none_only_when_no_signal() -> None:
    assert (
        phapp.extract_phapp_job_meta(
            "<html><body>denied</body></html>",
            job_url="https://careers.king.com/",
            fallback_company="King",
        )
        is None
    )


def test_can_handle_platform_hosts() -> None:
    assert phapp.can_handle(_ctx("careers.blizzard.com")) is True
    assert phapp.can_handle(_ctx("careers.king.com")) is True
    assert phapp.can_handle(_ctx("careers.activision.com")) is True
    assert phapp.can_handle(_ctx("careers.treyarch.com")) is True
    assert phapp.can_handle(_ctx("www.scopely.com")) is True


def test_can_handle_rejects_unrelated_hosts() -> None:
    assert phapp.can_handle(_ctx("upsurgestudios.com")) is False
    assert phapp.can_handle(_ctx("playstack.com")) is False


# --- end-to-end run -----------------------------------------------------------


def _run(page_url: str, *, routes: dict[str, str]) -> list[dict[str, Any]]:
    def fetch_text(url: str, timeout_s: int) -> str:
        return routes[url]

    return cast(
        list[dict[str, Any]],
        phapp.run(
            fetch_text=fetch_text,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            pages=[page_url],
            source_row={"id": "wp17", "url": page_url, "company": "King"},
        ),
    )


def test_run_recovers_jobs_from_sitemap() -> None:
    sitemap = """<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
      <url><loc>https://careers.king.com/us/en/job/R027381/Senior-Principal-AI-ML-Engineer</loc></url>
      <url><loc>https://careers.king.com/us/en/job/R027884/QA-Lead-Minecraft-Blast</loc></url>
    </urlset>"""
    detail = """<html><head><title>{title} in Stockholm, Sweden | Engineering at King</title></head></html>"""
    routes = {
        "https://careers.king.com/global/en/sitemap.xml": sitemap,
        "https://careers.king.com/us/en/job/R027381/Senior-Principal-AI-ML-Engineer": detail.format(
            title="Senior Principal AI/ML Engineer"
        ),
        "https://careers.king.com/us/en/job/R027884/QA-Lead-Minecraft-Blast": detail.format(
            title="QA Lead"
        ),
    }
    rows = _run("https://careers.king.com/", routes=routes)
    assert [r["title"] for r in rows] == ["Senior Principal AI/ML Engineer", "QA Lead"]
    assert rows[0]["jobLink"].endswith("/Senior-Principal-AI-ML-Engineer")
    assert rows[0]["sourceJobId"] == "phapp:R027381"
    assert rows[0]["company"] == "King"


def test_run_yields_empty_when_no_sitemap_or_no_jobs() -> None:
    routes = {
        "https://careers.king.com/global/en/sitemap.xml": "<html>no sitemap</html>",
        "https://careers.king.com/sitemap.xml": "<html>no sitemap</html>",
    }
    assert _run("https://careers.king.com/", routes=routes) == []


# --- registry selection -------------------------------------------------------


def test_registry_selects_phapp_for_widget_only_platform_hosts() -> None:
    # King has no dedicated per-host plugin, so the shared adapter must win the
    # single-plugin selection for it.
    ctx_kwargs = {
        "family": "static",
        "adapter_key": "static",
        "source_identity": "careers.king.com",
    }
    ctx = cast(Any, type("Ctx", (), ctx_kwargs)())
    plugin, selection = default_registry.select(ctx)
    assert plugin.name == "phapp"
    assert selection.plugin_name == "phapp"
