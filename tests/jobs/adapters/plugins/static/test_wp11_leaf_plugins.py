"""Focused tests for the WP11 list-only leaf static plugins (a4vr, amrita, animvs)."""

from __future__ import annotations

from typing import Any, cast

import pytest

from src.jobs.adapters.plugins.static import a4vr, amrita, animvs

_PLUGINS = (a4vr, amrita, animvs)


def _source_row(plugin_name: str) -> dict[str, Any]:
    return {
        "id": plugin_name,
        "name": f"{plugin_name.title()} Careers",
        "studio": f"{plugin_name.title()} Studio",
        "company": f"{plugin_name.title()} Studio",
    }


def _run_plugin(
    plugin: Any,
    *,
    page_url: str,
    html: str,
) -> list[dict[str, Any]]:
    def fetch_text(url: str, timeout_s: int) -> str:
        assert url == page_url
        assert timeout_s == 10
        return html

    return cast(
        list[dict[str, Any]],
        plugin.run(
            fetch_text=fetch_text,
            timeout_s=10,
            retries=0,
            backoff_s=0.0,
            pages=[page_url],
            source_row=_source_row(plugin.__name__.split(".")[-1]),
        ),
    )


@pytest.mark.parametrize(
    ("plugin", "host"),
    [
        (a4vr, "a4vr.com"),
        (a4vr, "www.a4vr.com"),
        (amrita, "amrita.studio"),
        (amrita, "www.amrita.studio"),
        (animvs, "animvs.com"),
        (animvs, "www.animvs.com"),
    ],
)
def test_can_handle_own_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is True


@pytest.mark.parametrize(
    ("plugin", "host"),
    [
        (a4vr, "upsurgestudios.com"),
        (amrita, "outerdawn.com"),
        (animvs, "a4vr.com"),
    ],
)
def test_plugin_rejects_unrelated_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is False


def test_a4vr_extracts_positions_and_excludes_speculative_block() -> None:
    html = """
    <html><body>
    <h2 style="text-align:center;"><strong>POSITION: TECHNICAL ARTIST (Mensch)</strong></h2>
    <p>Ort: Düsseldorf</p>
    <h2 style="text-align:center;"><strong>POSITION: SENIOR 3D ARTIST (Mensch)</strong></h2>
    <p>Ort: Düsseldorf</p>
    <h2 style="text-align:center;"><strong>POSITION: JUNIOR QA ENGINEER (Mensch)</strong></h2>
    <p>Ort: Düsseldorf</p>
    <h2 style="text-align:center;"><strong>INITIATIVBEWERBUNG - TALENTE FÜR VR/AR</strong></h2>
    <p>Send us your CV.</p>
    </body></html>
    """
    rows = _run_plugin(a4vr, page_url="https://a4vr.com/jobs", html=html)
    assert [r["title"] for r in rows] == [
        "TECHNICAL ARTIST (Mensch)",
        "SENIOR 3D ARTIST (Mensch)",
        "JUNIOR QA ENGINEER (Mensch)",
    ]
    assert [r["jobLink"] for r in rows] == [
        "https://a4vr.com/jobs?static-role=technical-artist-(mensch)",
        "https://a4vr.com/jobs?static-role=senior-3d-artist-(mensch)",
        "https://a4vr.com/jobs?static-role=junior-qa-engineer-(mensch)",
    ]
    assert len({r["sourceJobId"] for r in rows}) == 3


def test_a4vr_empty_page_yields_no_rows() -> None:
    assert _run_plugin(a4vr, page_url="https://a4vr.com/jobs", html="<html></html>") == []


def test_amrita_extracts_accordion_panel_roles() -> None:
    html = """
    <html><body>
    <div class="sppb-panel-heading">
      <span class="sppb-panel-title" aria-label="Middle/Senior Unity Developer"><i class="fab fa-connectdevelop" aria-hidden="true"></i> Middle/Senior Unity Developer</span>
    </div>
    <div class="sppb-panel-heading">
      <span class="sppb-panel-title" aria-label="Golang Developer"><i class="fab fa-goodreads-g" aria-hidden="true"></i> Golang Developer</span>
    </div>
    <div class="sppb-panel-heading">
      <span class="sppb-panel-title" aria-label="QA Engineer"><i class="fas fa-gamepad" aria-hidden="true"></i> QA Engineer</span>
    </div>
    </body></html>
    """
    rows = _run_plugin(amrita, page_url="https://amrita.studio/career", html=html)
    assert [r["title"] for r in rows] == [
        "Middle/Senior Unity Developer",
        "Golang Developer",
        "QA Engineer",
    ]
    assert (
        rows[0]["jobLink"]
        == "https://amrita.studio/career?static-role=middle/senior-unity-developer"
    )


def test_amrita_empty_page_yields_no_rows() -> None:
    assert _run_plugin(amrita, page_url="https://amrita.studio/career", html="<html></html>") == []


def test_animvs_extracts_desktop_tabs_only_and_skips_nav_tab() -> None:
    html = """
    <html><body>
    <div id="elementor-tab-title-1811" class="elementor-tab-title elementor-tab-desktop-title" data-tab="1" role="tab">ARTISTA 3D</div>
    <div id="elementor-tab-title-1812" class="elementor-tab-title elementor-tab-desktop-title" data-tab="2" role="tab">GAME DEVELOPER</div>
    <div id="elementor-tab-title-1813" class="elementor-tab-title elementor-tab-desktop-title" data-tab="3" role="tab">LEVEL DESIGNER</div>
    <div class="elementor-tab-title elementor-tab-mobile-title" data-tab="1" role="tab">ARTISTA 3D</div>
    <div class="elementor-tab-title elementor-tab-mobile-title" data-tab="2" role="tab">GAME DEVELOPER</div>
    <div class="elementor-tab-title elementor-tab-desktop-title" data-tab="6" role="tab">work with us</div>
    </body></html>
    """
    rows = _run_plugin(animvs, page_url="https://animvs.com/work-with-us/", html=html)
    assert [r["title"] for r in rows] == [
        "ARTISTA 3D",
        "GAME DEVELOPER",
        "LEVEL DESIGNER",
    ]
    assert [r["jobLink"] for r in rows] == [
        "https://animvs.com/work-with-us/?static-role=artista-3d",
        "https://animvs.com/work-with-us/?static-role=game-developer",
        "https://animvs.com/work-with-us/?static-role=level-designer",
    ]


def test_animvs_empty_page_yields_no_rows() -> None:
    assert (
        _run_plugin(animvs, page_url="https://animvs.com/work-with-us/", html="<html></html>") == []
    )
