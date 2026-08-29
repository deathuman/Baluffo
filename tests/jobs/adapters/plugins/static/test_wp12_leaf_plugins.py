"""Focused tests for the WP12 list-only leaf static plugins (playstack, twirlbound, tatem)."""

from __future__ import annotations

from typing import Any, cast

import pytest

from src.jobs.adapters.plugins.static import playstack, tatem, twirlbound

_PLUGINS = (playstack, twirlbound, tatem)


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
        (playstack, "playstack.com"),
        (playstack, "www.playstack.com"),
        (twirlbound, "twirlbound.com"),
        (twirlbound, "www.twirlbound.com"),
        (tatem, "tatem.games"),
        (tatem, "www.tatem.games"),
    ],
)
def test_can_handle_own_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is True


@pytest.mark.parametrize(
    ("plugin", "host"),
    [
        (playstack, "a4vr.com"),
        (twirlbound, "upsurgestudios.com"),
        (tatem, "twirlbound.com"),
    ],
)
def test_plugin_rejects_unrelated_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is False


def test_playstack_extracts_card_titles_and_filters_hero_heading() -> None:
    html = """
    <html><body>
    <h1><span id="dynamic-title">Join Our Team</span></h1>
    <div class="card">
      <span id="dynamic-title">Localisation Specialist</span>
    </div>
    <div class="card">
      <span id="dynamic-title">Technical Animator - FTC</span>
    </div>
    <div class="card">
      <span id="dynamic-title">PC &amp; Console Games Marketing Manager</span>
    </div>
    </body></html>
    """
    rows = _run_plugin(playstack, page_url="https://playstack.com/careers/", html=html)
    assert [r["title"] for r in rows] == [
        "Localisation Specialist",
        "Technical Animator - FTC",
        "PC & Console Games Marketing Manager",
    ]
    assert rows[2]["jobLink"] == (
        "https://playstack.com/careers/?static-role=pc-and-console-games-marketing-manager"
    )
    assert len({r["sourceJobId"] for r in rows}) == 3


def test_playstack_entity_variants_collapse_to_one_row() -> None:
    """'PC & Console' and 'PC and Console' (same role, entity vs literal) dedupe."""
    html = """
    <html><body>
    <div class="card"><span id="dynamic-title">PC &amp; Console Games Marketing Manager</span></div>
    <div class="card"><span id="dynamic-title">PC and Console Games Marketing Manager</span></div>
    </body></html>
    """
    rows = _run_plugin(playstack, page_url="https://playstack.com/careers/", html=html)
    assert [r["title"] for r in rows] == ["PC & Console Games Marketing Manager"]
    assert len(rows) == 1


def test_playstack_empty_page_yields_no_rows() -> None:
    assert (
        _run_plugin(playstack, page_url="https://playstack.com/careers/", html="<html></html>")
        == []
    )


def test_twirlbound_extracts_accordion_titles_with_unescaped_entities() -> None:
    html = """
    <html><body>
    <p class="wp-block-ub-content-toggle-accordion-title"><strong>Medior / Senior Content Designer</strong></p>
    <p class="wp-block-ub-content-toggle-accordion-title"><strong>Internship &#8211; Content Designer</strong></p>
    <p class="wp-block-ub-content-toggle-accordion-title"><strong>Internship &#8211; 3D Environment Artist</strong></p>
    </body></html>
    """
    rows = _run_plugin(twirlbound, page_url="https://twirlbound.com/jobs/", html=html)
    assert [r["title"] for r in rows] == [
        "Medior / Senior Content Designer",
        "Internship – Content Designer",
        "Internship – 3D Environment Artist",
    ]
    assert rows[1]["jobLink"] == (
        "https://twirlbound.com/jobs/?static-role=internship-–-content-designer"
    )


def test_twirlbound_empty_page_yields_no_rows() -> None:
    assert (
        _run_plugin(twirlbound, page_url="https://twirlbound.com/jobs/", html="<html></html>") == []
    )


def test_tatem_extracts_tilda_card_titles() -> None:
    html = """
    <html><body>
    <div class="t-card__title t-name t-name_lg t650__bottommargin" field="li_title__5970315145540">Junior UI Designer</div>
    <div class="t-card__title t-name t-name_lg t650__bottommargin" field="li_title__5970315145541">Unity Developer</div>
    <div class="t-card__title t-name t-name_lg t650__bottommargin" field="li_title__5970315145542">Game Producer</div>
    </body></html>
    """
    rows = _run_plugin(tatem, page_url="http://tatem.games/tatemjobs", html=html)
    assert [r["title"] for r in rows] == [
        "Junior UI Designer",
        "Unity Developer",
        "Game Producer",
    ]
    assert rows[0]["jobLink"] == "http://tatem.games/tatemjobs?static-role=junior-ui-designer"


def test_tatem_empty_page_yields_no_rows() -> None:
    assert _run_plugin(tatem, page_url="http://tatem.games/tatemjobs", html="<html></html>") == []
