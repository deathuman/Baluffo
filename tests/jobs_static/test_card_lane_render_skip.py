"""Card-lane render-skip oracle (PS Nuxt flip remediation, 2026-09-15).

The generic runner escalates a listing page to Playwright when the generic
parser returns zero rows on a JS-shell-shaped static document. But a board
can flip to an SSR variant whose document is a shell for the generic parser
while the rendered-card lane extracts its job rows from the same static
document — escalating then gambles the whole page on a render.

The oracle consults the same extractor the card-row flow runs next (same
config: allow_any_anchor, real company/source id) and keeps the static
document when the lane already parses rows from it. Fail-open: any oracle
error falls back to the legacy render path; the kill switch
(``BALUFFO_STATIC_CARD_LANE_RENDER_SKIP``) restores render-first escalation.
"""

from __future__ import annotations

import pytest

from src.jobs.adapters.static_listing_runner import StaticFetchRunner
from src.jobs.adapters.static_runtime import StaticRunDeps
from src.jobs.text_utils import clean_text
from tests.jobs_static.test_static_zero_kept_guard import _make_static_context

# PS-flip shape: SPA shell tokens (generic parser 0, detect_js_shell True)
# plus SSR job cards the card lane extracts from the same static document.
PS_FLIP_SHAPE = (
    "<html><head><title>Careers - PlayStation</title></head><body>"
    '<div id="app"></div><script src="/_nuxt/entry.abc123.js"></script>'
    '<div class="job-card">'
    '<a href="/en-us/jobs/engineering/senior-tools-programmer/job/P1-2528263-0">'
    "Senior Tools Programmer</a></div>"
    '<div class="job-card">'
    '<a href="/en-us/jobs/engineering/gameplay-engineer/job/P1-2528301-0">'
    "Gameplay Engineer</a></div>"
    '<nav><a href="/en-us/benefits">Benefits</a></nav>'
    "</body></html>"
)
_BARE_SHELL = (
    '<html><body><div id="app"></div>'
    '<nav><a href="/en-us/benefits">Benefits</a></nav></body></html>'
)
_RENDERED = (
    '<html><body><div id="app">'
    '<div class="job-card"><a href="/en-us/jobs/engineering/rendered/job/2">Rendered Job</a></div>'
    "</div></body></html>"
)


def _make_runner_ctx(try_playwright):
    ctx = _make_static_context(
        pages=["https://careers.playstation.com/en-us/jobs/"],
    )
    ctx.run_deps = StaticRunDeps(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
        try_playwright=try_playwright,
    )
    ctx.source["id"] = "static:siei"
    return ctx


def _forbid_render(url, _budget):
    raise AssertionError(f"render must not be called, was called for {url}")


def test_js_shell_page_with_parseable_cards_skips_render() -> None:
    """PS-flip regression pin: a shell-shaped static document whose card lane
    parses rows is kept as-is — no Playwright escalation, no render bill."""
    ctx = _make_runner_ctx(_forbid_render)
    runner = StaticFetchRunner(ctx)

    html = runner._try_playwright_fallback(
        PS_FLIP_SHAPE,
        "https://careers.playstation.com/en-us/jobs/",
        5,
        "js_shell",
        True,
    )

    assert html == PS_FLIP_SHAPE


def test_escalates_when_card_lane_finds_nothing() -> None:
    """A bare shell the card lane cannot parse still escalates to render."""
    render_calls: list[str] = []

    def try_playwright(url, _budget):
        render_calls.append(url)
        return _RENDERED, ""

    ctx = _make_runner_ctx(try_playwright)
    runner = StaticFetchRunner(ctx)

    html = runner._try_playwright_fallback(
        _BARE_SHELL,
        "https://careers.playstation.com/en-us/jobs/",
        5,
        "js_shell",
        True,
    )

    assert render_calls == ["https://careers.playstation.com/en-us/jobs/"]
    assert html == _RENDERED


def test_parseable_generic_page_never_renders() -> None:
    """The pre-existing parsed_pre early-return is untouched: a page the
    generic parser handles never reaches either the oracle or the render."""

    class _Forbidden(Exception):
        pass

    def try_playwright(_url, _budget):
        raise _Forbidden

    ctx = _make_runner_ctx(try_playwright)
    runner = StaticFetchRunner(ctx)
    generic_doc = (
        "<html><body><table>"
        '<tr class="job"><td><a href="/jobs/1">Engine Programmer</a></td></tr>'
        "</table></body></html>"
    )

    html = runner._try_playwright_fallback(
        generic_doc,
        "https://example.com/careers",
        5,
        "js_shell",
        True,
    )

    assert html == generic_doc


def test_kill_switch_restores_render_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """BALUFFO_STATIC_CARD_LANE_RENDER_SKIP=0 disables the oracle entirely."""
    monkeypatch.setenv("BALUFFO_STATIC_CARD_LANE_RENDER_SKIP", "0")
    render_calls: list[str] = []

    def try_playwright(url, _budget):
        render_calls.append(url)
        return _RENDERED, ""

    ctx = _make_runner_ctx(try_playwright)
    runner = StaticFetchRunner(ctx)

    html = runner._try_playwright_fallback(
        PS_FLIP_SHAPE,
        "https://careers.playstation.com/en-us/jobs/",
        5,
        "js_shell",
        True,
    )

    assert render_calls == ["https://careers.playstation.com/en-us/jobs/"]
    assert html == _RENDERED


def test_oracle_failure_fails_open_to_render(monkeypatch: pytest.MonkeyPatch) -> None:
    """An oracle crash must never break the listing pass — it falls back to
    the legacy render-first escalation."""

    def _broken(self, _html, _page_url):
        raise RuntimeError("oracle exploded")

    monkeypatch.setattr(StaticFetchRunner, "_card_lane_parses_static_html", _broken)
    render_calls: list[str] = []

    def try_playwright(url, _budget):
        render_calls.append(url)
        return _RENDERED, ""

    ctx = _make_runner_ctx(try_playwright)
    runner = StaticFetchRunner(ctx)

    html = runner._try_playwright_fallback(
        PS_FLIP_SHAPE,
        "https://careers.playstation.com/en-us/jobs/",
        5,
        "js_shell",
        True,
    )

    assert render_calls == ["https://careers.playstation.com/en-us/jobs/"]
    assert html == _RENDERED


def test_oracle_config_matches_downstream_card_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parity pin: the oracle invokes the card extractor with the same config
    the downstream card-row flow uses (allow_any_anchor, real company and
    source id) — a skip can never starve the lane of rows it would find."""
    captured: dict[str, object] = {}

    def _spy(html, **kwargs):
        captured["html"] = html
        captured.update(kwargs)
        return [object()]

    import src.jobs.adapters.plugins.static._rendered_cards as _rc

    monkeypatch.setattr(_rc, "extract_rendered_card_jobs", _spy)
    ctx = _make_runner_ctx(_forbid_render)
    runner = StaticFetchRunner(ctx)

    html = runner._try_playwright_fallback(
        PS_FLIP_SHAPE,
        "https://careers.playstation.com/en-us/jobs/",
        5,
        "js_shell",
        True,
    )

    assert captured["html"] == PS_FLIP_SHAPE
    assert captured["page_url"] == "https://careers.playstation.com/en-us/jobs/"
    assert captured["company"] == ctx.company
    assert captured["source_id"] == clean_text(ctx.source.get("id"))
    assert captured["allow_any_anchor"] is True
    assert html == PS_FLIP_SHAPE
