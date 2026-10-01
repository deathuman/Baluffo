"""Focused tests for the WP3 leaf static plugins (Astrid, Immersity, Perfect Garbage)."""

from __future__ import annotations

from typing import Any, cast

import pytest

from src.jobs.adapters.plugins.static import astrid, immersity, perfectgarbage
from tests.helpers.jobs_rows import run_static_plugin as _run_plugin

_PLUGINS = (astrid, immersity, perfectgarbage)


def test_astrid_extracts_all_blocks_with_locations() -> None:
    html = """
    <h3>Engineering</h3>
    <div class="job-listing my-6">
      <h5 class="job-title mb-2"><a href="https://apply.workable.com/j/BB8CA3C8D4">Senior Gameplay Engineer</a></h5>
      <div class="job-location">United Kingdom</div>
    </div>
    <div class="job-listing my-6">
      <h5 class="job-title mb-2"><a href="https://apply.workable.com/j/0730C4C703">Senior UI / UX Designer</a></h5>
      <div class="job-location">Remote</div>
    </div>
    """
    rows = _run_plugin(
        astrid,
        page_url="https://astridentertainment.com/careers",
        html=html,
    )
    assert [r["title"] for r in rows] == ["Senior Gameplay Engineer", "Senior UI / UX Designer"]
    assert [r["jobLink"] for r in rows] == [
        "https://apply.workable.com/j/BB8CA3C8D4",
        "https://apply.workable.com/j/0730C4C703",
    ]
    assert rows[0]["city"] == ""
    assert rows[0]["country"] in ("United Kingdom", "UK", "GB")


def test_immersity_extracts_company_careers_blocks() -> None:
    html = """
    <div role="listitem" class="careers_cms_item w-dyn-item">
      <div class="careers_item">
        <div class="careers_title">
          <h2 class="u-text-style-h4">IT Operations Specialist</h2>
          <div class="u-text-style-h6 u-color-faded">Nashua NH</div>
        </div>
        <a href="/company-careers/it-operations-specialist" class="g_clickable_link w-inline-block">View Job</a>
      </div>
    </div>
    <div role="listitem" class="careers_cms_item w-dyn-item">
      <div class="careers_item">
        <div class="careers_title">
          <h2 class="u-text-style-h4">Senior Machine Learning Engineer</h2>
          <div class="u-text-style-h6 u-color-faded">Remote</div>
        </div>
        <a href="/company-careers/senior-machine-learning-engineer" class="g_clickable_link w-inline-block">View Job</a>
      </div>
    </div>
    """
    rows = _run_plugin(
        immersity,
        page_url="https://immersity.ai/careers",
        html=html,
    )
    assert [r["title"] for r in rows] == [
        "IT Operations Specialist",
        "Senior Machine Learning Engineer",
    ]
    assert [r["jobLink"] for r in rows] == [
        "https://immersity.ai/company-careers/it-operations-specialist",
        "https://immersity.ai/company-careers/senior-machine-learning-engineer",
    ]
    assert rows[0]["city"] == ""
    assert rows[0]["country"] == "Unknown"


def test_perfectgarbage_extracts_all_workwithindies_links_and_strips_hiring_prefix() -> None:
    html = """
    <p>
      <a href="https://www.workwithindies.com/careers/perfect-garbage-senior-programmer">Hiring: Senior Programmer</a>
      <br>
      <a href="https://www.workwithindies.com/careers/perfect-garbage-technical-sound-designer">Hiring: Technical Sound Designer</a>
    </p>
    """
    rows = _run_plugin(
        perfectgarbage,
        page_url="https://www.perfectgarbage.com/careers",
        html=html,
    )
    assert [r["title"] for r in rows] == ["Senior Programmer", "Technical Sound Designer"]
    assert [r["jobLink"] for r in rows] == [
        "https://www.workwithindies.com/careers/perfect-garbage-senior-programmer",
        "https://www.workwithindies.com/careers/perfect-garbage-technical-sound-designer",
    ]


@pytest.mark.parametrize(
    ("plugin", "host"),
    [
        (astrid, "astridentertainment.com"),
        (astrid, "www.astridentertainment.com"),
        (immersity, "immersity.ai"),
        (immersity, "www.immersity.ai"),
        (perfectgarbage, "perfectgarbage.com"),
        (perfectgarbage, "www.perfectgarbage.com"),
    ],
)
def test_wp3_plugin_can_handle_own_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is True


@pytest.mark.parametrize(
    ("plugin", "host"),
    [
        (astrid, "outerdawn.com"),
        (immersity, "astridentertainment.com"),
        (perfectgarbage, "www.outerdawn.com"),
    ],
)
def test_wp3_plugin_rejects_unrelated_hosts(plugin: Any, host: str) -> None:
    ctx = cast(Any, type("Ctx", (), {"source_identity": host})())
    assert plugin.can_handle(ctx) is False
