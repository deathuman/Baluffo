"""Wave C rendered-card coverage: Two Robots (studio leaf) and Gameberry (keka host).

Two Robots is server-rendered with one shared Airtable application form (no
per-role detail URL), so it needs a studio leaf; Gameberry's keka board already
parses with the generic rendered-card extractor once its host is routed there.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, ".")

from src.jobs.adapters.plugins.static import tworobots  # noqa: E402
from src.jobs.adapters.plugins.static._rendered_cards import (  # noqa: E402
    can_handle_rendered_cards,
    extract_rendered_card_jobs,
)
from src.jobs.adapters.plugins.types import AdapterPluginContext  # noqa: E402

FIXTURES = Path("tests/fixtures")
TWO_ROBOTS_URL = "https://trb.tworobots.com/careers"
KEKA_URL = "https://gameberry.keka.com/careers"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _ctx(identity: str) -> AdapterPluginContext:
    return AdapterPluginContext(family="static", adapter_key="static", source_identity=identity)


def test_tworobots_can_handle_matches_careers_host() -> None:
    assert tworobots.can_handle(_ctx("trb.tworobots.com"))
    assert tworobots.can_handle(_ctx("www.trb.tworobots.com"))
    assert not tworobots.can_handle(_ctx("tworobots.com"))
    assert not tworobots.can_handle(_ctx("example.com"))


def test_tworobots_extracts_all_role_cards_with_distinct_identities() -> None:
    html = _fixture("tworobots_careers.html")

    def fetch_text(url: str, timeout_s: int) -> str:
        assert url == TWO_ROBOTS_URL
        return html

    rows = tworobots.run(
        fetch_text=fetch_text,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=[TWO_ROBOTS_URL],
        source_row={
            "id": f"static:listing_url:{TWO_ROBOTS_URL}",
            "name": "Two Robots Studios",
            "studio": "Two Robots Studios",
        },
    )

    assert [row["title"] for row in rows] == [
        "Translator / Localization Specialist",
        "Voice Actors — Multiple Languages",
        "QA Intern",
        "Card Artist",
    ]
    # Every role shares one Airtable apply form, so identities must come from the
    # query-anchored listing link: distinct sourceJobIds on the careers page.
    assert len({row["sourceJobId"] for row in rows}) == 4
    assert all(row["jobLink"].startswith(TWO_ROBOTS_URL) for row in rows)
    assert all("static-role=" in row["jobLink"] for row in rows)
    assert {row["company"] for row in rows} == {"Two Robots Studios"}
    assert {row["adapter"] for row in rows} == {"static"}


def test_rendered_cards_routes_keka_host() -> None:
    assert can_handle_rendered_cards(_ctx("gameberry.keka.com"))
    assert not can_handle_rendered_cards(_ctx("example.com"))


def test_rendered_cards_follows_keka_active_jobs_api() -> None:
    shell = _fixture("gameberry_keka_shell.html")
    payload = '[{"id": 87526, "title": "Game Artist - I", "jobLocations": [{"city": "Bangalore", "country": "India"}]}]'
    seen: list[str] = []

    def fetch_text(url: str, timeout_s: int) -> str:
        del timeout_s
        seen.append(url)
        if url == KEKA_URL:
            return shell
        return payload

    from src.jobs.adapters.plugins.static._rendered_cards import run_rendered_cards_plugin

    rows = run_rendered_cards_plugin(
        fetch_text=fetch_text,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=[KEKA_URL],
        source_row={"id": "gameberry", "name": "Gameberry Labs", "studio": "Gameberry Labs"},
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Game Artist - I"
    assert rows[0]["jobLink"].endswith("/careers/jobdetails/87526")
    assert seen[-1].endswith("/api/embedjobs/default/active/c69cfb2f-b876-4b10-bba3-a2f6411cd7f5")


def test_rendered_cards_follows_keka_embedded_document() -> None:
    shell = _fixture("gameberry_keka_shell.html")
    rendered = _fixture("gameberry_keka_careers.html")
    seen: list[str] = []

    def fetch_text(url: str, timeout_s: int) -> str:
        del timeout_s
        seen.append(url)
        return shell if url == KEKA_URL else rendered

    from src.jobs.adapters.plugins.static._rendered_cards import run_rendered_cards_plugin

    rows = run_rendered_cards_plugin(
        fetch_text=fetch_text,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=[KEKA_URL],
        source_row={"id": "gameberry", "name": "Gameberry Labs", "studio": "Gameberry Labs"},
    )

    assert len(rows) == 12
    assert seen == [
        KEKA_URL,
        "https://gameberry.keka.com/ats/documents/c69cfb2f-b876-4b10-bba3-a2f6411cd7f5/careerportal/13fbe6501ce545e3810697881257d27a.html",
    ]


def test_rendered_cards_extracts_keka_jobdetail_links_with_locations() -> None:
    rows = extract_rendered_card_jobs(
        _fixture("gameberry_keka_careers.html"),
        page_url=KEKA_URL,
        company="Gameberry Labs",
        source_id="gameberry",
        allow_any_anchor=True,
    )

    assert len(rows) == 12
    assert rows[0]["title"] == "Game Artist - I"
    assert rows[0]["city"] == "Bangalore"
    assert rows[0]["jobLink"] == "https://gameberry.keka.com/careers/jobdetails/87526"
    links = {row["jobLink"] for row in rows}
    assert len(links) == 12
    assert all("/careers/jobdetails/" in link for link in links)
    assert {row["company"] for row in rows} == {"Gameberry Labs"}
