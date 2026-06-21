"""Tests for jobs fetcher providers fixture parsing."""

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pytest

from src import jobs_fetcher as jf
from tests.helpers.job_fixtures import _fixture


@dataclass
class _FixtureParseCase:
    name: str
    parser: Callable[..., list[dict[str, Any]]]
    fixture_name: str
    loader: Callable[[str], Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    expected_len: int
    at_least: bool = False
    extra_check: Callable[[list[dict[str, Any]]], None] = lambda rows: None


def _assert_gamesindustry(rows: list[dict[str, Any]]) -> None:
    assert rows[0]["title"] == "Senior Quality Analyst"
    assert rows[0]["company"] == "Sharkmob"
    assert rows[0]["sourceJobId"] == "43821"
    assert rows[0]["jobLink"].startswith("https://jobs.gamesindustry.biz/job/")
    titles = {row["title"] for row in rows}
    assert "Read more" not in titles
    assert "Programming (6)" not in titles


def _assert_greenhouse(rows: list[dict[str, Any]]) -> None:
    assert all(row["sourceJobId"].startswith("greenhouse:guerrilla-games:") for row in rows)
    assert rows[0]["company"] == "Guerrilla Games"
    assert rows[0]["country"] == "NL"


def _assert_lever(rows: list[dict[str, Any]]) -> None:
    assert rows[0]["title"] == "Technical Artist"
    assert rows[0]["country"] == "NL"


def _assert_workable(rows: list[dict[str, Any]]) -> None:
    assert rows[0]["workType"] == "Remote"


def _assert_breezy(rows: list[dict[str, Any]]) -> None:
    assert all(row["company"] == "YallaPlay" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def _assert_jazzhr(rows: list[dict[str, Any]]) -> None:
    assert all(row["company"] == "Lost Boys Interactive" for row in rows)
    assert any(row["contractType"] == "Full Time" for row in rows)


def _assert_recruitee(rows: list[dict[str, Any]]) -> None:
    assert all(row["company"] == "CrazyGames" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def _assert_pinpoint(rows: list[dict[str, Any]]) -> None:
    assert all(row["company"] == "Gameplay Galaxy" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def _assert_personio(rows: list[dict[str, Any]]) -> None:
    assert any(row["title"] == "Environment Artist" for row in rows)


FIXTURE_PARSE_CASES = [
    pytest.param(
        _FixtureParseCase(
            name="gamesindustry",
            parser=jf.parse_gamesindustry_html,
            fixture_name="gamesindustry_jobs.html",
            loader=lambda text: text,
            args=("https://jobs.gamesindustry.biz",),
            kwargs={},
            expected_len=2,
            extra_check=_assert_gamesindustry,
        ),
        id="gamesindustry",
    ),
    pytest.param(
        _FixtureParseCase(
            name="greenhouse",
            parser=jf.parse_greenhouse_jobs_payload,
            fixture_name="greenhouse_guerrilla_jobs.json",
            loader=json.loads,
            args=("guerrilla-games",),
            kwargs={},
            expected_len=2,
            extra_check=_assert_greenhouse,
        ),
        id="greenhouse",
    ),
    pytest.param(
        _FixtureParseCase(
            name="lever",
            parser=jf.parse_lever_jobs_payload,
            fixture_name="lever_jobs.json",
            loader=json.loads,
            args=("sandboxvr",),
            kwargs={"fallback_company": "Sandbox VR"},
            expected_len=1,
            extra_check=_assert_lever,
        ),
        id="lever",
    ),
    pytest.param(
        _FixtureParseCase(
            name="workable",
            parser=jf.parse_workable_jobs_payload,
            fixture_name="workable_jobs.json",
            loader=json.loads,
            args=("hutch",),
            kwargs={"fallback_company": "Hutch"},
            expected_len=1,
            extra_check=_assert_workable,
        ),
        id="workable",
    ),
    pytest.param(
        _FixtureParseCase(
            name="breezy",
            parser=jf.parse_breezy_jobs_html,
            fixture_name="breezy_jobs.html",
            loader=lambda text: text,
            args=("https://yallaplay.breezy.hr/", "YallaPlay"),
            kwargs={},
            expected_len=2,
            extra_check=_assert_breezy,
        ),
        id="breezy",
    ),
    pytest.param(
        _FixtureParseCase(
            name="jazzhr",
            parser=jf.parse_jazzhr_jobs_html,
            fixture_name="jazzhr_jobs.html",
            loader=lambda text: text,
            args=("https://lostboysinteractive.applytojob.com/apply", "Lost Boys Interactive"),
            kwargs={},
            expected_len=2,
            extra_check=_assert_jazzhr,
        ),
        id="jazzhr",
    ),
    pytest.param(
        _FixtureParseCase(
            name="recruitee",
            parser=jf.parse_recruitee_jobs_payload,
            fixture_name="recruitee_jobs.json",
            loader=json.loads,
            args=("jobs.crazygames.com",),
            kwargs={"fallback_company": "CrazyGames"},
            expected_len=2,
            extra_check=_assert_recruitee,
        ),
        id="recruitee",
    ),
    pytest.param(
        _FixtureParseCase(
            name="pinpoint",
            parser=jf.parse_pinpoint_jobs_payload,
            fixture_name="pinpoint_jobs.json",
            loader=json.loads,
            args=("gameplaygalaxy",),
            kwargs={"fallback_company": "Gameplay Galaxy"},
            expected_len=2,
            extra_check=_assert_pinpoint,
        ),
        id="pinpoint",
    ),
    pytest.param(
        _FixtureParseCase(
            name="personio",
            parser=jf.parse_personio_feed_xml,
            fixture_name="personio_feed.xml",
            loader=lambda text: text,
            args=(),
            kwargs={"source_name": "InnoGames"},
            expected_len=1,
            at_least=True,
            extra_check=_assert_personio,
        ),
        id="personio",
    ),
]


@pytest.mark.parametrize("case", FIXTURE_PARSE_CASES, ids=lambda case: case.name)
def test_parse_fixture_provider_payloads(case: _FixtureParseCase) -> None:
    loaded = case.loader(_fixture(case.fixture_name))
    rows = case.parser(loaded, *case.args, **case.kwargs)
    if case.at_least:
        assert len(rows) >= case.expected_len
    else:
        assert len(rows) == case.expected_len
    case.extra_check(rows)
