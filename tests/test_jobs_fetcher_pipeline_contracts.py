"""Tests for jobs fetcher pipeline contracts."""

import json
from pathlib import Path
from unittest import mock

import pytest

from src import jobs_fetcher as jf
from src.jobs.adapters import static as static_adapter
from src.shared.json_io import read_json
from tests.helpers.job_fixtures import _fixture, _fixture_json
from tests.helpers.temp_paths import workspace_tmpdir


def test_pipeline_output_contract_matches_frontend() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Technical Artist",
                "company": "Orion Labs",
                "city": "Remote",
                "country": "Remote",
                "workType": "remote",
                "contractType": "contract",
                "jobLink": "https://example.com/jobs/ta",
                "sector": "gaming",
                "sourceJobId": "ta-1",
                "postedAt": "2026-02-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        jf.run_pipeline(output_dir=Path(tmp), source_loaders=[("ok", ok_loader)])
        rows = read_json(Path(tmp) / "jobs-unified.json", [])
        assert len(rows) == 1
        row = rows[0]
        for field in jf.REQUIRED_FIELDS:
            assert field in row
        for field in jf.OPTIONAL_FIELDS:
            assert field in row
        assert (row["workType"], isinstance(row["focusScore"], int)) == ("Remote", True)


def test_pipeline_default_source_loader_contract_excludes_wellfound_and_keeps_core_families() -> (
    None
):
    loader_names = [name for name, _ in jf.default_source_loaders()]

    assert "wellfound" not in loader_names
    assert "google_sheets" in loader_names
    assert "google_sheets_1er2oaxo" in loader_names
    assert "google_sheets_1mvqhxat" in loader_names
    assert "remote_ok" in loader_names
    assert "gamesindustry" in loader_names
    assert "gamejobs" in loader_names
    assert "workwithindies" in loader_names
    assert "greenhouse_boards" in loader_names
    assert "teamtailor_sources" in loader_names
    assert "lever_sources" in loader_names
    assert "smartrecruiters_sources" in loader_names
    assert "workable_sources" in loader_names
    assert "recruitee_sources" in loader_names
    assert "pinpoint_sources" in loader_names
    assert "ashby_sources" in loader_names
    assert "breezy_sources" in loader_names
    assert "jazzhr_sources" in loader_names
    assert "oracle_hcm_sources" in loader_names
    assert "personio_sources" in loader_names
    assert "scrapy_static_sources" in loader_names
    assert any(name.startswith("static_source::") for name in loader_names)


@pytest.mark.slow
@pytest.mark.integration
def test_pipeline_default_source_mix_smoke_excludes_wellfound_and_includes_guerrilla() -> None:
    google_csv = _fixture("google_sheets.csv")
    remote_json = _fixture_json("remoteok.json")
    gamesindustry_html = _fixture("gamesindustry_jobs.html")
    gamejobs_html = _fixture("gamejobs.html")
    workwithindies_html = _fixture("workwithindies.html")
    eightbitplay_html = _fixture("8bitplay_jobs.html")
    gracklehq_html = _fixture("gracklehq_jobs.html")
    greenhouse_json = _fixture_json("greenhouse_guerrilla_jobs.json")
    greenhouse_playstation_json = _fixture_json("greenhouse_playstation_jobs.json")
    teamtailor_listing = _fixture("teamtailor_listing.html")
    littlechicken_listing = _fixture("littlechicken_jobs_page.html")
    littlechicken_detail = _fixture("littlechicken_job_detail.html")
    lever_json = _fixture_json("lever_jobs.json")
    smart_json = _fixture_json("smartrecruiters_jobs.json")
    workable_json = _fixture_json("workable_jobs.json")
    ashby_html = _fixture("ashby_jobs.html")
    recruitee_json = _fixture_json("recruitee_jobs.json")
    pinpoint_json = _fixture_json("pinpoint_jobs.json")
    breezy_html = _fixture("breezy_jobs.html")
    jazzhr_html = _fixture("jazzhr_jobs.html")
    personio_xml = _fixture("personio_feed.xml")
    littlechicken_source = {
        "name": "Little Chicken (Manual Website)",
        "studio": "Little Chicken",
        "adapter": "static",
        "company": "Little Chicken",
        "pages": ["https://www.littlechicken.nl/jobs/"],
        "enabledByDefault": True,
        "id": "static:listing_url:https://www.littlechicken.nl/jobs/",
    }
    littlechicken_loader_name = static_adapter.static_source_name_for_registry_row(
        littlechicken_source
    )
    paradox_links = jf.parse_teamtailor_listing_links(
        teamtailor_listing,
        base_url="https://career.paradoxplaza.com/jobs",
    )

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://www.littlechicken.nl/jobs/":
            return littlechicken_listing
        if "littlechicken.nl/job/" in url:
            return littlechicken_detail
        if "jobs.gamesindustry.biz" in url:
            return gamesindustry_html
        raise RuntimeError(f"Unhandled URL in fake fetch: {url}")

    class _FakeRedirectResolver:
        def resolve(self, url: str) -> str:
            return url

        def seed_cache(self, cache: dict[str, str]) -> None:
            _ = cache

        def close(self) -> None:
            return None

    with (
        workspace_tmpdir("jobs-fetcher") as tmp,
        mock.patch.object(jf, "build_redirect_resolver", return_value=_FakeRedirectResolver()),
        mock.patch.object(
            jf,
            "default_source_loaders",
            return_value=[
                ("google_sheets", lambda **_: jf.parse_google_sheets_csv(google_csv)),
                ("google_sheets_1er2oaxo", lambda **_: jf.parse_google_sheets_csv(google_csv)),
                ("google_sheets_1mvqhxat", lambda **_: jf.parse_google_sheets_csv(google_csv)),
                ("remote_ok", lambda **_: jf.parse_remote_ok_payload(remote_json)),
                (
                    "gamesindustry",
                    lambda **_: jf.parse_gamesindustry_html(
                        gamesindustry_html,
                        base_url="https://jobs.gamesindustry.biz/jobs/",
                    ),
                ),
                (
                    "gamejobs",
                    lambda **_: jf.parse_gamejobs_html(
                        gamejobs_html,
                        base_url="https://gamejobs.co/",
                    ),
                ),
                (
                    "workwithindies",
                    lambda **_: jf.parse_workwithindies_html(
                        workwithindies_html,
                        base_url="https://www.workwithindies.com/",
                    ),
                ),
                (
                    "8bitplay",
                    lambda **_: jf.parse_8bitplay_html(
                        eightbitplay_html,
                        base_url="https://8bitplay.com/jobs/",
                    ),
                ),
                (
                    "gracklehq",
                    lambda **_: jf.parse_gracklehq_html(
                        gracklehq_html,
                        base_url="https://gracklehq.com/jobs",
                    ),
                ),
                (
                    "greenhouse_boards",
                    lambda **_: (
                        jf.parse_greenhouse_jobs_payload(
                            greenhouse_json,
                            "guerrilla-games",
                            fallback_company="Guerrilla Games",
                        )
                        + jf.parse_greenhouse_jobs_payload(
                            greenhouse_playstation_json,
                            "sonyinteractiveentertainmentglobal",
                            fallback_company="PlayStation Global",
                        )
                    ),
                ),
                (
                    "teamtailor_sources",
                    lambda **_: [
                        {
                            "sourceJobId": "teamtailor:paradox:1",
                            "title": "Senior Rendering Programmer",
                            "company": "Paradox Interactive",
                            "city": "Stockholm",
                            "country": "SE",
                            "workType": "Hybrid",
                            "contractType": "Full Time",
                            "jobLink": paradox_links[0],
                            "sector": "Game",
                            "postedAt": "",
                        }
                    ],
                ),
                (
                    "lever_sources",
                    lambda **_: jf.parse_lever_jobs_payload(
                        lever_json,
                        "pixelforge",
                        fallback_company="Pixel Forge",
                    ),
                ),
                (
                    "smartrecruiters_sources",
                    lambda **_: jf.parse_smartrecruiters_jobs_payload(
                        smart_json,
                        "ubisoft2",
                        fallback_company="Ubisoft",
                    ),
                ),
                (
                    "workable_sources",
                    lambda **_: jf.parse_workable_jobs_payload(
                        workable_json,
                        "pixeldominion",
                        fallback_company="Pixel Dominion",
                    ),
                ),
                (
                    "recruitee_sources",
                    lambda **_: jf.parse_recruitee_jobs_payload(
                        recruitee_json,
                        "jobs.crazygames.com",
                        fallback_company="CrazyGames",
                    ),
                ),
                (
                    "pinpoint_sources",
                    lambda **_: jf.parse_pinpoint_jobs_payload(
                        pinpoint_json,
                        "gameplaygalaxy",
                        fallback_company="Gameplay Galaxy",
                    ),
                ),
                (
                    "ashby_sources",
                    lambda **_: jf.parse_ashby_jobs_from_html(
                        ashby_html,
                        "https://jobs.ashbyhq.com/jagex/jobs",
                        "Jagex",
                    ),
                ),
                (
                    "breezy_sources",
                    lambda **_: jf.parse_breezy_jobs_html(
                        breezy_html,
                        "https://yallaplay.breezy.hr/",
                        "YallaPlay",
                    ),
                ),
                (
                    "jazzhr_sources",
                    lambda **_: jf.parse_jazzhr_jobs_html(
                        jazzhr_html,
                        "https://lostboysinteractive.applytojob.com/apply",
                        "Lost Boys Interactive",
                    ),
                ),
                (
                    "personio_sources",
                    lambda **_: jf.parse_personio_feed_xml(
                        personio_xml,
                        source_name="InnoGames",
                    ),
                ),
                (
                    littlechicken_loader_name,
                    lambda **kwargs: static_adapter.run_static_source_entry_source(
                        source_row=littlechicken_source,
                        diagnostics_name=littlechicken_loader_name,
                        **kwargs,
                    ),
                ),
            ],
        ),
    ):
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )

        sources = {row["name"]: row for row in report["sources"]}
        source_families = {row["name"]: row for row in (report.get("sourceFamilies") or [])}
        assert sources["google_sheets"]["status"] == "ok"
        assert sources["google_sheets_1er2oaxo"]["status"] == "ok"
        assert sources["google_sheets_1mvqhxat"]["status"] == "ok"
        assert sources["remote_ok"]["status"] == "ok"
        assert sources["gamesindustry"]["status"] == "ok"
        assert sources["gamejobs"]["status"] == "ok"
        assert sources["workwithindies"]["status"] == "ok"
        assert sources["8bitplay"]["status"] == "ok"
        assert sources["gracklehq"]["status"] == "ok"
        assert sources["greenhouse_boards"]["status"] == "ok"
        assert sources["teamtailor_sources"]["status"] == "ok"
        assert sources["lever_sources"]["status"] == "ok"
        assert sources["smartrecruiters_sources"]["status"] == "ok"
        assert sources["workable_sources"]["status"] == "ok"
        assert sources["recruitee_sources"]["status"] == "ok"
        assert sources["pinpoint_sources"]["status"] == "ok"
        assert sources["ashby_sources"]["status"] == "ok"
        assert sources["breezy_sources"]["status"] == "ok"
        assert sources["jazzhr_sources"]["status"] == "ok"
        assert sources["personio_sources"]["status"] == "ok"
        static_rows = [
            row for row in report["sources"] if str(row.get("adapter") or "").lower() == "static"
        ]
        assert static_rows
        assert any(str(row.get("status") or "").lower() == "ok" for row in static_rows)
        assert source_families["wellfound"]["status"] == "excluded"
        assert "disabled_by_default" in source_families["wellfound"]["error"]
        assert sources["greenhouse_boards"]["adapter"] == "greenhouse"
        assert sources["teamtailor_sources"]["adapter"] == "teamtailor"
        assert sources["lever_sources"]["adapter"] == "lever"
        assert sources["smartrecruiters_sources"]["adapter"] == "smartrecruiters"
        assert sources["workable_sources"]["adapter"] == "workable"
        assert sources["recruitee_sources"]["adapter"] == "recruitee"
        assert sources["pinpoint_sources"]["adapter"] == "pinpoint"
        assert sources["8bitplay"]["adapter"] == "html"
        assert sources["gracklehq"]["adapter"] == "html"
        assert sources["ashby_sources"]["adapter"] == "ashby"
        assert sources["breezy_sources"]["adapter"] == "breezy"
        assert sources["jazzhr_sources"]["adapter"] == "jazzhr"
        assert sources["personio_sources"]["adapter"] == "personio"
        assert "failedSources" in report["summary"]
        assert report["summary"]["excludedSources"] == 0
        assert "targetRoleCount" in report["summary"]
        assert "netherlandsCount" in report["summary"]
        assert "remoteCount" in report["summary"]
        assert "rawFetchedCount" in report["summary"]
        assert "uniqueOutputCount" in report["summary"]
        assert "sourceBundleCollisions" in report["summary"]

        rows = read_json(Path(tmp) / "jobs-unified.json", [])
        assert any("guerrilla" in row.get("company", "").lower() for row in rows)
        assert any("playstation" in row.get("company", "").lower() for row in rows)
        assert any("paradox" in row.get("company", "").lower() for row in rows)
        assert any("pixel forge" in row.get("company", "").lower() for row in rows)
        assert any("moonshot games" in row.get("company", "").lower() for row in rows)
        assert any("pixel dominion" in row.get("company", "").lower() for row in rows)
        assert any("ubisoft" in row.get("company", "").lower() for row in rows)
        assert any("crazygames" in row.get("company", "").lower() for row in rows)
        assert any("gameplay galaxy" in row.get("company", "").lower() for row in rows)
        assert any("little chicken" in row.get("company", "").lower() for row in rows)
        assert all("focusScore" in row for row in rows)
        assert all("sourceBundleCount" in row for row in rows)
        assert all("sourceBundle" in row for row in rows)
        all_errors = " ".join(row.get("error", "") for row in report["sources"])
        assert "403" not in all_errors


def test_run_pipeline_writes_normalized_report_task_and_source_state_contracts() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Engine Programmer",
                "company": "Contract Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/contract/engine-programmer",
                "sector": "Game",
                "sourceJobId": "contract-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("ok_source", ok_loader)],
            max_workers=2,
            max_per_domain=2,
        )
        assert str(report.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
        runtime = report.get("runtime") or {}
        assert int(runtime.get("maxWorkers") or 0) == 2
        assert int(runtime.get("maxPerDomain") or 0) == 2
        assert str(runtime.get("fetchStrategy") or "") == "auto"
        assert str(runtime.get("fetchClient") or "") in {"urllib", "httpx_async"}
        assert (
            int(runtime.get("adapterHttpConcurrency") or 0) == jf.DEFAULT_ADAPTER_HTTP_CONCURRENCY
        )
        assert (
            int(runtime.get("staticDetailConcurrency") or 0) == jf.DEFAULT_STATIC_DETAIL_CONCURRENCY
        )
        assert (
            int(runtime.get("googleSheetsRedirectConcurrency") or 0)
            == jf.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
        )
        assert int(runtime.get("selectedSourceCount") or 0) == 1
        assert isinstance(runtime.get("slowestSources"), list)
        assert "staticListingTimeoutStops" in runtime
        assert "staticListingBrowserFallbacks" in runtime
        timing = runtime.get("timingSummary") or {}
        assert "medianSourceDurationMs" in timing
        assert "p95SourceDurationMs" in timing
        assert "stageTotalsMs" in timing
        assert "adapterTimings" in timing
        assert "summary" in report
        assert "sources" in report
        assert "sourceFamilies" in report
        assert str(report["sources"][0].get("fetchStrategy") or "") == "auto"
        assert "loss" in report["sources"][0]
        assert "canonicalDropReasons" in (report["sources"][0].get("loss") or {})
        stage_timings = report["sources"][0].get("stageTimingsMs") or {}
        if stage_timings:
            assert "fetchAndParse" in stage_timings
        adapter_timings = timing.get("adapterTimings") or []
        if adapter_timings:
            assert str(adapter_timings[0].get("adapter") or "") == "custom"

        task_payload = json.loads((out / "jobs-fetch-tasks.json").read_text(encoding="utf-8"))
        assert str(task_payload.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
        assert "heartbeatAt" in task_payload
        assert "summary" in task_payload
        assert "tasks" not in task_payload
        assert "workItems" in task_payload
        assert "outputs" in task_payload
        assert str((task_payload.get("outputs") or {}).get("report") or "") == str(
            out / "jobs-fetch-report.json"
        )
        assert str((task_payload.get("workItems") or [])[0].get("status") or "") == "ok"
        assert (
            str(((report.get("runtime") or {}).get("lifecycle") or {}).get("owner") or "")
            == "fetch_report"
        )

        state_payload = read_json(out / "jobs-source-state.json", {})
        assert str(state_payload.get("schemaVersion") or "") == str(jf.SCHEMA_VERSION)
        sources_state = state_payload.get("sources") or {}
        assert "ok_source" in sources_state
        assert int((sources_state["ok_source"]).get("consecutiveFailures") or 0) == 0


def test_run_pipeline_includes_selection_exclusions() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Technical Artist",
                "company": "Incl Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/included",
                "sector": "Game",
                "sourceJobId": "incl-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("included_source", ok_loader)],
            selection_exclusions=[
                {
                    "name": "excluded_source",
                    "status": "excluded",
                    "adapter": "custom",
                    "fetchStrategy": "auto",
                    "studio": "",
                    "fetchedCount": 0,
                    "keptCount": 0,
                    "error": "only_sources_filter",
                    "exclusionReason": "only_sources_filter",
                    "durationMs": 0,
                }
            ],
        )
        excluded_rows = [
            row
            for row in (report.get("sourceFamilies") or [])
            if row.get("name") == "excluded_source"
        ]
        assert len(excluded_rows) == 1
        assert str(excluded_rows[0].get("status") or "") == "excluded"
        assert str(excluded_rows[0].get("exclusionReason") or "") == "only_sources_filter"
