"""Tests for jobs fetcher providers plugin traversal behavior."""

from unittest import mock


def test_hrmos_plugin_extracts_listing_rows_without_detail_fetch() -> None:
    from src.jobs.adapters.plugins.static import hrmos

    html = """
        <div>
          <a href="/pages/cygames/jobs/0001">
            <h2>Gameplay Programmer</h2>
            <span>Tokyo, Japan</span>
            <span>Full-time</span>
          </a>
          <a href="/pages/cygames/jobs/0002">
            <h2>Technical Artist</h2>
            <span>Osaka, Japan</span>
            <span>Contract</span>
          </a>
        </div>
        """

    rows = hrmos.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=["https://hrmos.co/pages/cygames/jobs"],
        source_row={"id": "cygames", "name": "Cygames"},
    )

    assert len(rows) == 2
    assert rows[0]["jobLink"] == "https://hrmos.co/pages/cygames/jobs/0001"
    assert rows[0]["title"] == "Gameplay Programmer"
    assert rows[0]["city"] == "Tokyo"
    assert rows[0]["country"] == "Japan"


def test_hrmos_plugin_does_not_emit_full_prose_blob_as_location() -> None:
    from src.jobs.adapters.plugins.static import hrmos

    html = """
        <div>
          <a href="/pages/gamefreak/jobs/10-4">
            <h2>キャリア登録</h2>
            <span>キャリア登録 「キャリア登録」とは？ 当社に興味・関心を持たれた方にご自身のキャリア（職務経歴）を簡易登録いただくことで、適したポジションがある場合、人事担当者から個別にご案内させていただく仕組みです。</span>
            <span>正社員</span>
          </a>
        </div>
        """

    rows = hrmos.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=["https://hrmos.co/pages/gamefreak/jobs?jobtype=full"],
        source_row={"id": "gamefreak", "name": "GAME FREAK inc."},
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "キャリア登録"
    assert rows[0]["city"] == ""


def test_riot_plugin_extracts_listing_rows_without_detail_fetch() -> None:
    from src.jobs.adapters.plugins.static import riot

    html = """
        <div>
          <a href="/en/j/7449593">
            <span>Senior Software Engineer</span>
            <span>Engineering</span>
            <span>Dublin, Ireland</span>
          </a>
        </div>
        """

    rows = riot.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=["https://www.riotgames.com/en/work-with-us/jobs"],
        source_row={"id": "riot", "name": "Riot Games"},
    )

    assert len(rows) == 1
    assert rows[0]["jobLink"] == "https://www.riotgames.com/en/j/7449593"
    assert rows[0]["title"] == "Senior Software Engineer"
    assert rows[0]["city"] == "Dublin"
    assert rows[0]["country"] == "Ireland"


def test_lionbridge_plugin_splits_city_region_country_listing_rows() -> None:
    from src.jobs.adapters.plugins.static import lionbridge

    html = """
        <table>
          <tr>
            <td><a href="/jobs/test-lead">Test Lead</a></td>
            <td>Mexico City, CMDX, Mexico</td>
            <td>Onsite</td>
          </tr>
        </table>
        """

    rows = lionbridge.run(
        fetch_text=lambda _url, _timeout: html,
        timeout_s=10,
        retries=0,
        backoff_s=0.0,
        pages=["https://careers.lionbridge.com/jobs/search"],
        source_row={"id": "lionbridge", "name": "Lionbridge Games"},
    )

    assert len(rows) == 1
    assert rows[0]["title"] == "Test Lead"
    assert rows[0]["city"] == "Mexico City"
    assert rows[0]["country"] == "Mexico"


def test_choose_detail_traversal_mode_static_detail_policy_cases() -> None:
    from src.jobs.adapters.static_detail_heuristics import choose_detail_traversal_mode
    from src.jobs.adapters.static_runtime_support import build_static_source_runtime_config

    climax_state_rows = {
        "static_source::climax": {
            "lastDetailPagesVisited": 42,
            "lastKeptCount": 1,
            "lastDurationMs": 52000,
            "lastDetailYieldPct": 2,
        }
    }
    cases = [
        (
            "verified-host-listing-only",
            "https://hrmos.co/pages/cygames/jobs",
            None,
            {"detail_fetch_required": False},
            {"detailFetchRequired": False},
            10,
            10,
            "static_source::cygames",
            {},
            None,
            "listing_only",
        ),
        (
            "uncapped-probable-detail-links-full-detail",
            "https://hrmos.co/pages/cygames/jobs",
            {"BALUFFO_UNCAPPED_DEEP_STATIC": "1"},
            {"detail_fetch_required": False},
            {"detailFetchRequired": False},
            10,
            10,
            "static_source::cygames",
            {},
            3,
            "full_detail",
        ),
        (
            "uncapped-without-probable-detail-links-listing-only",
            "https://hrmos.co/pages/cygames/jobs",
            {"BALUFFO_UNCAPPED_DEEP_STATIC": "1"},
            {"detail_fetch_required": False},
            {"detailFetchRequired": False},
            10,
            10,
            "static_source::cygames",
            {},
            0,
            "listing_only",
        ),
        (
            "regular-runtime-keeps-capped-detail",
            "https://careers.climaxstudios.com/jobs",
            None,
            {},
            {},
            0,
            28,
            "static_source::climax",
            climax_state_rows,
            None,
            "capped_detail",
        ),
        (
            "uncapped-zero-caps-promotes-full-detail",
            "https://careers.climaxstudios.com/jobs",
            {
                "BALUFFO_UNCAPPED_DEEP_STATIC": "1",
                "BALUFFO_STATIC_LOW_YIELD_DETAIL_CAP": "0",
                "BALUFFO_STATIC_VERY_LOW_YIELD_DETAIL_CAP": "0",
            },
            {},
            {},
            0,
            28,
            "static_source::climax",
            climax_state_rows,
            None,
            "full_detail",
        ),
    ]

    for (
        case_id,
        url,
        env,
        profile,
        plugin_meta,
        listing_jobs_found,
        discovered_links,
        source_key,
        source_state_rows,
        probable_detail_candidates,
        expected,
    ) in cases:
        if env:
            with mock.patch.dict("os.environ", env, clear=False):
                runtime = build_static_source_runtime_config(4)
        else:
            runtime = build_static_source_runtime_config(4)
        kwargs = {
            "runtime_config": runtime,
            "profile": profile,
            "plugin_meta": plugin_meta,
            "listing_jobs_found": listing_jobs_found,
            "discovered_links": discovered_links,
            "source_key": source_key,
            "source_state_rows": source_state_rows,
        }
        if probable_detail_candidates is not None:
            kwargs["probable_detail_candidates"] = probable_detail_candidates

        assert choose_detail_traversal_mode(url, **kwargs) == expected, case_id
