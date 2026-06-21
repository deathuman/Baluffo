"""Tests for jobs fetcher pipeline incremental behavior."""

from pathlib import Path
from unittest import mock

from src import jobs_fetcher as jf
from tests.helpers.temp_paths import workspace_tmpdir


def test_should_skip_source_by_ttl_honors_recent_success_and_failure_state() -> None:
    now = jf.now_iso()
    rows = {"source_a": {"lastSuccessAt": now, "consecutiveFailures": 0}}
    assert jf.should_skip_source_by_ttl("source_a", rows, ttl_minutes=360)

    rows["source_a"]["consecutiveFailures"] = 2
    assert not jf.should_skip_source_by_ttl("source_a", rows, ttl_minutes=360)


def test_should_skip_source_by_cadence_uses_hot_and_cold_windows() -> None:
    now = jf.datetime.now(jf.timezone.utc)
    rows = {
        "hot_source": {
            "lastSuccessAt": (now - jf.timedelta(minutes=10)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(minutes=30)).isoformat(),
            "consecutiveFailures": 0,
        },
        "cold_source": {
            "lastSuccessAt": (now - jf.timedelta(minutes=20)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(days=2)).isoformat(),
            "consecutiveFailures": 0,
        },
    }
    assert jf.should_skip_source_by_cadence("hot_source", rows, hot_minutes=15, cold_minutes=60)
    assert jf.should_skip_source_by_cadence("cold_source", rows, hot_minutes=15, cold_minutes=60)

    rows["hot_source"]["lastSuccessAt"] = (now - jf.timedelta(minutes=20)).isoformat()
    rows["cold_source"]["lastSuccessAt"] = (now - jf.timedelta(minutes=70)).isoformat()
    assert not jf.should_skip_source_by_cadence("hot_source", rows, hot_minutes=15, cold_minutes=60)
    assert not jf.should_skip_source_by_cadence(
        "cold_source", rows, hot_minutes=15, cold_minutes=60
    )


def test_get_incremental_cache_decision_prefers_skip_and_listing_modes() -> None:
    import src.jobs.state_incremental as state_pkg

    now = jf.datetime.now(jf.timezone.utc)
    rows = {
        "provider_source": {
            "lastAdapter": "greenhouse",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=10)).isoformat(),
            "lastChangedAt": (now - jf.timedelta(minutes=20)).isoformat(),
            "lastKeptCount": 3,
        },
        "static_source::example": {
            "lastAdapter": "static",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=20)).isoformat(),
            "lastKeptCount": 2,
            "lastListingFingerprint": "abc123",
        },
    }
    provider_decision = state_pkg.get_incremental_cache_decision(
        "provider_source", rows, adapter="greenhouse"
    )
    static_decision = state_pkg.get_incremental_cache_decision(
        "static_source::example", rows, adapter="static"
    )
    assert provider_decision["cacheDecision"] == "skip_fresh"
    assert static_decision["cacheDecision"] == "listing_only"


def test_get_incremental_cache_decision_treats_future_next_eligible_after_run_as_skip_fresh() -> (
    None
):
    import src.jobs.state_incremental as state_pkg

    now = jf.datetime.now(jf.timezone.utc)
    rows = {
        "provider_board": {
            "lastAdapter": "lever",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=1)).isoformat(),
            "lastKeptCount": 5,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=30)).isoformat(),
            "cacheDecision": "run_now",
            "cacheDecisionReason": "no_cache_state",
        }
    }
    decision = state_pkg.get_incremental_cache_decision("provider_board", rows, adapter="lever")
    assert decision["cacheDecision"] == "skip_fresh"
    assert decision["cacheDecisionReason"] == "within_freshness_window"


def test_run_pipeline_incremental_second_run_skips_fresh_source_and_preserves_output() -> None:
    calls = {"count": 0}

    def ok_loader(**_: object):
        calls["count"] += 1
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Incremental Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/incremental/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "incremental-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-incremental") as tmp:
        out = Path(tmp)
        first = jf.run_pipeline(
            output_dir=out, source_loaders=[("incremental_source", ok_loader)], show_progress=False
        )
        second = jf.run_pipeline(
            output_dir=out, source_loaders=[("incremental_source", ok_loader)], show_progress=False
        )
        assert calls["count"] == 1
        assert int(first["summary"].get("outputCount") or 0) == 1
        assert int(second["summary"].get("outputCount") or 0) == 1
        excluded = [
            row
            for row in (second.get("sourceFamilies") or [])
            if row.get("name") == "incremental_source"
        ]
        assert len(excluded) == 1
        assert str(excluded[0].get("status") or "") == "excluded"
        assert str(excluded[0].get("cacheDecision") or "") == "skip_fresh"
        assert "cache_" in str(excluded[0].get("exclusionReason") or "")


def test_run_pipeline_force_refresh_all_bypasses_incremental_skip() -> None:
    calls = {"count": 0}

    def ok_loader(**_: object):
        calls["count"] += 1
        return [
            {
                "title": "Engine Programmer",
                "company": "Refresh Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/refresh/engine-programmer",
                "sector": "Game",
                "sourceJobId": "refresh-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-force-refresh") as tmp:
        out = Path(tmp)
        jf.run_pipeline(
            output_dir=out, source_loaders=[("refresh_source", ok_loader)], show_progress=False
        )
        jf.run_pipeline(
            output_dir=out,
            source_loaders=[("refresh_source", ok_loader)],
            show_progress=False,
            force_refresh_all=True,
        )
        assert calls["count"] == 2


def test_run_pipeline_force_refresh_all_without_seed_env_drops_existing_output() -> None:
    calls = {"count": 0}

    def loader(**_: object):
        calls["count"] += 1
        if calls["count"] == 1:
            return [
                {
                    "title": "Engine Programmer",
                    "company": "Refresh Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/refresh/engine-programmer",
                    "sector": "Game",
                    "sourceJobId": "refresh-1",
                    "postedAt": "2026-03-01",
                }
            ]
        return []

    with workspace_tmpdir("jobs-fetcher-force-refresh-no-seed") as tmp:
        out = Path(tmp)
        first = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("refresh_source", loader)],
            show_progress=False,
            preserve_previous_on_empty=False,
        )
        second = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("refresh_source", loader)],
            show_progress=False,
            preserve_previous_on_empty=False,
            force_refresh_all=True,
        )
        assert int(first["summary"].get("outputCount") or 0) == 1
        assert int(second["summary"].get("outputCount") or 0) == 0
        assert bool((second.get("runtime") or {}).get("seedFromExistingOutput")) is False


def test_run_pipeline_force_refresh_all_can_seed_existing_output_via_env() -> None:
    calls = {"count": 0}

    def loader(**_: object):
        calls["count"] += 1
        if calls["count"] == 1:
            return [
                {
                    "title": "Engine Programmer",
                    "company": "Refresh Studio",
                    "city": "Remote",
                    "country": "Remote",
                    "workType": "Remote",
                    "contractType": "Full-time",
                    "jobLink": "https://example.com/refresh/engine-programmer",
                    "sector": "Game",
                    "sourceJobId": "refresh-1",
                    "postedAt": "2026-03-01",
                }
            ]
        return []

    with workspace_tmpdir("jobs-fetcher-force-refresh-seeded") as tmp:
        out = Path(tmp)
        first = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("refresh_source", loader)],
            show_progress=False,
            preserve_previous_on_empty=False,
        )
        with mock.patch.dict(
            "os.environ", {"BALUFFO_FETCH_SEED_EXISTING_OUTPUT": "1"}, clear=False
        ):
            second = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("refresh_source", loader)],
                show_progress=False,
                preserve_previous_on_empty=False,
                force_refresh_all=True,
            )
        assert int(first["summary"].get("outputCount") or 0) == 1
        assert int(second["summary"].get("outputCount") or 0) == 1
        assert int(second["summary"].get("lifecycleLikelyRemovedCount") or 0) == 0


def test_apply_incremental_cache_exclusions_keeps_provider_family_loader_for_board_level_refresh() -> (
    None
):
    import src.jobs.state_incremental as state_pkg
    from src.jobs import pipeline_loader_selection as selection_pkg

    now = jf.datetime.now(jf.timezone.utc)
    future = (now + jf.timedelta(minutes=10)).isoformat()
    selected = [
        ("greenhouse_boards", lambda **_: []),
        ("incremental_source", lambda **_: []),
    ]
    source_state_rows = {
        "greenhouse_boards": {
            "lastAdapter": "greenhouse",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 2,
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
        "incremental_source": {
            "lastAdapter": "custom",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 1,
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
    }
    filtered, skipped = selection_pkg.apply_incremental_cache_exclusions(
        selected,
        incremental_cache_enabled=True,
        force_refresh_all=False,
        source_state_rows=source_state_rows,
        get_incremental_cache_decision=state_pkg.get_incremental_cache_decision,
        build_excluded_source_report=lambda name, reason: {"name": name, "exclusionReason": reason},
        source_report_meta={
            "greenhouse_boards": {"adapter": "greenhouse"},
            "incremental_source": {"adapter": "custom"},
        },
    )
    assert [name for name, _ in filtered] == ["greenhouse_boards"]
    assert [row["name"] for row in skipped] == ["incremental_source"]
