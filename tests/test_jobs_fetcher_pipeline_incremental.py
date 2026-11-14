"""Tests for jobs fetcher pipeline incremental behavior."""

from pathlib import Path
from typing import Any, cast
from unittest import mock

from src import jobs_fetcher as jf
from src.bridge.task_launch_api import (
    TaskLaunchApi,
    TaskLaunchDeps,
    TaskLaunchPaths,
    TaskLaunchRuntime,
)
from src.jobs.interfaces import SourceLoader
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import read_json
from src.storage import BaluffoStore, JobRuntimeStore
from src.storage.job_runtime import jobs_feed_content_hash
from tests.helpers.temp_paths import workspace_tmpdir

_REFRESH_JOB: dict[str, object] = {
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


def _once_loader(calls: dict[str, int], job: dict[str, object] = _REFRESH_JOB) -> SourceLoader:
    """A loader that yields ``job`` on its first call and nothing after.

    Two tests below needed byte-identical copies of this; the row and the
    call counter are the only parts that varied, so both are parameters.
    """

    def loader(**_: object) -> list[dict[str, object]]:
        calls["count"] += 1
        return [dict(job)] if calls["count"] == 1 else []

    return cast("SourceLoader", loader)


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
    loader = _once_loader(calls)

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
    loader = _once_loader(calls)

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
    selected: list[tuple[str, SourceLoader]] = [
        ("greenhouse_boards", cast(SourceLoader, lambda **_: [])),
        ("incremental_source", cast(SourceLoader, lambda **_: [])),
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


def _pipeline_feed_row(index: int, *, last_seen: str) -> dict[str, object]:
    return {
        "id": index,
        "title": f"Pipeline Job {index}",
        "company": "Studio A",
        "city": "Amsterdam",
        "country": "Netherlands",
        "workType": "Hybrid",
        "contractType": "Full-time",
        "jobLink": f"https://example.com/pipeline/{index}",
        "sector": "Game",
        "profession": "Engineering",
        "source": "Studio A",
        "sourceJobId": f"pipeline-{index}",
        "status": "active",
        "firstSeenAt": "2026-05-12T10:00:00Z",
        "lastSeenAt": last_seen,
        "availabilityId": f"availability-{index}",
        "availabilityStatus": "available",
        "availabilityCheckedAt": last_seen,
        "availabilityVerifiedAt": last_seen,
        "availabilityEvidence": {
            "kind": "source_present",
            "confidence": "definitive",
            "checkedAt": last_seen,
            "source": "Studio A",
        },
        "sourceBundle": [
            {
                "sourceName": "Studio A",
                "sourceId": "studio-a",
                "sourceJobId": f"pipeline-{index}",
            }
        ],
    }


def _save_json_atomic(path: Path, payload: Any) -> None:
    write_atomic_if_changed(path, payload)


def _pipeline_task_launch_api(
    data_dir: Path,
    *,
    runtime: JobRuntimeStore,
    diagnostics: list[dict[str, Any]],
) -> TaskLaunchApi:
    return TaskLaunchApi(
        runtime=TaskLaunchRuntime(root=data_dir, data_dir=data_dir),
        paths=TaskLaunchPaths(
            discovery_log=data_dir / "source-discovery.log",
            discovery_report=data_dir / "source-discovery-report.json",
            fetcher_log=data_dir / "jobs-fetcher.log",
            task_state=data_dir / "admin-task-state.json",
            jobs_fetch_report=data_dir / "jobs-fetch-report.json",
            jobs_fetch_tasks=data_dir / "jobs-fetch-tasks.json",
            approval_state=data_dir / "source-approval-state.json",
        ),
        deps=TaskLaunchDeps(
            now_iso=jf.now_iso,
            bridge_log=lambda *_args, **_kwargs: None,
            load_json_object=lambda path, default: read_json(path, default),
            save_json_atomic=_save_json_atomic,
            task_state_lock=None,
            default_source_loaders=lambda: [],
            failed_source_names_from_latest_report=lambda _allowed: [],
            safe_int=lambda value, default, floor, ceil: max(
                floor, min(ceil, int(value or default))
            ),
            job_runtime_store=lambda: runtime,
            record_storage_diagnostic=lambda **fields: diagnostics.append(dict(fields)),
        ),
    )


def _close_pipeline_fetch(api: TaskLaunchApi, report: dict[str, Any]) -> bool:
    return api._close_fetch_lifecycle_from_report(  # noqa: SLF001
        run_id=str(report["runId"]),
        normalize_fetch_report_contract=lambda payload: payload,
        load_json_object=lambda path, default: read_json(path, default),
        finish_lifecycle_run=lambda *_args, **_kwargs: {},
        fail_lifecycle_run=lambda *_args, **_kwargs: {},
    )


def test_real_pipeline_unchanged_feed_keeps_the_same_sqlite_generation() -> None:
    first_rows = [_pipeline_feed_row(1, last_seen="2026-05-12T10:00:00Z")]
    calls = {"count": 0}

    with workspace_tmpdir("jobs-fetcher-unchanged-feed") as tmp:
        data_dir = Path(tmp)
        with BaluffoStore(data_dir) as store:
            store.set_authority_mode("jobsFeed", "sqlite", reason="pipeline-test")
            runtime = JobRuntimeStore(store)
            diagnostics: list[dict[str, Any]] = []
            api = _pipeline_task_launch_api(data_dir, runtime=runtime, diagnostics=diagnostics)
            loader_rows = first_rows

            def loader(**_: object) -> list[dict[str, object]]:
                calls["count"] += 1
                return [dict(row) for row in loader_rows]

            first_report = jf.run_pipeline(
                output_dir=data_dir,
                run_id="pipeline_fetch_1",
                source_loaders=[("pipeline_feed_source", loader)],
                show_progress=False,
                force_refresh_all=True,
            )
            assert _close_pipeline_fetch(api, first_report) is True
            generation = runtime.current_generation()
            first_hash = jobs_feed_content_hash(runtime.current_rows())
            first_sqlite_last_seen = runtime.current_rows()[0]["lastSeenAt"]
            assert generation
            assert [item["code"] for item in diagnostics if item["surface"] == "jobsFeed"] == [
                "jobs_feed_sqlite_export_written",
                "jobs_feed_projection_match",
            ]

            diagnostics.clear()
            loader_rows = [_pipeline_feed_row(1, last_seen="2026-05-12T11:00:00Z")]
            second_report = jf.run_pipeline(
                output_dir=data_dir,
                run_id="pipeline_fetch_2",
                source_loaders=[("pipeline_feed_source", loader)],
                show_progress=False,
                force_refresh_all=True,
            )
            assert _close_pipeline_fetch(api, second_report) is True

            assert calls["count"] == 2
            assert int(second_report["summary"]["outputCount"]) == 1
            assert runtime.current_generation() == generation
            assert jobs_feed_content_hash(runtime.current_rows()) == first_hash
            assert runtime.current_rows()[0]["lastSeenAt"] == first_sqlite_last_seen
            assert [item["code"] for item in diagnostics if item["surface"] == "jobsFeed"] == [
                "jobs_feed_content_unchanged"
            ]
            assert store.execute_scalar("SELECT COUNT(*) FROM job_feed_generation_stats") == 1
