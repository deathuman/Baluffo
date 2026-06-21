"""Tests for jobs fetcher pipeline social incremental behavior."""

from pathlib import Path
from unittest import mock

from src import jobs_fetcher as jf
from tests.helpers.temp_paths import workspace_tmpdir


def test_apply_incremental_cache_exclusions_keeps_social_multi_feed_loaders_for_detail_level_refresh() -> (
    None
):
    import src.jobs.state_incremental as state_pkg
    from src.jobs import pipeline_loader_selection as selection_pkg

    now = jf.datetime.now(jf.timezone.utc)
    future = (now + jf.timedelta(minutes=10)).isoformat()
    selected = [
        ("social_x", lambda **_: []),
        ("social_mastodon", lambda **_: []),
        ("social_reddit", lambda **_: []),
    ]
    source_state_rows = {
        "social_x": {
            "lastAdapter": "social",
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
        "social_mastodon": {
            "lastAdapter": "social",
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        },
        "social_reddit": {
            "lastAdapter": "social",
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
            "social_x": {"adapter": "social"},
            "social_mastodon": {"adapter": "social"},
            "social_reddit": {"adapter": "social"},
        },
    )
    assert [name for name, _ in filtered] == ["social_x", "social_mastodon"]
    assert [row["name"] for row in skipped] == ["social_reddit"]


def test_social_x_skips_fresh_query_without_fetching() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 40,
        "rejectForHirePosts": True,
        "x": {
            "enabled": True,
            "queries": ["game jobs"],
            "rssFallback": {"enabled": True, "instances": ["https://nitter.example"]},
        },
    }
    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "x:game jobs": {
            "lastAdapter": "social",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 2,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=20)).isoformat(),
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        }
    }

    def failing_fetch(url: str, timeout: int) -> str:
        raise AssertionError("social_x fetch should be skipped for fresh query")

    rows = jf.run_social_x_source(
        fetch_text=failing_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        social_config=cfg,
        source_state_rows=state_rows,
        force_refresh_all=False,
    )
    assert rows == []
    diag = jf.SOURCE_DIAGNOSTICS.get("social_x") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "skip_fresh"


def test_social_mastodon_skips_fresh_instance_tag_without_fetching() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 40,
        "rejectForHirePosts": True,
        "mastodon": {
            "enabled": True,
            "instances": ["https://mastodon.example"],
            "hashtags": ["gamedevjobs"],
        },
    }
    now = jf.datetime.now(jf.timezone.utc)
    state_rows = {
        "mastodon:mastodon.example:#gamedevjobs": {
            "lastAdapter": "social",
            "lastStatus": "ok",
            "lastSuccessAt": (now - jf.timedelta(minutes=5)).isoformat(),
            "lastKeptCount": 2,
            "nextEligibleCheckAt": (now + jf.timedelta(minutes=20)).isoformat(),
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        }
    }

    def failing_fetch(url: str, timeout: int) -> str:
        raise AssertionError("social_mastodon fetch should be skipped for fresh instance/tag")

    rows = jf.run_social_mastodon_source(
        fetch_text=failing_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        social_config=cfg,
        source_state_rows=state_rows,
        force_refresh_all=False,
    )
    assert rows == []
    diag = jf.SOURCE_DIAGNOSTICS.get("social_mastodon") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "skip_fresh"


def test_social_reddit_skips_fresh_subreddit_without_fetching() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 40,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": True,
            "htmlFallback": True,
            "rateLimitDelay": 0,
        },
    }
    future = "2099-01-01T00:00:00+00:00"
    state_rows = {
        "reddit:r/gamedev": {
            "lastAdapter": "social",
            "nextEligibleCheckAt": future,
            "cacheDecision": "skip_fresh",
            "cacheDecisionReason": "within_freshness_window",
        }
    }

    def failing_fetch(_: str, __: int) -> str:
        raise AssertionError("social_reddit fetch should be skipped for fresh subreddit")

    rows = jf.run_social_reddit_source(
        fetch_text=failing_fetch,
        timeout_s=5,
        retries=0,
        backoff_s=0,
        social_config=cfg,
        source_state_rows=state_rows,
        force_refresh_all=False,
    )
    assert rows == []
    diag = jf.SOURCE_DIAGNOSTICS.get("social_reddit") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "excluded"
    assert details[0]["cacheDecision"] == "skip_fresh"


def test_run_social_reddit_source_keeps_successful_rss_fallback_out_of_error_state() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": True,
            "htmlFallback": False,
            "rateLimitDelay": 0,
        },
    }
    calls = []

    def fake_fetch(url: str, _: int) -> str:
        calls.append(url)
        if url.endswith("/new.json?limit=5"):
            raise RuntimeError("json api blocked")
        if url.endswith("/new.rss"):
            return "<feed />"
        raise AssertionError(f"unexpected reddit url: {url}")

    with mock.patch(
        "src.jobs.adapters.plugins.social.register._social_parsers.parse_reddit_rss_payload",
        return_value=(
            [
                {
                    "title": "Technical Artist",
                    "company": "Nebula Games",
                    "jobLink": "https://jobs.nebula.dev/ta",
                    "sourceJobId": "reddit:gamedev:abc123",
                    "source": "social_reddit",
                }
            ],
            0,
        ),
    ):
        rows = jf.run_social_reddit_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=cfg,
        )
    assert len(rows) >= 1
    diag = jf.SOURCE_DIAGNOSTICS.get("social_reddit") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "ok"
    assert details[0]["error"] == ""


def test_run_social_reddit_source_forwards_heartbeat_to_plugin_run() -> None:
    cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": False,
            "htmlFallback": False,
            "rateLimitDelay": 0,
        },
    }
    heartbeat_calls: list[str] = []

    class _Plugin:
        def run(self, **kwargs):  # noqa: ANN001, ANN202
            heartbeat = kwargs.get("heartbeat_callback")
            assert callable(heartbeat)
            heartbeat()
            return []

    with (
        mock.patch("src.jobs.adapters.social.ensure_social_plugins"),
        mock.patch(
            "src.jobs.adapters.social.default_registry.select", return_value=(_Plugin(), None)
        ),
    ):
        rows = jf.run_social_reddit_source(
            fetch_text=lambda url, _: "{}",
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=cfg,
            heartbeat_callback=lambda: heartbeat_calls.append("tick"),
        )

    assert rows == []
    assert len(heartbeat_calls) >= 2


def test_run_social_reddit_source_keeps_successful_old_reddit_html_fallback_out_of_error_state() -> (
    None
):
    cfg = {
        "enabled": True,
        "minConfidence": 20,
        "rejectForHirePosts": True,
        "reddit": {
            "enabled": True,
            "subreddits": ["gamedev"],
            "maxPostsPerSubreddit": 5,
            "rssFallback": False,
            "htmlFallback": True,
            "rateLimitDelay": 0,
        },
    }
    calls = []

    def fake_fetch(url: str, _: int) -> str:
        calls.append(url)
        if url.endswith("/new.json?limit=5"):
            raise RuntimeError("json api blocked")
        if url.startswith("https://old.reddit.com/r/gamedev/new/"):
            return "<html />"
        raise AssertionError(f"unexpected reddit url: {url}")

    with mock.patch(
        "src.jobs.adapters.plugins.social.register._social_parsers.parse_reddit_html_payload",
        return_value=(
            [
                {
                    "title": "Technical Artist",
                    "company": "Nebula Games",
                    "jobLink": "https://jobs.nebula.dev/ta",
                    "sourceJobId": "reddit:gamedev:abc123",
                    "source": "social_reddit",
                }
            ],
            0,
        ),
    ):
        rows = jf.run_social_reddit_source(
            fetch_text=fake_fetch,
            timeout_s=5,
            retries=0,
            backoff_s=0,
            social_config=cfg,
        )
    assert len(rows) >= 1
    assert any(url.startswith("https://old.reddit.com/r/gamedev/new/") for url in calls)
    diag = jf.SOURCE_DIAGNOSTICS.get("social_reddit") or {}
    details = diag.get("details") or []
    assert len(details) == 1
    assert details[0]["status"] == "ok"
    assert details[0]["error"] == ""


def test_run_pipeline_reports_social_subsource_cache_rollup() -> None:
    def social_loader(**_: object):
        jf.SOURCE_DIAGNOSTICS["social_x"] = {
            "adapter": "social",
            "studio": "x",
            "details": [
                {
                    "name": "x:game jobs",
                    "studio": "x",
                    "status": "excluded",
                    "cacheDecision": "skip_fresh",
                    "cacheDecisionReason": "within_freshness_window",
                },
                {
                    "name": "x:unity jobs",
                    "studio": "x",
                    "status": "ok",
                    "cacheDecision": "run_now",
                    "cacheDecisionReason": "provider_refresh_due",
                    "fetchedCount": 1,
                    "keptCount": 1,
                },
            ],
        }
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Studio Social",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": "https://example.com/social/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "social-x-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-social-subsource-rollup") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            source_loaders=[("social_x", social_loader)],
            show_progress=False,
            force_refresh_all=True,
        )
        row = next(item for item in report["sources"] if item["name"] == "social_x")
        assert row["subsourceCount"] == 2
        assert row["subsourceCacheDecisionCounts"] == {"skip_fresh": 1, "run_now": 1}
        assert row["subsourceSkippedCount"] == 1
        assert row["subsourceRefreshedCount"] == 1


def test_run_pipeline_reports_board_level_provider_cache_rollup() -> None:
    def provider_family_loader(**_: object):
        jf.SOURCE_DIAGNOSTICS["greenhouse_boards"] = {
            "adapter": "greenhouse",
            "studio": "multiple",
            "details": [
                {
                    "name": "Board A",
                    "studio": "Board A",
                    "status": "excluded",
                    "cacheDecision": "skip_fresh",
                    "cacheDecisionReason": "within_freshness_window",
                },
                {
                    "name": "Board B",
                    "studio": "Board B",
                    "status": "excluded",
                    "cacheDecision": "revalidate_only",
                    "cacheDecisionReason": "not_modified_304",
                    "httpStatus": 304,
                },
                {
                    "name": "Board C",
                    "studio": "Board C",
                    "status": "ok",
                    "cacheDecision": "run_now",
                    "cacheDecisionReason": "provider_refresh_due",
                    "fetchedCount": 1,
                    "keptCount": 1,
                },
            ],
        }
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Board C",
                "city": "",
                "country": "Unknown",
                "workType": "",
                "contractType": "",
                "jobLink": "https://example.com/board-c/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "greenhouse:board-c:1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-provider-board-rollup") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp),
            source_loaders=[("greenhouse_boards", provider_family_loader)],
            show_progress=False,
            force_refresh_all=True,
        )
        row = next(item for item in report["sources"] if item["name"] == "greenhouse_boards")
        assert row["boardCount"] == 3
        assert row["boardCacheDecisionCounts"] == {
            "skip_fresh": 1,
            "revalidate_only": 1,
            "run_now": 1,
        }
        assert row["boardSkippedCount"] == 1
        assert row["boardRevalidatedCount"] == 1
        assert row["boardNotModifiedCount"] == 1
        assert row["boardRefreshedCount"] == 1
