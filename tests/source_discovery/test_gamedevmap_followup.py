from __future__ import annotations

import json

from src.source_discovery import gamedevmap
from src.source_discovery import gamedevmap_active_dry_run as dry_run

from ._helpers import (
    discovery_config_without_generator_stages,
    discovery_orchestrator,
    override_discovery_runtime,
    sd,
    workspace_tmpdir,
)
from .gamedevmap_test_helpers import (
    gamedevmap_config,
    validated_static_candidate,
)
from .gamedevmap_test_helpers import (
    write_gamedevmap_audit_artifact as _write_artifact,
)


def _config(**overrides) -> dict[str, object]:
    defaults = {
        "blockedCategories": [],
        "activeAuditTtlMinutes": 360,
        "validatedStaticQueueCap": 500,
        "validatedStaticDomainCap": 8,
    }
    defaults.update(overrides)
    return gamedevmap_config(
        allowed_categories=["Developer"],
        **defaults,
    )


def _validated_static_candidate(index: int) -> dict[str, object]:
    return validated_static_candidate(index=index, recovered=True)


def test_gamedevmap_lost_recovery_compare_classifies_fetch_loss() -> None:
    previous = {
        "activeCandidates": [
            _validated_static_candidate(1),
            _validated_static_candidate(2),
        ]
    }
    current = {
        "activeCandidates": [_validated_static_candidate(2)],
        "rejectedForActivation": [
            {
                "reason": "no_careers_evidence",
                "reasonDetail": "recovery_fetch_failed",
                "candidate": _validated_static_candidate(1),
            }
        ],
    }

    audit = dry_run.compare_gamedevmap_recovered_sources(
        current_artifact=current,
        previous_artifact=previous,
    )

    assert audit["lostCount"] == 1
    assert audit["lossCauseCounts"] == {"recovery_timeout_or_fetch_failed": 1}
    assert (
        audit["lostCandidates"][0]["sourceId"]
        == "static:listing_url:https://validated-1.example1.com/jobs"
    )


def test_gamedevmap_browser_recovery_processes_only_browser_candidates() -> None:
    config = _config(activeAuditBrowserRecoveryConcurrency=1)
    browser_candidates = [
        {
            "name": "Browser Studio browser recovery",
            "studio": "Browser Studio",
            "url": "https://browser.example.com",
            "sourceDirectoryEntryUrl": "https://www.gamedevmap.com/?query=browser",
            "reasonDetail": "js_shell",
        }
    ]
    calls: list[str] = []

    def fake_browser(url: str, _timeout_s: int) -> tuple[str, str]:
        calls.append(url)
        return (
            '<html><a href="/jobs/rendering-engineer">Rendering Engineer</a></html>',
            "",
        )

    with workspace_tmpdir("gamedevmap-browser-recovery") as root:
        output_path = root / "gamedevmap-active-source-dry-run.json"
        _write_artifact(
            output_path,
            config=config,
            active_candidates=[],
            browser_candidates=browser_candidates,
        )
        output = sd.run_gamedevmap_browser_recovery(
            timeout_s=5,
            config=config,
            output_path=output_path,
            browser_fetcher=fake_browser,
            fetcher=lambda _url, _timeout: "",
        )

    assert calls == ["https://browser.example.com"]
    assert output["summary"]["browserRecoveredActiveCandidates"] == 1
    assert output["activeCandidates"][0]["gamedevmapBrowserRecovery"] is True
    assert output["browserRecovery"]["processedCount"] == 1


def test_gamedevmap_validated_static_queue_cap_override_expands_default_static_cap() -> None:
    with workspace_tmpdir("gamedevmap-static-queue-cap") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]) as paths:
            config = discovery_config_without_generator_stages(
                gamedevmap=_config(validatedStaticQueueCap=25)["gamedevmap"]
            )
            config["autoApproveHealthyPendingOnComplete"] = False
            _write_artifact(
                root / "gamedevmap-active-source-dry-run.json",
                config=config,
                active_candidates=[_validated_static_candidate(index) for index in range(20)],
            )

            report = discovery_orchestrator.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=lambda _url, _timeout: "",
            )

            active_rows = (
                json.loads(paths.active_path.read_text(encoding="utf-8"))
                if paths.active_path.exists()
                else []
            )
            pending_rows = json.loads(paths.pending_path.read_text(encoding="utf-8"))

    assert active_rows == []
    assert len(pending_rows) == 20
    assert int((report["summary"].get("queuedByAdapter") or {}).get("static") or 0) == 20
    assert int((report["summary"].get("deferredByAdapter") or {}).get("static") or 0) == 0
    assert all("queueAdapterCapOverride" not in row for row in report["candidates"])
    assert all("queueDomainCapOverride" not in row for row in report["candidates"])


def test_gamedevmap_validated_static_promotion_can_be_disabled() -> None:
    artifact = {"activeCandidates": [_validated_static_candidate(1)]}

    provider_rows, static_rows = dry_run.gamedevmap_validated_candidates_from_artifact(
        artifact,
        promote_validated_static=False,
        validated_static_queue_cap=25,
        validated_static_domain_cap=8,
    )

    assert provider_rows == []
    assert static_rows == []


def test_gamedevmap_audit_summary_is_written_to_discovery_report() -> None:
    with workspace_tmpdir("gamedevmap-audit-report-summary") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            config = discovery_config_without_generator_stages(gamedevmap=_config()["gamedevmap"])
            config["autoApproveHealthyPendingOnComplete"] = False
            _write_artifact(
                root / "gamedevmap-active-source-dry-run.json",
                config=config,
                active_candidates=[_validated_static_candidate(1)],
                browser_candidates=[
                    {
                        "name": "Browser",
                        "studio": "Browser",
                        "url": "https://browser.example.com",
                    }
                ],
            )

            report = discovery_orchestrator.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=lambda _url, _timeout: "",
            )

    audit_summary = report["gamedevmapAuditSummary"]
    assert audit_summary["cacheHit"] is True
    assert audit_summary["activeStaticCandidates"] == 1
    assert audit_summary["browserRecoveryCandidates"] == 1
    assert audit_summary["artifactSizeBytes"] == 1234
    assert report["summary"]["gamedevmapAudit"]["auditDurationMs"] == 123


def test_gamedevmap_discovery_reuses_active_audit_artifact() -> None:
    with workspace_tmpdir("gamedevmap-active-audit-reuse") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            config = discovery_config_without_generator_stages(gamedevmap=_config()["gamedevmap"])
            _write_artifact(
                root / "gamedevmap-active-source-dry-run.json",
                config=config,
                active_candidates=[_validated_static_candidate(1)],
            )

            provider_rows, static_rows, failures = gamedevmap.discover_gamedevmap_candidates(
                5,
                config=config,
                fetcher=lambda _url, _timeout: (_ for _ in ()).throw(
                    AssertionError("fresh active-source audit artifact should bypass fetch")
                ),
            )

    assert provider_rows == []
    assert len(static_rows) == 1
    assert len(failures) == 1


def test_gamedevmap_audit_report_summary_shape_stays_compatible() -> None:
    summary = dry_run.gamedevmap_audit_report_summary(
        {
            "progress": {"complete": True},
            "runtime": {"artifactSizeBytes": 222},
            "timings": {"totalsMs": {"totalMs": 123, "probeMs": 45}},
            "summary": {
                "activeCandidates": 7,
                "activeAdapterCounts": {"static": 3},
                "recoveredActiveCandidates": 2,
                "browserRecoveryCandidates": 4,
                "browserRecoveredActiveCandidates": 1,
                "lostRecoveredActiveCandidates": 5,
                "rejectedReasonDetailCounts": {"js_shell": 9},
            },
            "failureCounts": {"homepage_fetch": 6},
        },
        cache_hit=True,
        output_path="artifact.json",
    )

    assert summary == {
        "cacheHit": True,
        "complete": True,
        "auditDurationMs": 123,
        "activeCandidates": 7,
        "activeProviderCandidates": 4,
        "activeStaticCandidates": 3,
        "recoveredActiveCandidates": 2,
        "browserRecoveryCandidates": 4,
        "browserRecoveredActiveCandidates": 1,
        "artifactSizeBytes": 222,
        "timingTotalsMs": {"totalMs": 123, "probeMs": 45},
        "topFailureBuckets": [
            {"key": "js_shell", "count": 9},
            {"key": "homepage_fetch", "count": 6},
        ],
        "lostRecoveredActiveCandidates": 5,
        "outputPath": "artifact.json",
    }


def test_gamedevmap_artifact_helpers_keep_report_compatibility() -> None:
    source_list = [{"name": "one"}]
    copied_list = dry_run._as_list(source_list)
    copied_list.append({"name": "two"})
    assert source_list == [{"name": "one"}]

    source_dict = {"count": 1}
    copied_dict = dry_run._as_dict(source_dict)
    copied_dict["count"] = 2
    assert source_dict == {"count": 1}

    assert dry_run._safe_int("bad", 7) == 7

    summary = dry_run.gamedevmap_audit_report_summary(
        {
            "summary": "not-a-dict",
            "runtime": "not-a-dict",
            "timings": {"totalsMs": {"totalMs": "9"}},
            "activeCandidates": "not-a-list",
            "rejectedForActivation": "not-a-list",
            "failureSamples": "not-a-list",
            "browserRecovery": "not-a-dict",
            "lostRecoveryAudit": "not-a-dict",
        },
        cache_hit=False,
    )

    assert summary["activeCandidates"] == 0
    assert summary["artifactSizeBytes"] == 0
    assert summary["browserRecoveryCandidates"] == 0


def test_gamedevmap_active_audit_ttl_uses_active_key_only() -> None:
    assert dry_run._active_audit_ttl_minutes({"activeAuditTtlMinutes": 42}) == 42
    assert dry_run._active_audit_ttl_minutes({}) == 360
    assert dry_run._active_audit_ttl_minutes({"activeAuditTtlMinutes": "bad"}) == 360


def test_gamedevmap_followup_cli_flags_parse() -> None:
    args = discovery_orchestrator.parse_args(
        [
            "--gamedevmap-active-dry-run",
            "--gamedevmap-dry-run-compare-artifact",
            "previous.json",
            "--gamedevmap-browser-recovery",
        ]
    )

    assert str(args.gamedevmap_dry_run_compare_artifact) == "previous.json"
    assert bool(args.gamedevmap_browser_recovery)
