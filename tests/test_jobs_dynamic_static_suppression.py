import copy
import json
from pathlib import Path

from src import jobs_fetcher as jf
from src.jobs.pipeline_loader_selection import apply_dynamic_redundant_static_exclusions
from src.jobs.state_source_state import update_source_state_rows
from tests.helpers.temp_paths import workspace_tmpdir

STATIC_SOURCE_NAME = "static_source::static:listing_url:https://studio.example/jobs"
MIGRATION_SOURCE_IDENTITY = "static:listing_url:https://studio.example/jobs"
PROVIDER_SOURCE_NAME = "Studio Greenhouse"


def _eligible_provider_state(**overrides):
    state = {
        "Studio Greenhouse": {
            "lastAdapter": "greenhouse",
            "providerCoverageStatus": "validated_provider",
            "providerCoverageConsecutiveSuccesses": 2,
            "providerCoverageLatestKeptCount": 3,
            "migrationSourceIdentity": MIGRATION_SOURCE_IDENTITY,
        }
    }
    state["Studio Greenhouse"].update(overrides)
    return state


def _excluded_report(name, reason):
    return {
        "name": name,
        "status": "excluded",
        "adapter": "custom",
        "fetchStrategy": "auto",
        "studio": "",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": reason,
        "exclusionReason": reason,
        "durationMs": 0,
    }


def _provider_report(**overrides):
    report = {
        "name": PROVIDER_SOURCE_NAME,
        "adapter": "greenhouse",
        "status": "ok",
        "keptCount": 2,
        "fetchedCount": 2,
        "migrationSourceIdentity": MIGRATION_SOURCE_IDENTITY,
    }
    report.update(overrides)
    return report


def _update_source_state(existing, report, *, finished_at):
    return update_source_state_rows(
        source_state_rows=existing,
        source_reports=[report],
        canonical_rows=[
            {
                "source": PROVIDER_SOURCE_NAME,
                "sourceBundle": [{"source": PROVIDER_SOURCE_NAME}],
            }
        ],
        finished_at=finished_at,
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=60,
    )


def _apply_dynamic_suppression(source_state_rows):
    return apply_dynamic_redundant_static_exclusions(
        [
            (PROVIDER_SOURCE_NAME, lambda **_: []),
            (STATIC_SOURCE_NAME, lambda **_: []),
        ],
        source_state_rows=source_state_rows,
        build_excluded_source_report=_excluded_report,
        source_report_meta={
            PROVIDER_SOURCE_NAME: {"adapter": "greenhouse"},
            STATIC_SOURCE_NAME: {"adapter": "static"},
        },
    )


def test_dynamic_static_suppression_requires_repeated_provider_successes():
    filtered, excluded, policy = apply_dynamic_redundant_static_exclusions(
        [
            ("greenhouse_boards", lambda **_: []),
            (STATIC_SOURCE_NAME, lambda **_: []),
        ],
        source_state_rows=_eligible_provider_state(),
        build_excluded_source_report=_excluded_report,
        source_report_meta={"greenhouse_boards": {"adapter": "greenhouse"}},
    )

    assert [name for name, _loader in filtered] == ["greenhouse_boards"]
    assert excluded[0]["name"] == STATIC_SOURCE_NAME
    assert excluded[0]["adapter"] == "static"
    assert excluded[0]["exclusionReason"] == "dynamic_redundant_provider"
    assert excluded[0]["coveredByProviderSourceId"] == "Studio Greenhouse"
    assert excluded[0]["providerCoverageConsecutiveSuccesses"] == 2
    assert excluded[0]["migrationSourceIdentity"] == MIGRATION_SOURCE_IDENTITY
    assert policy["suppressedCount"] == 1
    assert policy["suppressedPairs"][0]["reason"] == "missing_prior_evidence"


def test_dynamic_static_suppression_does_not_apply_after_one_success_or_bad_status():
    loaders = [("greenhouse_boards", lambda **_: []), (STATIC_SOURCE_NAME, lambda **_: [])]
    one_success_filtered, one_success_excluded, one_success_policy = (
        apply_dynamic_redundant_static_exclusions(
            loaders,
            source_state_rows=_eligible_provider_state(providerCoverageConsecutiveSuccesses=1),
            build_excluded_source_report=_excluded_report,
            source_report_meta={"greenhouse_boards": {"adapter": "greenhouse"}},
        )
    )
    unstable_filtered, unstable_excluded, unstable_policy = (
        apply_dynamic_redundant_static_exclusions(
            loaders,
            source_state_rows=_eligible_provider_state(providerCoverageStatus="unstable_provider"),
            build_excluded_source_report=_excluded_report,
            source_report_meta={"greenhouse_boards": {"adapter": "greenhouse"}},
        )
    )

    assert [name for name, _loader in one_success_filtered] == [
        "greenhouse_boards",
        STATIC_SOURCE_NAME,
    ]
    assert one_success_excluded == []
    assert one_success_policy["eligibleCount"] == 0
    assert [name for name, _loader in unstable_filtered] == [
        "greenhouse_boards",
        STATIC_SOURCE_NAME,
    ]
    assert unstable_excluded == []
    assert unstable_policy["eligibleCount"] == 0


def test_linked_provider_success_sequence_controls_dynamic_static_suppression():
    static_registry_row = {
        "id": MIGRATION_SOURCE_IDENTITY,
        "adapter": "static",
        "name": "Static Studio",
        "pages": ["https://studio.example/jobs"],
    }
    original_static_registry_row = copy.deepcopy(static_registry_row)

    first = _update_source_state(
        {},
        _provider_report(),
        finished_at="2026-04-30T12:00:00+00:00",
    )
    first_filtered, first_excluded, first_policy = _apply_dynamic_suppression(first)

    assert first[PROVIDER_SOURCE_NAME]["providerCoverageStatus"] == "validated_provider"
    assert first[PROVIDER_SOURCE_NAME]["providerCoverageConsecutiveSuccesses"] == 1
    assert [name for name, _loader in first_filtered] == [
        PROVIDER_SOURCE_NAME,
        STATIC_SOURCE_NAME,
    ]
    assert first_excluded == []
    assert first_policy["suppressedCount"] == 0

    skipped = _update_source_state(
        first,
        _provider_report(status="excluded", keptCount=0, exclusionReason="cache_fresh"),
        finished_at="2026-04-30T13:00:00+00:00",
    )
    skipped_filtered, skipped_excluded, skipped_policy = _apply_dynamic_suppression(skipped)

    assert skipped[PROVIDER_SOURCE_NAME]["providerCoverageConsecutiveSuccesses"] == 1
    assert [name for name, _loader in skipped_filtered] == [
        PROVIDER_SOURCE_NAME,
        STATIC_SOURCE_NAME,
    ]
    assert skipped_excluded == []
    assert skipped_policy["suppressedCount"] == 0

    second = _update_source_state(
        skipped,
        _provider_report(keptCount=3, fetchedCount=3),
        finished_at="2026-04-30T14:00:00+00:00",
    )
    second_filtered, second_excluded, second_policy = _apply_dynamic_suppression(second)

    assert second[PROVIDER_SOURCE_NAME]["providerCoverageConsecutiveSuccesses"] == 2
    assert second[PROVIDER_SOURCE_NAME]["providerReplacementReadiness"] == "ready_later"
    assert [name for name, _loader in second_filtered] == [PROVIDER_SOURCE_NAME]
    assert second_excluded[0]["name"] == STATIC_SOURCE_NAME
    assert second_excluded[0]["exclusionReason"] == "dynamic_redundant_provider"
    assert second_excluded[0]["providerCoverageConsecutiveSuccesses"] == 2
    assert second_excluded[0]["migrationSourceIdentity"] == MIGRATION_SOURCE_IDENTITY
    assert second_policy["suppressedCount"] == 1
    assert static_registry_row == original_static_registry_row


def test_run_pipeline_dynamically_suppresses_default_static_source_without_mutating_registry():
    calls = {"static": 0, "provider": 0}
    static_registry_row = {
        "id": MIGRATION_SOURCE_IDENTITY,
        "adapter": "static",
        "name": "Static Studio",
        "pages": ["https://studio.example/jobs"],
    }
    original_static_registry_row = copy.deepcopy(static_registry_row)

    def provider_loader(**_: object):
        calls["provider"] += 1
        return [
            {
                "title": "Provider Engineer",
                "company": "Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://boards.greenhouse.io/studio/jobs/provider-engineer",
                "sector": "Game",
                "sourceJobId": "provider-1",
                "postedAt": "2026-03-01",
            }
        ]

    def static_loader(**_: object):
        calls["static"] += 1
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-dynamic-static-suppression") as tmp:
            out = Path(tmp)
            sources = _eligible_provider_state()
            sources[STATIC_SOURCE_NAME] = {"lastKeptCount": 2}
            source_state = {
                "schemaVersion": jf.SCHEMA_VERSION,
                "sources": sources,
            }
            (out / "jobs-source-state.json").write_text(json.dumps(source_state), encoding="utf-8")
            jf.default_source_loaders = lambda **_: [
                ("greenhouse_boards", provider_loader),
                (STATIC_SOURCE_NAME, static_loader),
            ]

            report = jf.run_pipeline(output_dir=out, show_progress=False, force_refresh_all=True)

            assert calls == {"static": 0, "provider": 1}
            suppressed = next(row for row in report["sources"] if row["name"] == STATIC_SOURCE_NAME)
            assert suppressed["status"] == "excluded"
            assert suppressed["exclusionReason"] == "dynamic_redundant_provider"
            assert suppressed["coveredByProviderSourceId"] == "Studio Greenhouse"
            assert report["sourceHealth"]["dynamicRedundantStaticSources"] == 1
            assert report["providerStaticOverlap"]["suppressedStaticCount"] == 1
            assert report["providerStaticOverlap"]["safePairCount"] == 1
            assert report["providerStaticOverlap"]["pairs"][0]["auditStatus"] == "safe"
            assert report["staticSuppressionPolicy"]["suppressedCount"] == 1
            assert (
                report["staticSuppressionPolicy"]["suppressedPairs"][0]["lastAuditStatus"] == "safe"
            )
            assert static_registry_row == original_static_registry_row
    finally:
        jf.default_source_loaders = previous_default_loaders


def test_explicit_static_selection_bypasses_dynamic_suppression():
    calls = {"static": 0}

    def static_loader(**_: object):
        calls["static"] += 1
        return []

    with workspace_tmpdir("jobs-fetcher-explicit-static-bypasses-suppression") as tmp:
        out = Path(tmp)
        source_state = {
            "schemaVersion": jf.SCHEMA_VERSION,
            "sources": _eligible_provider_state(),
        }
        (out / "jobs-source-state.json").write_text(json.dumps(source_state), encoding="utf-8")

        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[(STATIC_SOURCE_NAME, static_loader)],
            show_progress=False,
            force_refresh_all=True,
            preserve_previous_on_empty=False,
        )

        assert calls["static"] == 1
        row = next(item for item in report["sources"] if item["name"] == STATIC_SOURCE_NAME)
        assert row["status"] == "ok"
        assert row.get("exclusionReason", "") != "dynamic_redundant_provider"


def test_provider_failure_later_allows_static_source_to_run_again():
    calls = {"static": 0}

    def static_loader(**_: object):
        calls["static"] += 1
        return []

    previous_default_loaders = jf.default_source_loaders
    try:
        with workspace_tmpdir("jobs-fetcher-dynamic-static-unsuppressed") as tmp:
            out = Path(tmp)
            source_state = {
                "schemaVersion": jf.SCHEMA_VERSION,
                "sources": _eligible_provider_state(providerCoverageStatus="failed_provider"),
            }
            (out / "jobs-source-state.json").write_text(json.dumps(source_state), encoding="utf-8")
            jf.default_source_loaders = lambda **_: [
                ("greenhouse_boards", lambda **_: []),
                (STATIC_SOURCE_NAME, static_loader),
            ]

            report = jf.run_pipeline(output_dir=out, show_progress=False, force_refresh_all=True)

            assert calls["static"] == 1
            row = next(item for item in report["sources"] if item["name"] == STATIC_SOURCE_NAME)
            assert row["status"] == "ok"
            assert row.get("exclusionReason", "") != "dynamic_redundant_provider"
    finally:
        jf.default_source_loaders = previous_default_loaders
