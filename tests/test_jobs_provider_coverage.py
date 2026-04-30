from src.jobs.common.contracts_provider_coverage import build_provider_coverage_summary
from src.jobs.common.contracts_fetch_report import normalize_fetch_report_payload
from src.jobs.common.contracts_source_reports import normalize_source_report_row
from src.jobs.state_source_state import update_source_state_rows


FINISHED_AT = "2026-04-30T12:00:00+00:00"


def _update(existing, report, *, canonical_rows=None, finished_at=FINISHED_AT):
    return update_source_state_rows(
        source_state_rows=existing,
        source_reports=[report],
        canonical_rows=canonical_rows or [],
        finished_at=finished_at,
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=60,
    )


def _provider_report(**overrides):
    report = {
        "name": "Studio Greenhouse",
        "adapter": "greenhouse",
        "status": "ok",
        "keptCount": 2,
        "fetchedCount": 2,
        "migrationSourceIdentity": "static:listing_url:https://studio.example/jobs",
        "detectedProviderFamily": "greenhouse",
        "detectedProviderUrl": "https://boards.greenhouse.io/studio",
        "detectedProviderId": "studio",
        "migrationConfidence": 92,
        "migrationReasons": ["supported_provider_url"],
    }
    report.update(overrides)
    return report


def test_provider_fetch_with_jobs_validates_provider_coverage():
    rows = _update(
        {},
        _provider_report(),
        canonical_rows=[
            {
                "source": "Studio Greenhouse",
                "sourceBundle": [{"source": "Studio Greenhouse"}],
            }
        ],
    )

    entry = rows["Studio Greenhouse"]
    assert entry["providerCoverageStatus"] == "validated_provider"
    assert entry["providerCoverageFirstSuccessAt"] == FINISHED_AT
    assert entry["providerCoverageLastSuccessAt"] == FINISHED_AT
    assert entry["providerCoverageSuccessCount"] == 1
    assert entry["providerCoverageConsecutiveSuccesses"] == 1
    assert entry["providerCoverageConsecutiveFailures"] == 0
    assert entry["providerCoverageLatestKeptCount"] == 2
    assert entry["providerReplacementReadiness"] == "candidate"
    assert entry["migrationSourceIdentity"].startswith("static:listing_url")


def test_repeated_provider_successes_make_replacement_readiness_ready_later_only():
    first = _update({}, _provider_report())
    second = _update(
        first,
        _provider_report(keptCount=3, fetchedCount=3),
        finished_at="2026-04-30T13:00:00+00:00",
    )

    entry = second["Studio Greenhouse"]
    assert entry["providerCoverageStatus"] == "validated_provider"
    assert entry["providerCoverageSuccessCount"] == 2
    assert entry["providerCoverageConsecutiveSuccesses"] == 2
    assert entry["providerReplacementReadiness"] == "ready_later"

    summary = build_provider_coverage_summary(second)
    assert summary["statusCounts"]["validated_provider"] == 1
    assert summary["readyLaterProviders"][0]["name"] == "Studio Greenhouse"


def test_provider_failure_records_failure_without_validation():
    rows = _update({}, _provider_report(status="error", keptCount=0, error="HTTP 500"))

    entry = rows["Studio Greenhouse"]
    assert entry["providerCoverageStatus"] == "failed_provider"
    assert entry.get("providerCoverageSuccessCount", 0) == 0
    assert entry["providerCoverageConsecutiveFailures"] == 1
    assert entry["providerCoverageLatestError"] == "HTTP 500"
    assert entry["providerReplacementReadiness"] == "none"


def test_provider_failure_after_success_becomes_unstable():
    first = _update({}, _provider_report())
    second = _update(
        first,
        _provider_report(status="error", keptCount=0, error="timeout"),
        finished_at="2026-04-30T13:00:00+00:00",
    )

    entry = second["Studio Greenhouse"]
    assert entry["providerCoverageStatus"] == "unstable_provider"
    assert entry["providerCoverageSuccessCount"] == 1
    assert entry["providerCoverageConsecutiveSuccesses"] == 0
    assert entry["providerCoverageConsecutiveFailures"] == 1


def test_excluded_provider_skip_preserves_prior_coverage_counters():
    existing = {
        "Studio Greenhouse": {
            "lastAdapter": "greenhouse",
            "migrationSourceIdentity": "static:listing_url:https://studio.example/jobs",
            "providerCoverageStatus": "failed_provider",
            "providerCoverageConsecutiveFailures": 2,
            "providerCoverageLatestKeptCount": 4,
            "providerCoverageLatestError": "previous error",
        }
    }
    rows = _update(
        existing,
        _provider_report(status="excluded", keptCount=0, exclusionReason="cache_fresh"),
    )

    entry = rows["Studio Greenhouse"]
    assert entry["providerCoverageStatus"] == "failed_provider"
    assert entry["providerCoverageConsecutiveFailures"] == 2
    assert entry["providerCoverageLatestKeptCount"] == 4
    assert entry["providerCoverageLatestError"] == "previous error"


def test_zero_job_provider_success_needs_review_without_counter_increment():
    rows = _update({}, _provider_report(keptCount=0, fetchedCount=0))

    entry = rows["Studio Greenhouse"]
    assert entry["providerCoverageStatus"] == "needs_review"
    assert entry.get("providerCoverageSuccessCount", 0) == 0
    assert entry.get("providerCoverageConsecutiveSuccesses", 0) == 0
    assert entry.get("providerCoverageConsecutiveFailures", 0) == 0


def test_static_report_with_provider_metadata_does_not_update_provider_coverage():
    rows = _update(
        {},
        _provider_report(
            name="Static Studio",
            adapter="static",
            keptCount=12,
            fetchedCount=12,
        ),
    )

    entry = rows["Static Studio"]
    assert "providerCoverageStatus" not in entry
    assert "providerCoverageSuccessCount" not in entry


def test_source_report_normalization_preserves_provider_migration_details_only_for_providers():
    row = normalize_source_report_row(
        {
            "name": "provider_api",
            "adapter": "greenhouse",
            "status": "ok",
            "details": [_provider_report()],
        }
    )

    detail = row["details"][0]
    assert detail["migrationSourceIdentity"].startswith("static:listing_url")
    assert detail["detectedProviderFamily"] == "greenhouse"

    static_row = normalize_source_report_row(
        {
            "name": "static",
            "adapter": "static",
            "status": "ok",
            "migrationSourceIdentity": "static:listing_url:https://studio.example/jobs",
            "keptCount": 1,
        }
    )
    assert "migrationSourceIdentity" not in static_row


def test_fetch_report_normalization_preserves_provider_coverage_summary():
    report = normalize_fetch_report_payload(
        {
            "summary": {"sourceCount": 1},
            "providerCoverage": {
                "totalProviderCandidates": 1,
                "statusCounts": {"validated_provider": 1},
                "validatedProviders": [
                    {
                        "name": "Studio Greenhouse",
                        "providerCoverageStatus": "validated_provider",
                        "providerReplacementReadiness": "candidate",
                        "providerCoverageLatestKeptCount": 2,
                    }
                ],
            },
        }
    )

    coverage = report["providerCoverage"]
    assert coverage["totalProviderCandidates"] == 1
    assert coverage["statusCounts"]["validated_provider"] == 1
    assert coverage["validatedProviders"][0]["providerCoverageStatus"] == "validated_provider"
