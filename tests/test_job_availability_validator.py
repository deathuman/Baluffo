from src.jobs.availability_validator import DirectResponse, classify_direct_response
from src.jobs.state_lifecycle import apply_direct_availability_evidence

URL = "https://boards.greenhouse.io/studio/jobs/123"


def _classify(
    status: int, text: str = "", final_url: str = URL, at: str = "2026-07-01T10:00:00+00:00"
):
    return classify_direct_response(
        DirectResponse(status=status, final_url=final_url, text=text),
        original_url=URL,
        checked_at=at,
    )


def test_definitive_http_closure_and_live_details() -> None:
    assert _classify(410)["confidence"] == "definitive"
    assert _classify(410)["kind"] == "direct_closed"
    assert (
        _classify(
            200,
            "<main>Senior Tools Engineer "
            "<a aria-label='Apply now' href='/studio/jobs/123#application'>Apply</a></main>",
        )["kind"]
        == "direct_live"
    )


def test_closure_phrase_in_script_does_not_close_live_provider_job() -> None:
    evidence = _classify(
        200,
        "<script>const copy = 'this position is no longer available'</script>"
        "<main>Senior Tools Engineer "
        "<a aria-label='Apply now' href='/studio/jobs/123#application'>Apply</a></main>",
    )
    assert evidence["kind"] == "direct_live"
    assert evidence["confidence"] == "definitive"


def test_conflicting_visible_provider_signals_are_not_definitive() -> None:
    evidence = _classify(
        200,
        "<main>This position is no longer available. "
        "<a aria-label='Apply now' href='/studio/jobs/123#application'>Apply now</a></main>",
    )
    assert evidence["kind"] == "direct_closed"
    assert evidence["confidence"] == "ambiguous"


def test_unrelated_visible_closure_phrase_is_only_ambiguous() -> None:
    evidence = _classify(
        200,
        "<main>Senior Tools Engineer details</main>"
        "<aside>Recommended role: this position is no longer available</aside>",
    )
    assert evidence["kind"] == "direct_closed"
    assert evidence["confidence"] == "ambiguous"


def test_unknown_success_page_is_not_definitively_live() -> None:
    evidence = _classify(
        200,
        "<main>Senior Tools Engineer - Apply now</main>",
        "https://company.example/careers/search/123",
    )
    assert evidence["kind"] == "direct_unverified"
    assert evidence["confidence"] == "unknown"


def test_transient_and_blocked_responses_never_imply_closure() -> None:
    for status in (403, 429, 500, 503):
        evidence = _classify(status)
        assert evidence["kind"] == "direct_unverified"
        assert evidence["confidence"] == "unknown"
    assert _classify(200, "Verify you are human - Cloudflare Ray ID")["kind"] == "anti_bot"


def test_generic_cross_domain_career_redirect_is_ambiguous() -> None:
    evidence = _classify(200, "Careers", "https://company.example/careers")
    assert evidence["kind"] == "generic_redirect"
    assert evidence["confidence"] == "ambiguous"


def test_redirect_to_different_provider_posting_is_ambiguous() -> None:
    evidence = _classify(
        200,
        "<main>Senior Tools Engineer "
        "<a aria-label='Apply now' href='/studio/jobs/999/application'>Apply</a></main>",
        "https://boards.greenhouse.io/studio/jobs/999",
    )
    assert evidence["kind"] == "generic_redirect"
    assert evidence["confidence"] == "ambiguous"


def test_redirect_preserving_exact_provider_posting_can_prove_live() -> None:
    evidence = _classify(
        200,
        "<main>Senior Tools Engineer "
        "<a aria-label='Apply now' href='/studio/jobs/123/application'>Apply</a></main>",
        "https://job-boards.greenhouse.io/studio/jobs/123",
    )
    assert evidence["kind"] == "direct_live"
    assert evidence["confidence"] == "definitive"


def test_redirect_to_different_provider_tenant_is_ambiguous() -> None:
    original = "https://studio-a.myworkdayjobs.com/en-US/careers/job/London/Engineer_R123"
    final = "https://studio-b.myworkdayjobs.com/en-US/careers/job/London/Engineer_R123"
    evidence = classify_direct_response(
        DirectResponse(
            status=200,
            final_url=final,
            text=(
                "<main>Engineer<form aria-label='Apply now' "
                "action='/en-US/careers/job/London/Engineer_R123/application'></form></main>"
            ),
        ),
        original_url=original,
        checked_at="2026-07-01T10:00:00+00:00",
    )
    assert evidence["kind"] == "generic_redirect"
    assert evidence["confidence"] == "ambiguous"


def test_generic_same_domain_career_redirect_is_ambiguous() -> None:
    evidence = _classify(200, "Careers", "https://jobs.example.com/careers")
    assert evidence["kind"] == "generic_redirect"
    assert evidence["confidence"] == "ambiguous"


def test_direct_generic_career_page_is_not_definitively_live() -> None:
    url = "https://jobs.example.com/jobs"
    evidence = classify_direct_response(
        DirectResponse(status=200, final_url=url, text="Browse open roles"),
        original_url=url,
        checked_at="2026-07-01T10:00:00+00:00",
    )
    assert evidence["kind"] == "generic_redirect"
    assert evidence["confidence"] == "ambiguous"


def test_provider_detail_path_remains_definitively_live() -> None:
    evidence = _classify(
        200,
        "<main>Senior Tools Engineer "
        "<form aria-label='Apply now' action='/studio/jobs/123/application'></form></main>",
    )
    assert evidence["kind"] == "direct_live"
    assert evidence["confidence"] == "definitive"


def test_unrelated_provider_apply_action_does_not_prove_checked_posting_live() -> None:
    evidence = _classify(
        200,
        "<header><a aria-label='Apply now' href='/studio/jobs/999/application'>"
        "Apply to a recommended role</a></header><main>Browse similar roles</main>",
    )
    assert evidence["kind"] == "direct_unverified"
    assert evidence["confidence"] == "unknown"


def test_provider_action_posting_id_requires_exact_match() -> None:
    evidence = _classify(
        200,
        "<main><a aria-label='Apply now' href='/studio/jobs/9123/application'>"
        "Apply to a recommended role</a></main>",
    )
    assert evidence["kind"] == "direct_unverified"
    assert evidence["confidence"] == "unknown"


def test_visible_apply_phrase_without_posting_action_is_not_definitive() -> None:
    evidence = _classify(200, "<main>Senior Tools Engineer - Apply now</main>")
    assert evidence["kind"] == "direct_unverified"
    assert evidence["confidence"] == "unknown"


def test_jobposting_token_or_unrelated_schema_does_not_prove_live() -> None:
    token_only = _classify(
        200,
        '<script type="application/ld+json">{"note":"JobPosting"}</script>'
        "<main>Senior Tools Engineer</main>",
    )
    unrelated = _classify(
        200,
        '<script type="application/ld+json">'
        '{"@type":"JobPosting","url":"https://boards.greenhouse.io/studio/jobs/999",'
        '"validThrough":"2026-08-01T00:00:00+00:00"}'
        "</script><main>Senior Tools Engineer</main>",
    )
    assert token_only["kind"] == "direct_unverified"
    assert unrelated["kind"] == "direct_unverified"


def test_matching_non_expired_jobposting_is_definitively_live() -> None:
    evidence = _classify(
        200,
        '<script type="application/ld+json">'
        '{"@type":"JobPosting","url":"https://boards.greenhouse.io/studio/jobs/123",'
        '"validThrough":"2026-08-01T00:00:00+00:00"}'
        "</script><main>Senior Tools Engineer</main>",
    )
    assert evidence["kind"] == "direct_live"
    assert evidence["confidence"] == "definitive"


def test_matching_expired_jobposting_is_only_ambiguous_closure() -> None:
    evidence = _classify(
        200,
        '<script type="application/ld+json">'
        '{"@type":"JobPosting","@id":"https://boards.greenhouse.io/studio/jobs/123",'
        '"validThrough":"2026-06-01T00:00:00+00:00"}'
        "</script><main>Senior Tools Engineer</main>",
    )
    assert evidence["kind"] == "direct_closed"
    assert evidence["confidence"] == "ambiguous"


def test_ambiguous_closure_requires_matching_checks_24_hours_apart() -> None:
    entry = {
        "availabilityId": "availability_123",
        "availabilityStatus": "available",
        "status": "active",
        "lastSeenAt": "2026-06-20T10:00:00+00:00",
    }
    first = apply_direct_availability_evidence(
        entry, _classify(200, "Job not found", at="2026-07-01T10:00:00+00:00")
    )
    assert first["availabilityStatus"] == "available"
    too_soon = apply_direct_availability_evidence(
        first, _classify(200, "Job not found", at="2026-07-02T09:59:00+00:00")
    )
    assert too_soon["availabilityStatus"] == "available"
    closed = apply_direct_availability_evidence(
        first, _classify(200, "Job not found", at="2026-07-02T10:00:00+00:00")
    )
    assert closed["availabilityStatus"] == "unavailable"
    assert closed["availabilityClosureOrigin"] == "direct"


def test_direct_evidence_wins_both_source_conflicts() -> None:
    source_live = {
        "availabilityId": "availability_123",
        "availabilityStatus": "available",
        "status": "active",
    }
    closed = apply_direct_availability_evidence(source_live, _classify(404))
    assert closed["availabilityStatus"] == "unavailable"

    source_absent = {
        **closed,
        "availabilityClosureOrigin": "source_absent",
        "availabilityEvidence": {"kind": "source_absent", "confidence": "definitive"},
    }
    reopened = apply_direct_availability_evidence(
        source_absent,
        _classify(
            200,
            "<main>Senior Tools Engineer "
            "<a aria-label='Apply now' href='/studio/jobs/123#application'>Apply</a></main>",
        ),
    )
    assert reopened["availabilityStatus"] == "available"
    assert reopened["status"] == "active"


def test_source_reappearance_does_not_clear_direct_closure() -> None:
    from src.jobs.models import CanonicalJob
    from src.jobs.state_lifecycle import apply_job_lifecycle_state

    current = CanonicalJob.from_mapping(
        {
            "dedupKey": "job-1",
            "title": "Tools Engineer",
            "company": "Studio",
            "jobLink": URL,
            "source": "provider",
            "sourceJobId": "123",
        }
    )
    previous = {
        "status": "likely_removed",
        "availabilityId": "availability_123",
        "availabilityStatus": "unavailable",
        "availabilityClosureOrigin": "direct",
        "availabilityCheckedAt": "2026-07-01T10:00:00+00:00",
        "availabilityVerifiedAt": "2026-07-01T10:00:00+00:00",
        "availabilityUnavailableAt": "2026-07-01T10:00:00+00:00",
        "availabilityEvidence": _classify(410),
        "availabilityAliases": ["job-1", "source:provider:123"],
        "jobLink": URL,
        "source": "provider",
        "sourceJobId": "123",
    }
    rows, lifecycle, _archive, _summary = apply_job_lifecycle_state(
        deduped_rows=[current],
        observed_rows=[current],
        lifecycle_rows={"job-1": previous},
        finished_at="2026-07-03T10:00:00+00:00",
        allow_mark_missing=False,
    )

    assert rows == []
    assert lifecycle["job-1"]["availabilityStatus"] == "unavailable"
    assert lifecycle["job-1"]["availabilityClosureOrigin"] == "direct"
