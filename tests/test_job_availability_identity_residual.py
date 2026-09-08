from src.jobs.availability_identity import (
    _availability_id,
    _identity_audit,
    _resolve_residual_identity_conflicts,
    _row_identity_token,
    prepare_availability_identities,
)
from src.jobs.common.url import fingerprint_url
from src.jobs.models import CanonicalJob


def test_residual_conflict_resolution_keeps_owner_and_moves_inheritor() -> None:
    """Google-Sheets positional reindex residue: two rows inherit one identity."""
    owner_url = "https://studio.example/jobs/tech-lead"
    inheritor_url = "https://boards.greenhouse.io/studio/jobs/8175336"
    owner_token = f"url:{fingerprint_url(owner_url)}"
    conflicted_id = _availability_id(owner_token)
    owner = CanonicalJob.from_mapping(
        {
            "title": "Technical Artist",
            "company": "Studio",
            "jobLink": owner_url,
            "source": "google_sheets",
            "sourceJobId": "sheet-298",
            "availabilityId": conflicted_id,
        }
    )
    inheritor = CanonicalJob.from_mapping(
        {
            "title": "Community Manager",
            "company": "Studio",
            "jobLink": inheritor_url,
            "source": "google_sheets",
            "sourceJobId": "sheet-261",
            "availabilityId": conflicted_id,
        }
    )

    _monitorable, missing, conflicts = _identity_audit([owner, inheritor])
    assert conflicts == 1
    assert missing == 0
    resolved, tracked, rejected = _resolve_residual_identity_conflicts([owner, inheritor])
    _monitorable, missing, conflicts = _identity_audit(resolved)
    assert (conflicts, missing) == (0, 0)
    assert not rejected
    by_token = {_row_identity_token(row.to_dict()): row.availabilityId for row in resolved}
    assert by_token[owner_token] == conflicted_id
    assert by_token[f"url:{fingerprint_url(inheritor_url)}"] != conflicted_id
    assert conflicted_id in tracked
    assert len(tracked[conflicted_id]) == 1


def test_prepare_identity_quarantine_tracks_residual_replacements() -> None:
    owner_url = "https://studio.example/jobs/tech-lead"
    inheritor_url = "https://boards.greenhouse.io/studio/jobs/8175336"
    owner_token = f"url:{fingerprint_url(owner_url)}"
    conflicted_id = _availability_id(owner_token)
    rows = [
        CanonicalJob.from_mapping(
            {
                "title": title,
                "company": "Studio",
                "jobLink": url,
                "source": "google_sheets",
                "sourceJobId": source_job_id,
                "availabilityId": conflicted_id,
            }
        )
        for title, url, source_job_id in (
            ("Technical Artist", owner_url, "sheet-298"),
            ("Community Manager", inheritor_url, "sheet-261"),
        )
    ]
    prepared = prepare_availability_identities(
        rows=rows,
        observed_rows=rows,
        lifecycle_rows={"legacy": {"availabilityId": conflicted_id}},
        detected_at="2026-07-16T12:00:00+00:00",
    )

    quarantine = prepared.quarantine_additions[conflicted_id]
    expected_rows = sorted(
        (row.availabilityId, fingerprint_url(row.jobLink)) for row in prepared.rows
    )
    assert quarantine["replacementAvailabilityIds"] == [item[0] for item in expected_rows]
    assert quarantine["replacementIdentities"] == [
        {"availabilityId": availability_id, "urlFingerprints": [url_fingerprint]}
        for availability_id, url_fingerprint in expected_rows
    ]


def test_residual_conflict_resolution_rejects_rows_without_public_url() -> None:
    owner_url = "https://studio.example/jobs/tech-lead"
    conflicted_id = _availability_id(f"url:{fingerprint_url(owner_url)}")
    owner = CanonicalJob.from_mapping(
        {
            "title": "Technical Artist",
            "company": "Studio",
            "jobLink": owner_url,
            "source": "google_sheets",
            "sourceJobId": "sheet-298",
            "availabilityId": conflicted_id,
        }
    )
    url_less = CanonicalJob.from_mapping(
        {
            "title": "Producer",
            "company": "Studio",
            "jobLink": "",
            "source": "google_sheets",
            "sourceJobId": "sheet-261",
            "availabilityId": conflicted_id,
        }
    )

    resolved, _tracked, rejected = _resolve_residual_identity_conflicts([owner, url_less])

    assert [row.title for row in resolved] == ["Technical Artist"]
    assert [row.title for row, _reason in rejected] == ["Producer"]
    assert rejected[0][1] == "post_assignment_identity_conflict_without_public_url"


def test_residual_conflict_resolution_is_a_noop_without_conflicts() -> None:
    rows = [
        CanonicalJob.from_mapping(
            {
                "title": title,
                "company": "Studio",
                "jobLink": url,
                "source": "google_sheets",
                "sourceJobId": source_job_id,
                "availabilityId": _availability_id(f"url:{fingerprint_url(url)}"),
            }
        )
        for title, url, source_job_id in (
            ("Tech Lead", "https://studio.example/jobs/tech-lead", "sheet-1"),
            ("Artist", "https://studio.example/jobs/artist", "sheet-2"),
        )
    ]

    resolved, tracked, rejected = _resolve_residual_identity_conflicts(rows)

    assert [row.availabilityId for row in resolved] == [row.availabilityId for row in rows]
    assert not tracked
    assert not rejected
