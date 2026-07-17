from datetime import UTC, datetime

from src.jobs.availability_identity import reconcile_identity_quarantine


def test_identity_quarantine_prunes_expired_entries() -> None:
    rows = reconcile_identity_quarantine(
        {
            "expired": {"detectedAt": "2026-06-01T00:00:00+00:00"},
            "current": {"detectedAt": "2026-07-10T00:00:00+00:00"},
        },
        {},
        now=datetime(2026, 7, 16, tzinfo=UTC),
    )

    assert set(rows) == {"current"}


def test_identity_quarantine_addition_replaces_prior_compact_evidence() -> None:
    rows = reconcile_identity_quarantine(
        {"availability_old": {"detectedAt": "2026-07-01T00:00:00+00:00"}},
        {
            "availability_old": {
                "detectedAt": "2026-07-16T00:00:00+00:00",
                "reason": "cross_url_identity_collision",
                "replacementAvailabilityIds": ["availability_new"],
            }
        },
        now=datetime(2026, 7, 16, tzinfo=UTC),
    )

    assert rows["availability_old"]["replacementAvailabilityIds"] == ["availability_new"]
