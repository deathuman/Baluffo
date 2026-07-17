import json
from datetime import UTC, datetime
from pathlib import Path

from src.jobs.availability_identity import (
    read_identity_quarantine,
    reconcile_identity_quarantine,
    write_identity_quarantine,
)


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


def test_identity_quarantine_v2_writes_bounded_metadata_and_reads_v1(tmp_path: Path) -> None:
    path = tmp_path / "jobs-availability-identity-quarantine.json"
    write_identity_quarantine(
        path,
        {
            "unresolved_1": {
                "kind": "unresolved_candidate_group",
                "detectedAt": "2026-07-17T00:00:00+00:00",
                "sourceAliasFingerprint": "abc123",
            }
        },
        updated_at="2026-07-17T00:00:00+00:00",
        truncated_count=2,
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schemaVersion"] == 2
    assert payload["rowCount"] == 1
    assert payload["truncatedCount"] == 2
    assert read_identity_quarantine(path)["unresolved_1"]["sourceAliasFingerprint"] == "abc123"

    path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "rows": {
                    "legacy": {
                        "detectedAt": "2026-07-16T00:00:00+00:00",
                        "reason": "cross_url_identity_collision",
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    assert set(read_identity_quarantine(path)) == {"legacy"}


def test_identity_quarantine_reports_deterministic_truncation() -> None:
    entries = {
        f"row_{index:04d}": {
            "detectedAt": f"2026-07-17T00:{index // 60:02d}:{index % 60:02d}+00:00"
        }
        for index in range(2_002)
    }
    stats: dict[str, int] = {}

    rows = reconcile_identity_quarantine(
        entries,
        {},
        now=datetime(2026, 7, 17, tzinfo=UTC),
        stats=stats,
    )

    assert len(rows) == 2_000
    assert stats["quarantineTruncatedCount"] == 2
