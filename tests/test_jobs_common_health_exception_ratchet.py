from datetime import UTC, datetime, timedelta

import pytest

from src.jobs.common.health import get_quarantined_sources


class _BrokenQuarantineValue:
    def replace(self, *_args: object, **_kwargs: object) -> str:
        raise RuntimeError("unexpected quarantine value bug")


def test_get_quarantined_sources_ignores_malformed_quarantine_dates() -> None:
    assert (
        get_quarantined_sources(
            {
                "bad-date": {"quarantinedUntilAt": "not-a-date"},
                "bad-type": {"quarantinedUntilAt": 123},
            }
        )
        == []
    )


def test_get_quarantined_sources_preserves_active_quarantine() -> None:
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()

    assert get_quarantined_sources(
        {
            "static-source": {
                "quarantinedUntilAt": future,
                "consecutiveFailures": 2,
                "consecutiveZeroKept": 3,
            }
        }
    ) == [
        {
            "name": "static-source",
            "quarantinedUntilAt": future,
            "reason": "consecutive_zero_kept",
            "consecutiveFailures": 2,
            "consecutiveZeroKept": 3,
        }
    ]


def test_get_quarantined_sources_does_not_hide_unexpected_state_failures() -> None:
    with pytest.raises(RuntimeError, match="unexpected quarantine value bug"):
        get_quarantined_sources({"broken": {"quarantinedUntilAt": _BrokenQuarantineValue()}})
