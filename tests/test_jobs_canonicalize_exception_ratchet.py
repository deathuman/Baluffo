from __future__ import annotations

import pytest

from src.jobs import canonicalize


def _raw_job() -> dict[str, object]:
    return {
        "title": "Technical Artist",
        "company": "Studio A",
        "city": "Amsterdam",
        "country": "NL",
        "workType": "Remote",
        "contractType": "Full-time",
        "jobLink": "https://example.com/jobs/technical-artist?utm_source=x",
        "sector": "Game",
    }


def test_canonicalize_job_preserves_link_when_redirect_resolution_fails() -> None:
    def fail_redirect(_url: str) -> str:
        raise RuntimeError("redirect resolver unavailable")

    job = canonicalize.canonicalize_job(
        _raw_job(),
        source="unit",
        fetched_at="2026-03-13T10:00:00+00:00",
        resolve_redirect_url=fail_redirect,
    )

    assert job is not None
    assert job.jobLink == "https://example.com/jobs/technical-artist"


def test_canonicalize_job_does_not_swallow_unexpected_redirect_failure() -> None:
    def fail_redirect(_url: str) -> str:
        raise AssertionError("unexpected redirect resolver bug")

    with pytest.raises(AssertionError, match="unexpected redirect resolver bug"):
        canonicalize.canonicalize_job(
            _raw_job(),
            source="unit",
            fetched_at="2026-03-13T10:00:00+00:00",
            resolve_redirect_url=fail_redirect,
        )
