from __future__ import annotations

from unittest import mock

import pytest

from src.scrapers.providers import jobylon_v1


def test_jobylon_expected_parse_failure_remains_provider_error() -> None:
    with (
        mock.patch.object(jobylon_v1, "_http_text", return_value="<html></html>"),
        mock.patch.object(
            jobylon_v1,
            "extract_jobylon_company_id",
            side_effect=ValueError("bad company id"),
        ),
    ):
        jobs, stats, errors, reject_reasons = jobylon_v1.extract_jobylon_v1_jobs(
            source_name="Remedy",
            studio="Remedy Entertainment",
            page_url="https://example.test/careers",
            timeout_s=20,
        )

    assert jobs == []
    assert stats["jobs_emitted"] == 0
    assert reject_reasons == {}
    assert errors == ["Remedy: jobylon_v1 parse failed: bad company id"]


def test_jobylon_unexpected_parse_failure_propagates() -> None:
    with (
        mock.patch.object(jobylon_v1, "_http_text", return_value="<html></html>"),
        mock.patch.object(
            jobylon_v1,
            "extract_jobylon_company_id",
            side_effect=RuntimeError("unexpected parser bug"),
        ),
    ):
        with pytest.raises(RuntimeError, match="unexpected parser bug"):
            jobylon_v1.extract_jobylon_v1_jobs(
                source_name="Remedy",
                studio="Remedy Entertainment",
                page_url="https://example.test/careers",
                timeout_s=20,
            )
