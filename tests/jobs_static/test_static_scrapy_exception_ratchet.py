from __future__ import annotations

from concurrent.futures import Future
from unittest import mock

import pytest

from ._helpers import Path, static_scrapy


def _source(name: str) -> dict[str, object]:
    return {
        "name": name,
        "studio": name,
        "adapter": "scrapy_static",
        "pages": ["https://example.com/jobs"],
    }


def test_scrapy_static_entry_records_expected_runner_failure() -> None:
    with mock.patch.object(
        static_scrapy,
        "_run_runner_envelope",
        side_effect=ValueError("bad runner envelope"),
    ):
        rows, detail, errors = static_scrapy._run_scrapy_static_source_entry(
            _source("Scrapy Expected Failure Studio"),
            runner_path=Path("runner.py"),
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )

    assert rows == []
    assert detail["status"] == "error"
    assert detail["classification"] == "parse_error"
    assert "bad runner envelope" in detail["error"]
    assert errors == ["Scrapy Expected Failure Studio: ValueError: bad runner envelope"]


def test_scrapy_static_entry_does_not_hide_unexpected_runner_bug() -> None:
    with (
        mock.patch.object(
            static_scrapy,
            "_run_runner_envelope",
            side_effect=AssertionError("unexpected scrapy runner bug"),
        ),
        pytest.raises(AssertionError, match="unexpected scrapy runner bug"),
    ):
        static_scrapy._run_scrapy_static_source_entry(
            _source("Scrapy Unexpected Failure Studio"),
            runner_path=Path("runner.py"),
            timeout_s=5,
            retries=0,
            backoff_s=0,
        )


def test_future_scrapy_result_records_expected_worker_failure() -> None:
    future: Future[tuple[list[dict[str, object]], dict[str, object], list[str]]] = Future()
    future.set_exception(ValueError("bad future envelope"))

    rows, detail, errors = static_scrapy._future_scrapy_result(
        future, _source("Scrapy Future Expected Studio")
    )

    assert rows == []
    assert detail["status"] == "error"
    assert detail["classification"] == "parse_error"
    assert "bad future envelope" in detail["error"]
    assert errors == ["Scrapy Future Expected Studio: ValueError: bad future envelope"]


def test_future_scrapy_result_does_not_hide_unexpected_worker_bug() -> None:
    future: Future[tuple[list[dict[str, object]], dict[str, object], list[str]]] = Future()
    future.set_exception(AssertionError("unexpected future worker bug"))

    with pytest.raises(AssertionError, match="unexpected future worker bug"):
        static_scrapy._future_scrapy_result(future, _source("Scrapy Future Unexpected Studio"))
