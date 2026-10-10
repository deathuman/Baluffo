"""S2 per-source HTTP counters: what they count, and what they must not change.

Three properties carry the weight:

* with counting off the wrapper is the *same object*, not an equivalent one, so the
  disabled path is the pre-change code rather than a no-op wrapper on the hot fetch path;
* the wrapper never swallows or rewraps an exception, because it sits under
  ``fetch_with_retries``, whose retry decision depends on the exception it raises;
* the counters reach the *persisted* fetch report, since the point of S2 is that "why is
  this source slow" is answerable from one report rather than from a debug build.

The size figures are pinned as UTF-8 rather than character counts: the seam yields
decoded text, and reporting characters as bytes would understate every non-ASCII board.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

import pytest

from src import jobs_fetcher as jf
from src.jobs import fetch_counters
from src.jobs.common.http import HttpStatusError
from src.shared.json_io import read_json
from tests.helpers.temp_paths import workspace_tmpdir

SOURCE_NAME = "counter_source"


def test_counters_are_off_by_default_and_the_off_path_is_the_same_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(fetch_counters.COUNTERS_ENV, raising=False)
    importlib.reload(fetch_counters)

    assert fetch_counters.COUNTERS_ENABLED is False
    assert isinstance(fetch_counters.COUNTERS_ENABLED, bool)
    assert fetch_counters.start_source_counters() is None

    def fetch(_url: str, _timeout_s: int) -> str:
        return "x"

    # Identity rather than equivalence: nothing is allocated or called on the off path.
    assert fetch_counters.wrap_fetch_text(fetch, None) is fetch

    monkeypatch.setenv(fetch_counters.COUNTERS_ENV, "1")
    importlib.reload(fetch_counters)
    assert fetch_counters.COUNTERS_ENABLED is True
    assert fetch_counters.start_source_counters() is not None

    # reload re-executes the module in place, so the restored constant is the one every
    # other module already holds a reference to.
    monkeypatch.delenv(fetch_counters.COUNTERS_ENV, raising=False)
    importlib.reload(fetch_counters)
    assert fetch_counters.COUNTERS_ENABLED is False


def test_counters_classify_attempts_status_and_size() -> None:
    counters = fetch_counters.SourceHttpCounters()
    bodies = {"https://example.test/a": "ok", "https://example.test/b": "café"}

    def fake(url: str, _timeout_s: int) -> str:
        if url in bodies:
            return bodies[url]
        if url.endswith("/rate-limited"):
            raise RuntimeError(f"HTTP 429 for {url}")
        raise HttpStatusError(404, url)

    wrapped = fetch_counters.wrap_fetch_text(fake, counters)

    assert wrapped("https://example.test/a", 5) == "ok"
    assert wrapped("https://example.test/a", 5) == "ok"
    assert wrapped("https://example.test/b", 5) == "café"
    with pytest.raises(RuntimeError):
        wrapped("https://example.test/rate-limited", 5)
    with pytest.raises(HttpStatusError):
        wrapped("https://example.test/missing", 5)

    payload = counters.as_report()
    assert payload is not None
    assert payload["attempts"] == 5
    assert payload["succeeded"] == 3
    assert payload["failed"] == 2
    assert payload["distinctUrls"] == 4
    # One repeat fetch of the same URL, which is the most this seam can see.
    assert payload["repeatFetches"] == 1
    assert payload["statusCodes"] == {"404": 1, "429": 1}
    assert payload["errorClasses"] == {"HttpStatusError": 1, "RuntimeError": 1}
    assert payload["decodedChars"] == 4 + len("café")
    assert payload["textBytes"] == 4 + len("café".encode())
    assert payload["textBytes"] > payload["decodedChars"]


def test_wrapper_never_swallows_or_rewraps() -> None:
    counters = fetch_counters.SourceHttpCounters()
    text = "body"
    original = RuntimeError("boom")

    def ok(_url: str, _timeout_s: int) -> str:
        return text

    def bad(_url: str, _timeout_s: int) -> str:
        raise original

    assert fetch_counters.wrap_fetch_text(ok, counters)("https://example.test/", 5) is text
    with pytest.raises(RuntimeError) as caught:
        fetch_counters.wrap_fetch_text(bad, counters)("https://example.test/", 5)
    assert caught.value is original


def test_wrapper_forwards_the_request_keyword_to_the_seam() -> None:
    """`transport.make_fetch_text` calls the seam as `(url, timeout, request=...)`."""

    counters = fetch_counters.SourceHttpCounters()
    sentinel = object()
    seen: dict[str, Any] = {}

    def inner(_url: str, timeout_s: int, *, request: object | None = None) -> str:
        seen["request"] = request
        seen["timeout_s"] = timeout_s
        return "body"

    wrapped = fetch_counters.wrap_fetch_text(inner, counters)

    assert wrapped("https://example.test/headers", 7, request=sentinel) == "body"
    assert seen == {"request": sentinel, "timeout_s": 7}
    payload = counters.as_report()
    assert payload is not None
    assert payload["succeeded"] == 1


def test_folding_counters_is_additive_and_silent_when_empty() -> None:
    report: dict[str, Any] = {"name": SOURCE_NAME, "status": "ok"}

    assert fetch_counters.fold_source_counters(report, None) is None
    assert "httpCounters" not in report

    assert fetch_counters.fold_source_counters(report, fetch_counters.SourceHttpCounters()) is None
    assert "httpCounters" not in report

    counted = fetch_counters.SourceHttpCounters()
    counted.record_attempt("https://example.test/listing")
    counted.record_success("listing")
    payload = fetch_counters.fold_source_counters(report, counted)

    assert payload == counted.as_report()
    assert report["httpCounters"] == payload
    assert report["name"] == SOURCE_NAME
    assert report["status"] == "ok"
    assert set(report) == {"name", "status", "httpCounters"}


def _run_counter_pipeline(tmp: Path) -> dict[str, Any]:
    """One real pipeline run whose transport is a fake, so nothing touches the network."""

    def fake_fetch(url: str, _timeout_s: int) -> str:
        if url.endswith("/listing"):
            return "listing"
        raise RuntimeError(f"unhandled URL in fake fetch: {url}")

    def loader(**kwargs: Any) -> list[Any]:
        fetch = kwargs["fetch_text"]
        assert fetch("https://example.test/listing", 5) == "listing"
        assert fetch("https://example.test/listing", 5) == "listing"
        with pytest.raises(RuntimeError):
            fetch("https://example.test/missing", 5)
        return []

    return dict(
        jf.run_pipeline(
            output_dir=Path(tmp),
            fetch_text=fake_fetch,
            source_loaders=[(SOURCE_NAME, loader)],
            retries=0,
            backoff_s=0,
        )
    )


def _persisted_source_rows(tmp: Path) -> list[dict[str, Any]]:
    """The run's own source rows, read back from every report-shaped artifact it wrote."""

    rows: list[dict[str, Any]] = []
    for path in sorted(Path(tmp).rglob("*report*.json")):
        payload = read_json(path, {})
        sources = payload.get("sources") if isinstance(payload, dict) else None
        if not isinstance(sources, list):
            continue
        rows.extend(
            source
            for source in sources
            if isinstance(source, dict) and source.get("name") == SOURCE_NAME
        )
    return rows


def test_pipeline_source_row_carries_http_counters(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fetch_counters, "COUNTERS_ENABLED", True)

    with workspace_tmpdir("jobs-fetcher-http-counters") as tmp:
        report = _run_counter_pipeline(tmp)
        row = next(entry for entry in report["sources"] if entry.get("name") == SOURCE_NAME)
        assert row["name"] == SOURCE_NAME
        counters = row["httpCounters"]
        assert counters["attempts"] == 3
        assert counters["succeeded"] == 2
        assert counters["failed"] == 1
        assert counters["repeatFetches"] == 1
        assert counters["distinctUrls"] == 2
        assert counters["errorClasses"] == {"RuntimeError": 1}
        assert counters["decodedChars"] == 2 * len("listing")

        persisted = _persisted_source_rows(tmp)

    assert persisted, "the run must persist a source row for the loader it ran"
    assert any("httpCounters" in entry for entry in persisted), (
        "httpCounters must survive to the persisted fetch report, not only the in-memory one"
    )


def test_pipeline_source_row_omits_http_counters_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fetch_counters, "COUNTERS_ENABLED", False)

    with workspace_tmpdir("jobs-fetcher-http-counters-off") as tmp:
        report = _run_counter_pipeline(tmp)
        row = next(entry for entry in report["sources"] if entry.get("name") == SOURCE_NAME)
        persisted = _persisted_source_rows(tmp)

    assert "httpCounters" not in row
    assert not any("httpCounters" in entry for entry in persisted)
