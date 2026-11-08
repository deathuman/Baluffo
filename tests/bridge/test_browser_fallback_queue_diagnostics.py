"""Ops diagnostics for legacy browser-fallback queue hygiene."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import src.jobs.common.browser_fallback_queue_diagnostics as diagnostics
from src.bridge.routes import get_ops_diagnostics
from src.jobs.common.browser_fallback_queue_diagnostics import (
    LEGACY_BROWSER_FALLBACK_QUEUE_FILENAME,
    build_browser_fallback_queue_diagnostics,
)

PAGE = "https://same-board.example/careers"


class _RecordingHandler:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def send_json(self, payload: dict[str, Any]) -> None:
        self.payloads.append(payload)


def _write_queue(directory: Path, rows: Any) -> Path:
    path = directory / LEGACY_BROWSER_FALLBACK_QUEUE_FILENAME
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return path


def _queue_row(source_id: str, *, classification: str = "blocked_or_challenge") -> dict[str, Any]:
    return {
        "adapter": "scrapy_static",
        "sourceId": source_id,
        "name": f"Same Board ({source_id})",
        "studio": "Same Board",
        "page": PAGE,
        "classification": classification,
        "reason": "HTTP 403" if classification != "parse_error" else "no jobs extracted",
    }


def test_diagnostics_flag_duplicate_pages_and_reader_collapse(tmp_path: Path) -> None:
    queue = _write_queue(tmp_path, [_queue_row("static:sheet"), _queue_row("static:map")])

    payload = build_browser_fallback_queue_diagnostics(queue)

    assert payload["ok"] is True
    assert payload["readable"] is True
    assert payload["rowCount"] == 2
    assert payload["eligibleRowCount"] == 2
    assert payload["eligibleUniquePageCount"] == 1
    assert payload["duplicatePageCount"] == 1
    assert payload["duplicateRowCount"] == 2
    assert payload["duplicateExcessRowCount"] == 1
    assert payload["eligibleDuplicatePageCount"] == 1
    assert payload["readerCollapsedRowCount"] == 1
    assert payload["issueCount"] == 1
    assert payload["clean"] is False
    entry = payload["duplicatePages"][0]
    assert entry["page"] == PAGE
    assert entry["sourceIds"] == ["static:sheet", "static:map"]
    assert entry["readerKeptSourceId"] == "static:sheet"
    assert entry["readerCollapsedRowCount"] == 1


def test_ineligible_duplicate_page_is_reported_without_reader_collapse(tmp_path: Path) -> None:
    queue = _write_queue(
        tmp_path,
        [
            _queue_row("static:parser", classification="parse_error"),
            _queue_row("static:timeout", classification="timeout"),
        ],
    )

    payload = build_browser_fallback_queue_diagnostics(queue)

    # The page is repeated in the artifact, but the reader would already skip the
    # parse_error row, so nothing is actually collapsed for the next run.
    assert payload["duplicatePageCount"] == 1
    assert payload["duplicateExcessRowCount"] == 1
    assert payload["eligibleDuplicatePageCount"] == 0
    assert payload["readerCollapsedRowCount"] == 0
    entry = payload["duplicatePages"][0]
    assert entry["eligibleRowCount"] == 1
    assert entry["readerKeptSourceId"] == "static:timeout"


def test_diagnostics_flag_non_allowlisted_parse_error_rows(tmp_path: Path) -> None:
    queue = _write_queue(
        tmp_path,
        [
            _queue_row("static:parser", classification="parse_error"),
            _queue_row("static:timeout", classification="timeout"),
        ],
    )

    payload = build_browser_fallback_queue_diagnostics(queue)

    assert "parse_error" not in payload["allowlistedClassifications"]
    assert payload["parseErrorRowCount"] == 1
    assert payload["parseErrorRows"] == [
        {
            "page": PAGE,
            "sourceId": "static:parser",
            "name": "Same Board (static:parser)",
            "classification": "parse_error",
            "reason": "no jobs extracted",
        }
    ]
    assert payload["eligibleRowCount"] == 1
    assert payload["clean"] is False


def test_diagnostics_report_a_clean_queue(tmp_path: Path) -> None:
    queue = _write_queue(tmp_path, [_queue_row("static:sheet")])

    payload = build_browser_fallback_queue_diagnostics(queue)

    assert payload["clean"] is True
    assert payload["issueCount"] == 0
    assert payload["duplicatePages"] == []
    assert payload["parseErrorRows"] == []
    assert payload["warnings"] == []


def test_diagnostics_report_a_missing_queue(tmp_path: Path) -> None:
    payload = build_browser_fallback_queue_diagnostics(tmp_path / "missing.json")

    assert payload["ok"] is True
    assert payload["exists"] is False
    assert payload["readable"] is False
    assert payload["warnings"] == ["missing"]


def test_diagnostics_report_unreadable_queue_payloads(tmp_path: Path) -> None:
    invalid_json = tmp_path / "invalid.json"
    invalid_json.write_text("{not json", encoding="utf-8")
    payload = build_browser_fallback_queue_diagnostics(invalid_json)
    assert payload["readable"] is False
    assert "invalid_json" in payload["warnings"]

    wrapped = _write_queue(tmp_path, {"rows": []})
    payload = build_browser_fallback_queue_diagnostics(wrapped)
    assert payload["readable"] is False
    assert "invalid_payload" in payload["warnings"]


def test_diagnostics_bound_the_row_lists(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.setattr(diagnostics, "MAX_DIAGNOSTIC_ROWS", 1)
    rows: list[dict[str, Any]] = []
    for index in range(2):
        page = f"https://board{index}.example/careers"
        rows.extend(
            [
                {
                    "sourceId": f"static:{index}a",
                    "page": page,
                    "classification": "blocked_or_challenge",
                },
                {
                    "sourceId": f"static:{index}b",
                    "page": page,
                    "classification": "blocked_or_challenge",
                },
                {
                    "sourceId": f"static:parse{index}",
                    "page": f"https://parser{index}.example/careers",
                    "classification": "parse_error",
                    "reason": "no jobs extracted",
                },
            ]
        )
    queue = _write_queue(tmp_path, rows)

    payload = build_browser_fallback_queue_diagnostics(queue)

    assert payload["duplicatePageCount"] == 2
    assert len(payload["duplicatePages"]) == 1
    assert payload["truncated"]["duplicatePages"] is True
    assert payload["parseErrorRowCount"] == 2
    assert len(payload["parseErrorRows"]) == 1
    assert payload["truncated"]["parseErrorRows"] is True


def test_ops_route_serves_browser_fallback_queue_diagnostics(tmp_path: Path) -> None:
    _write_queue(
        tmp_path,
        [
            _queue_row("static:sheet"),
            _queue_row("static:map"),
            _queue_row("static:parser", classification="parse_error"),
        ],
    )
    api = SimpleNamespace(
        runtime_config=SimpleNamespace(data_dir=str(tmp_path)),
        JOBS_FETCH_REPORT_PATH=tmp_path / "jobs-fetch-report.json",
    )
    handler = _RecordingHandler()

    handled = get_ops_diagnostics.handle_ops_diagnostic_routes(
        cast(Any, handler),
        api=cast(Any, api),
        path="/ops/browser-fallback-queue",
        query={},
    )

    assert handled is True
    assert len(handler.payloads) == 1
    payload = handler.payloads[0]
    assert payload["readable"] is True
    assert payload["duplicatePageCount"] == 1
    assert payload["parseErrorRowCount"] == 1
