from __future__ import annotations

import builtins
import io
import json
from typing import Any

import pytest

from src.scrapers import runner as scrapy_runner


def _validated_payload() -> dict[str, Any]:
    return {
        "source": {
            "name": "Scrapy Test Studio",
            "studio": "Scrapy Test Studio",
            "pages": ["https://example.com/jobs"],
        },
        "runtime": {"timeout_s": 5, "retries": 0, "backoff_s": 0.0},
    }


def test_emit_envelope_reports_expected_json_serialization_failure(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    real_dumps = scrapy_runner.json.dumps
    calls = {"count": 0}

    def fail_first_dumps(payload: object, **kwargs: object) -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            raise TypeError("cannot serialize envelope")
        return real_dumps(payload, **kwargs)

    monkeypatch.setattr(scrapy_runner.json, "dumps", fail_first_dumps)

    scrapy_runner._emit_envelope(
        {
            "ok": True,
            "jobs": [],
            "details": [{"name": "Source A", "studio": "Studio A"}],
        }
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["details"][0]["name"] == "Source A"
    assert "Envelope serialization failed: cannot serialize envelope" in payload["partialErrors"]


def test_emit_envelope_does_not_swallow_unexpected_json_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_dumps(_payload: object, **_kwargs: object) -> str:
        raise AssertionError("unexpected json bug")

    monkeypatch.setattr(scrapy_runner.json, "dumps", fail_dumps)

    with pytest.raises(AssertionError, match="unexpected json bug"):
        scrapy_runner._emit_envelope({"ok": True, "jobs": [], "details": []})


def test_run_scrapy_reports_missing_scrapy_import(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "scrapy.crawler":
            raise ImportError("scrapy unavailable")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    payload = scrapy_runner._run_scrapy(_validated_payload())

    assert payload["ok"] is False
    assert payload["details"][0]["classification"] == "parse_error"
    assert "Scrapy import failed: scrapy unavailable" in payload["partialErrors"]


def test_run_scrapy_does_not_swallow_unexpected_scrapy_import_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "scrapy.crawler":
            raise RuntimeError("unexpected import bug")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError, match="unexpected import bug"):
        scrapy_runner._run_scrapy(_validated_payload())


def test_main_reports_invalid_stdin_json(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(scrapy_runner.sys, "stdin", io.StringIO("{not-json"))

    assert scrapy_runner.main() == 1

    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert "Failed to parse stdin JSON:" in payload["partialErrors"][0]


def test_main_does_not_swallow_unexpected_json_loader_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_load(_handle: object) -> object:
        raise AssertionError("unexpected loader bug")

    monkeypatch.setattr(scrapy_runner.json, "load", fail_load)

    with pytest.raises(AssertionError, match="unexpected loader bug"):
        scrapy_runner.main()
