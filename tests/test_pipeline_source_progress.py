from __future__ import annotations

import builtins

import pytest

from src.jobs import pipeline_source_progress


class _StdoutWithEncoding:
    def __init__(self, encoding: str) -> None:
        self.encoding = encoding


def test_console_safe_text_uses_stdout_encoding_when_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stdout", _StdoutWithEncoding("cp1252"))

    assert pipeline_source_progress.console_safe_text("boom \U0001f4a5") == "boom \\U0001f4a5"


def test_console_safe_text_falls_back_for_unknown_stdout_encoding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stdout", _StdoutWithEncoding("not-a-codec"))

    assert pipeline_source_progress.console_safe_text("boom \U0001f4a5") == "boom \\U0001f4a5"


def test_console_safe_text_does_not_swallow_unexpected_encoding_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BadText:
        def encode(self, *_args, **_kwargs):  # noqa: ANN202
            raise RuntimeError("unexpected encode failure")

    def fake_str(value):  # noqa: ANN202
        if value == "bad-text":
            return BadText()
        return builtins.str(value)

    monkeypatch.setattr("sys.stdout", _StdoutWithEncoding("utf-8"))
    monkeypatch.setattr(pipeline_source_progress, "str", fake_str, raising=False)

    with pytest.raises(RuntimeError, match="unexpected encode failure"):
        pipeline_source_progress.console_safe_text("bad-text")
