from __future__ import annotations

import argparse
from pathlib import Path
from typing import cast

import pytest

from src.jobs.interfaces import SourceLoader
from src.jobs.pipeline_cli import OnlySourcesSelectionError, _only_sources_selection


def _loaders() -> list[tuple[str, SourceLoader]]:
    return [(name, cast(SourceLoader, lambda **_: [])) for name in ("alpha", "beta", "gamma")]


def _args(*, comma: str = "", file: str = "") -> argparse.Namespace:
    return argparse.Namespace(only_sources=comma, only_sources_file=file)


def test_only_sources_file_selects_exact_loaders(tmp_path: Path) -> None:
    path = tmp_path / "retry-sources.txt"
    path.write_text("beta\nalpha\n", encoding="utf-8")

    selected, seed, exclusions, forced = _only_sources_selection(_args(file=str(path)), _loaders())

    assert [name for name, _loader in selected or []] == ["alpha", "beta"]
    assert seed is True
    assert forced is True
    assert [(row["name"], row["error"]) for row in exclusions] == [("gamma", "only_sources_filter")]


def test_only_sources_file_merges_with_inline_list(tmp_path: Path) -> None:
    path = tmp_path / "retry-sources.txt"
    path.write_text("beta\n", encoding="utf-8")

    selected, *_rest = _only_sources_selection(_args(comma="alpha", file=str(path)), _loaders())

    assert [name for name, _loader in selected or []] == ["alpha", "beta"]


def test_missing_only_sources_file_fails_loudly(tmp_path: Path) -> None:
    with pytest.raises(OnlySourcesSelectionError, match="Cannot read --only-sources-file"):
        _only_sources_selection(_args(file=str(tmp_path / "missing.txt")), _loaders())


def test_empty_only_sources_file_fails_loudly(tmp_path: Path) -> None:
    path = tmp_path / "empty-sources.txt"
    path.write_text("\n\n", encoding="utf-8")

    with pytest.raises(OnlySourcesSelectionError, match="listed no source loader names"):
        _only_sources_selection(_args(file=str(path)), _loaders())
