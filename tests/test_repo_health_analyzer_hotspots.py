"""Regression tests for the refactorability analyzer's hotspot ranking.

The hotspot top-10 must be size-primary (line count descending, score only as a
tiebreak) so the largest files always surface. A score-primary ranking hides
pure-size files like ``canonicalize_google_sheets.py`` behind the many smaller
runtime-named files that pick up the +30 name bonus.
"""

from __future__ import annotations

import pathlib

from tools.repo_health.bin.analyze_refactorability import RefactorabilityAnalyzer


def _write_lines(path: pathlib.Path, count: int) -> None:
    path.write_text("x\n" * count, encoding="utf-8")


def _hotspot_names(analyzer: RefactorabilityAnalyzer) -> list[str]:
    result = analyzer._check_hotspots()  # noqa: SLF001
    return [pathlib.Path(h["file"]).name for h in result["hotspots"]]


def test_hotspot_top10_is_size_primary(tmp_path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    # Three runtime-named files (score 55: 25 size + 30 name bonus)...
    _write_lines(src / "pipeline_runtime.py", 600)
    _write_lines(src / "bridge_app.py", 550)
    _write_lines(src / "domain_orchestrator.py", 520)
    # ...and one much larger pure-size file (score 50: high-risk size only).
    _write_lines(src / "huge_pure_data.py", 1200)

    analyzer = RefactorabilityAnalyzer(str(tmp_path))
    names = _hotspot_names(analyzer)

    # The largest file surfaces first even though its score (50) is lower than
    # the runtime-named files' (55) — regression: a score-primary key hid it.
    assert names[0] == "huge_pure_data.py"
    # The rest follow by descending line count, not by score.
    assert names[1:] == ["pipeline_runtime.py", "bridge_app.py", "domain_orchestrator.py"]


def test_hotspot_ranking_breaks_size_ties_by_score(tmp_path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    # Same line count; the runtime-named file has the higher score (80 vs 50).
    _write_lines(src / "bridge_same_size.py", 1000)
    _write_lines(src / "plain_same_size.py", 1000)

    analyzer = RefactorabilityAnalyzer(str(tmp_path))
    names = _hotspot_names(analyzer)

    assert names == ["bridge_same_size.py", "plain_same_size.py"]


def test_hotspot_top10_caps_large_pools_by_size(tmp_path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    # 20 runtime-named files at 500-519 lines (score 55) plus one 900-line
    # pure-size file (score 50): the biggest file must still be in the top-10.
    for i in range(20):
        _write_lines(src / f"pipeline_module_{i:02}.py", 500 + i)
    _write_lines(src / "largest_pure_data.py", 900)

    analyzer = RefactorabilityAnalyzer(str(tmp_path))
    result = analyzer._check_hotspots()  # noqa: SLF001
    hotspots = result["hotspots"]

    assert len(hotspots) == 10
    locs = [h["loc"] for h in hotspots]
    assert locs == sorted(locs, reverse=True)
    names = [pathlib.Path(h["file"]).name for h in hotspots]
    assert "largest_pure_data.py" in names
    assert names[0] == "largest_pure_data.py"
