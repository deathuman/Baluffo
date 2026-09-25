"""Live source-registry preflight: the surface the seed-only guardrails miss.

The point of the tool is that the live registry is checked at all. These tests
pin that it loads rows the way the pipeline does, that it reuses the existing
predicates rather than reimplementing them, and that advisory-by-default is a
deliberate choice rather than an accident of a missing flag.
"""

from __future__ import annotations

import json
from pathlib import Path

from tools.repo_health import source_registry_preflight as preflight

_MINIMAL_SEED = [{"id": "a", "listing_url": "https://a.example/jobs", "pages": []}]


def _write_seed(data_root: Path, rows: list[dict]) -> None:
    path = data_root / "defaults" / "source-registry-active.seed.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")


def _write_live(data_root: Path, rows: list[dict]) -> None:
    path = data_root / "source-registry-active.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")


def _write_known_collisions(data_root: Path, payload: dict) -> None:
    path = data_root / "defaults" / "source-registry-known-url-collisions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_report_counts_live_and_seed_independently(tmp_path: Path) -> None:
    _write_seed(tmp_path, _MINIMAL_SEED)
    _write_live(
        tmp_path,
        [
            *_MINIMAL_SEED,
            {"id": "b", "listing_url": "https://b.example/jobs", "pages": []},
        ],
    )

    report = preflight.build_report(tmp_path)

    assert report["seedRowCount"] == 1
    assert report["liveRowCount"] == 2
    assert report["liveOnlyRowCount"] == 1
    assert report["liveOnlyRowIds"] == ["b"]
    assert report["seedOnlyRowCount"] == 0


def test_report_surfaces_seed_only_rows(tmp_path: Path) -> None:
    _write_seed(tmp_path, [*_MINIMAL_SEED, {"id": "retired", "listing_url": "https://r.example/"}])
    _write_live(tmp_path, _MINIMAL_SEED)

    report = preflight.build_report(tmp_path)

    # A row in the seed but not live would be resurrected by a seed restore.
    assert report["seedOnlyRowCount"] == 1
    assert report["seedOnlyRowIds"] == ["retired"]


def test_report_flags_structural_defects_in_live_rows(tmp_path: Path) -> None:
    _write_seed(tmp_path, _MINIMAL_SEED)
    _write_live(
        tmp_path,
        [
            {"id": "dup1", "listing_url": "https://t.example/jobs", "pages": []},
            {"id": "dup2", "listing_url": "https://www.t.example/jobs/", "pages": []},
            {"id": "static:listing_url:https://empty.example/", "pages": []},
            {"id": "clash", "listing_url": "https://c.example/jobs", "pages": ["/relative"]},
        ],
    )

    report = preflight.build_report(tmp_path)

    assert report["uncoveredCollisionsLive"] == 1
    assert report["definitionlessRows"]
    assert report["malformedPageRows"]
    assert report["duplicateIdRows"] == []


def test_advisory_by_default_returns_zero_even_with_findings(tmp_path: Path) -> None:
    _write_seed(tmp_path, _MINIMAL_SEED)
    _write_live(
        tmp_path,
        [
            {"id": "dup1", "listing_url": "https://t.example/jobs", "pages": []},
            {"id": "dup2", "listing_url": "https://www.t.example/jobs/", "pages": []},
        ],
    )

    assert preflight.main(["--data-root", str(tmp_path)]) == 0
    assert preflight.main(["--data-root", str(tmp_path), "--strict"]) == 1


def test_missing_live_registry_falls_back_to_the_seed(tmp_path: Path) -> None:
    """A fresh install has no live registry; the seed *is* the live registry.

    The runtime loader resolves the seed transparently, so the preflight must
    inherit that rather than reporting an empty live registry -- otherwise a
    clean checkout would look like a mass deletion of 1,893 sources.
    """
    _write_seed(tmp_path, _MINIMAL_SEED)

    report = preflight.build_report(tmp_path)

    assert report["liveRowCount"] == 1
    assert report["liveOnlyRowCount"] == 0
    assert report["seedOnlyRowCount"] == 0
    assert report["warnings"] == []


def test_malformed_live_registry_is_surfaced_even_though_the_loader_swallows_it(
    tmp_path: Path,
) -> None:
    """A corrupt live registry must not read as a clean seed-only install.

    The runtime loader degrades a corrupt live file to the committed seed
    without complaint, which is correct at runtime and dangerous to hide: the
    operator's live rows vanish and every count still looks plausible.
    """
    _write_seed(tmp_path, _MINIMAL_SEED)
    path = tmp_path / "source-registry-active.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")

    report = preflight.build_report(tmp_path)

    assert any("unreadable" in warning for warning in report["warnings"])
    assert any("NOT the ones on disk" in warning for warning in report["warnings"])
    # A warning is enough; strict mode is what turns it into a failure.
    assert preflight.main(["--data-root", str(tmp_path)]) == 0
    assert preflight.main(["--data-root", str(tmp_path), "--strict"]) == 1


def test_live_registry_of_the_wrong_shape_is_surfaced(tmp_path: Path) -> None:
    _write_seed(tmp_path, _MINIMAL_SEED)
    path = tmp_path / "source-registry-active.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"active": []}), encoding="utf-8")

    report = preflight.build_report(tmp_path)

    assert any("not a JSON array" in warning for warning in report["warnings"])


def test_missing_seed_warns_without_crashing(tmp_path: Path) -> None:
    _write_live(tmp_path, _MINIMAL_SEED)

    report = preflight.build_report(tmp_path)

    assert report["seedRowCount"] == 0
    assert report["warnings"]


def test_malformed_seed_warns_without_crashing(tmp_path: Path) -> None:
    path = tmp_path / "defaults" / "source-registry-active.seed.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")
    _write_live(tmp_path, _MINIMAL_SEED)

    report = preflight.build_report(tmp_path)

    assert report["seedRowCount"] == 0
    assert any("could not read active seed" in warning for warning in report["warnings"])


def test_clean_live_registry_reports_no_findings(tmp_path: Path) -> None:
    rows = [{"id": "solo", "listing_url": "https://solo.example/jobs", "pages": []}]
    _write_seed(tmp_path, rows)
    _write_live(tmp_path, rows)

    report = preflight.build_report(tmp_path)

    assert preflight._has_structural_defects(report) == []
    assert report["warnings"] == []


def test_text_output_reports_advisory_not_failure(tmp_path: Path, capsys) -> None:
    rows = [
        {"id": "dup1", "listing_url": "https://t.example/jobs", "pages": []},
        {"id": "dup2", "listing_url": "https://www.t.example/jobs/", "pages": []},
    ]
    _write_seed(tmp_path, _MINIMAL_SEED)
    _write_live(tmp_path, rows)

    preflight.main(["--data-root", str(tmp_path)])
    printed = capsys.readouterr().out

    assert "uncovered duplicate-URL groups" in printed
    assert "advisory" in printed
    assert "pass --strict" in printed


def test_worksheet_groups_items_by_decision_and_leaves_them_undecided(
    tmp_path: Path, capsys
) -> None:
    _write_seed(tmp_path, _MINIMAL_SEED)
    # An empty baseline present in the data root keeps the fixture hermetic
    # instead of inheriting the repository's seven reviewed entries, all of
    # which are stale against a two-row fixture.
    _write_known_collisions(tmp_path, {})
    _write_live(
        tmp_path,
        [
            *_MINIMAL_SEED,
            {"id": "dup1", "listing_url": "https://t.example/jobs", "pages": []},
            {"id": "dup2", "listing_url": "https://www.t.example/jobs/", "pages": []},
            {"id": "static:listing_url:https://empty.example/", "pages": []},
        ],
    )
    target = tmp_path / "out" / "worksheet.json"

    preflight.main(["--data-root", str(tmp_path), "--worksheet", str(target)])
    capsys.readouterr()
    payload = json.loads(target.read_text(encoding="utf-8"))

    kinds = {item["kind"] for item in payload["items"]}
    assert "uncovered_duplicate_url" in kinds
    assert "definitionless_static_row" in kinds
    # This fixture keeps the seed row live, so there is nothing seed-only and
    # nothing stale to adjudicate; the other worksheet test covers those kinds.
    assert "seed_only_row" not in kinds
    assert "stale_baseline_entry" not in kinds
    # The worksheet records what is outstanding; it must not decide for the
    # operator, and every item must be individually adjudicable.
    assert all(item["disposition"] == "undecided" for item in payload["items"])
    assert all(item["note"] == "" and item["decidedBy"] == "" for item in payload["items"])
    assert all(item["evidence"] for item in payload["items"])
    assert payload["summary"]["undecidedCount"] == len(payload["items"])


def test_baseline_is_resolved_from_the_data_root_not_the_repo(tmp_path: Path) -> None:
    """Another data root must be judged by its own baseline, not the repo's."""
    _write_known_collisions(tmp_path, {"t.example/jobs": "reviewed twin"})
    rows = [
        *_MINIMAL_SEED,
        {"id": "dup1", "listing_url": "https://t.example/jobs", "pages": []},
        {"id": "dup2", "listing_url": "https://www.t.example/jobs/", "pages": []},
    ]
    _write_seed(tmp_path, rows)
    _write_live(tmp_path, rows)

    report = preflight.build_report(tmp_path)

    # The repo baseline would not cover t.example/jobs, so 0 proves the
    # data-root copy was used.
    assert report["uncoveredCollisionsLive"] == 0
    assert report["staleBaselineEntries"] == []


def test_worksheet_lists_seed_only_and_live_only_rows(tmp_path: Path, capsys) -> None:
    _write_seed(tmp_path, [*_MINIMAL_SEED, {"id": "retired", "listing_url": "https://r.example/"}])
    _write_live(
        tmp_path,
        [*_MINIMAL_SEED, {"id": "fresh", "listing_url": "https://f.example/jobs", "pages": []}],
    )
    target = tmp_path / "worksheet.json"

    preflight.main(["--data-root", str(tmp_path), "--worksheet", str(target)])
    capsys.readouterr()
    payload = json.loads(target.read_text(encoding="utf-8"))

    by_kind: dict[str, set[str]] = {}
    for item in payload["items"]:
        by_kind.setdefault(item["kind"], set()).add(item["sourceId"])
    assert by_kind["seed_only_row"] == {"retired"}
    assert by_kind["live_only_row"] == {"fresh"}
