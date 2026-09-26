"""Preflight seed/store split consistency (2026-09-26).

The registry is stored as a lean core plus a metadata map. A row written
through the wrong read path loses its payload from the metadata map while its
lean record survives, so the row still exists and still reports ok while having
nothing left to fetch. Before this check no surface could see that: the
commit-time guards read the seed, where such a row is healthy, and the runtime
report had no flag for it.

These cases are split from the structural preflight tests because they are a
different failure class with different fixes.
"""

from __future__ import annotations

import json
from pathlib import Path

from tools.repo_health import source_registry_preflight as preflight


def _write_seed(data_root: Path, rows: list[dict]) -> None:
    path = data_root / "defaults" / "source-registry-active.seed.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")


def _write_live(data_root: Path, rows: list[dict]) -> None:
    path = data_root / "source-registry-active.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")


def _write_pending(data_root: Path, rows: list[dict]) -> None:
    path = data_root / "source-registry-pending.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")


def test_split_consistency_reports_a_payload_stripped_row(tmp_path: Path) -> None:
    """A store row that lost its payload while the seed kept it.

    This is the shape a live Miniclip row sat in for weeks: present, reporting
    ok, and fetching nothing, with no guard able to see it because every
    structural check read the seed.
    """
    _write_seed(
        tmp_path,
        [
            {
                "id": "static:listing_url:https://a.example/jobs",
                "registryState": "active",
                "listing_url": "https://a.example/jobs",
                "pages": ["https://a.example/jobs"],
            }
        ],
    )
    _write_live(
        tmp_path,
        [{"id": "static:listing_url:https://a.example/jobs", "registryState": "active"}],
    )

    report = preflight.build_report(tmp_path)

    assert report["splitConsistency"]["stub_row_repairable"] == [
        "static:listing_url:https://a.example/jobs"
    ]
    # It is also the definition-less shape, so both surfaces must agree.
    assert report["definitionlessRows"]


def test_split_consistency_separates_a_lost_row_from_a_repairable_one(tmp_path: Path) -> None:
    _write_seed(
        tmp_path,
        [
            {
                "id": "static:listing_url:https://stripped.example/jobs",
                "registryState": "active",
                "listing_url": "https://stripped.example/jobs",
            },
            {
                "id": "static:listing_url:https://gone.example/jobs",
                "registryState": "active",
                "listing_url": "https://gone.example/jobs",
            },
        ],
    )
    _write_live(
        tmp_path,
        [{"id": "static:listing_url:https://stripped.example/jobs", "registryState": "active"}],
    )

    split = preflight.build_report(tmp_path)["splitConsistency"]

    assert split["stub_row_repairable"] == ["static:listing_url:https://stripped.example/jobs"]
    # The absent row is a different failure with a different fix, so it must not
    # be folded in with the repairable one.
    assert split["seed_row_lost_from_store"] == ["static:listing_url:https://gone.example/jobs"]
    assert split["seed_row_demoted_in_store"] == []


def test_split_consistency_separates_a_demoted_row_from_a_lost_one(tmp_path: Path) -> None:
    """A demoted row and a lost row need opposite actions, so they are split."""
    _write_seed(
        tmp_path,
        [
            {
                "id": "static:listing_url:https://demoted.example/jobs",
                "registryState": "active",
                "listing_url": "https://demoted.example/jobs",
            },
            {
                "id": "static:listing_url:https://lost.example/jobs",
                "registryState": "active",
                "listing_url": "https://lost.example/jobs",
            },
        ],
    )
    _write_live(tmp_path, [])
    _write_pending(
        tmp_path,
        [
            {
                "id": "static:listing_url:https://demoted.example/jobs",
                "registryState": "pending",
                "listing_url": "https://demoted.example/jobs",
            }
        ],
    )

    split = preflight.build_report(tmp_path)["splitConsistency"]

    assert split["seed_row_demoted_in_store"] == ["static:listing_url:https://demoted.example/jobs"]
    assert split["seed_row_lost_from_store"] == ["static:listing_url:https://lost.example/jobs"]


def test_split_consistency_reports_state_divergence(tmp_path: Path) -> None:
    _write_seed(
        tmp_path,
        [
            {
                "id": "static:listing_url:https://a.example/jobs",
                "registryState": "active",
                "listing_url": "https://a.example/jobs",
            }
        ],
    )
    _write_live(
        tmp_path,
        [
            {
                "id": "static:listing_url:https://a.example/jobs",
                "registryState": "pending",
                "listing_url": "https://a.example/jobs",
            }
        ],
    )

    report = preflight.build_report(tmp_path)

    assert report["splitConsistency"]["seed_active_but_store_pending"] == [
        "static:listing_url:https://a.example/jobs"
    ]


def test_split_consistency_is_empty_when_the_views_agree(tmp_path: Path) -> None:
    rows = [
        {
            "id": "static:listing_url:https://a.example/jobs",
            "registryState": "active",
            "listing_url": "https://a.example/jobs",
        }
    ]
    _write_seed(tmp_path, rows)
    _write_live(tmp_path, rows)

    report = preflight.build_report(tmp_path)

    assert report["splitConsistency"] == {
        "stub_row_repairable": [],
        "seed_row_demoted_in_store": [],
        "seed_row_lost_from_store": [],
        "seed_active_but_store_pending": [],
        "seed_row_id_case_mismatch": [],
    }
    assert preflight._has_structural_defects(report) == []


def test_split_divergence_counts_as_a_structural_finding(tmp_path: Path) -> None:
    """A divergent row must reach the advisory summary, not just the JSON."""
    _write_seed(
        tmp_path,
        [
            {
                "id": "static:listing_url:https://gone.example/jobs",
                "registryState": "active",
                "listing_url": "https://gone.example/jobs",
            }
        ],
    )
    _write_live(tmp_path, [])

    findings = preflight._has_structural_defects(preflight.build_report(tmp_path))

    assert any("seed_row_lost_from_store" in finding for finding in findings)
