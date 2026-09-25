"""Advisory registry hygiene audit (2026-09-25).

The monitor generalizes the existing asset-page report with bounded evidence
for duplicate candidates, repeated unreachable state, and host drift. It is
observability-only: no row is demoted, deleted, or rewritten.
"""

from __future__ import annotations

import json
from pathlib import Path

from src.jobs.common.contracts_runtime import normalize_runtime_payload
from src.jobs.registry_hygiene import (
    REGISTRY_HYGIENE_SAMPLE_LIMIT,
    REGISTRY_HYGIENE_SOURCE_LIMIT,
    registry_hygiene_audit,
)


def _row(source_id: str, pages: list[str], *, listing_url: str = "") -> dict[str, object]:
    return {
        "id": source_id,
        "registryState": "active",
        "listing_url": listing_url,
        "pages": pages,
    }


def test_audit_reports_all_four_advisory_categories() -> None:
    source_id = "static:listing_url:https://board.example/careers"
    audit = registry_hygiene_audit(
        [
            _row(
                source_id,
                [
                    "https://board.example/careers",
                    "https://board.example/js/app.js",
                    "https://provider.example/jobs",
                ],
                listing_url="https://board.example/careers",
            ),
            _row(
                "static:listing_url:https://board.example/careers/",
                ["https://board.example/careers/"],
                listing_url="https://board.example/careers/",
            ),
        ],
        source_state_rows={
            f"static_source::{source_id}": {
                "lastStatus": "error",
                "lastError": "HTTP 404 not found",
                "consecutiveFailures": 3,
            }
        },
    )

    assert audit["sourceCount"] == 2
    assert audit["assetPageCount"] == 1
    assert audit["duplicateGroupCount"] == 1
    assert audit["duplicateRowCount"] == 2
    assert audit["unreachablePageCount"] == 1
    assert audit["hostDriftCount"] == 1
    first = audit["sources"][0]
    assert first["sourceId"] == source_id
    assert first["flags"] == [
        "asset_pages",
        "duplicate_candidate",
        "host_drift_candidate",
        "unreachable_page",
    ]
    assert first["sampleAssetPages"] == ["https://board.example/js/app.js"]
    assert first["unreachableEvidence"] == "http_404"


def test_audit_resolves_provider_state_by_registry_name() -> None:
    audit = registry_hygiene_audit(
        [
            {
                "id": "workable:account:ghost",
                "name": "Ghost Studio (Workable)",
                "registryState": "active",
                "api_url": "https://apply.workable.com/api/v1/widget/accounts/ghost",
            }
        ],
        source_state_rows={
            "Ghost Studio (Workable)": {
                "lastStatus": "error",
                "lastError": "Network error for https://ghost.example/career: [SSL: CERTIFICATE_VERIFY_FAILED]",
                "consecutiveFailures": 3,
            }
        },
    )

    assert audit["unreachablePageCount"] == 1
    assert audit["sources"][0]["unreachableEvidence"] == "dns_or_tls"
    assert audit["sources"][0]["sampleUnreachablePages"] == [
        "https://apply.workable.com/api/v1/widget/accounts/ghost"
    ]


def test_audit_distinguishes_not_found_from_http_404() -> None:
    rows = [
        _row(
            "static:listing_url:https://gone.example/careers",
            ["https://gone.example/careers"],
        ),
        _row("static:listing_url:https://sheet.example/careers", ["https://sheet.example/careers"]),
    ]
    audit = registry_hygiene_audit(
        rows,
        source_state_rows={
            "static_source::static:listing_url:https://gone.example/careers": {
                "lastStatus": "error",
                "lastError": "Remote end closed connection: not found",
                "consecutiveFailures": 2,
            },
            "static_source::static:listing_url:https://sheet.example/careers": {
                "lastStatus": "error",
                "lastError": "HTTP 404 for https://sheet.example/careers",
                "consecutiveFailures": 2,
            },
        },
    )

    evidence = {s["sourceId"]: s["unreachableEvidence"] for s in audit["sources"]}
    assert evidence["static:listing_url:https://gone.example/careers"] == "not_found"
    assert evidence["static:listing_url:https://sheet.example/careers"] == "http_404"


def test_audit_ignores_single_transient_failure() -> None:
    audit = registry_hygiene_audit(
        [
            _row(
                "static:listing_url:https://flaky.example/careers",
                ["https://flaky.example/careers"],
            )
        ],
        source_state_rows={
            "static_source::static:listing_url:https://flaky.example/careers": {
                "lastStatus": "error",
                "lastError": "HTTP 404 not found",
                "consecutiveFailures": 1,
            }
        },
    )

    assert audit["unreachablePageCount"] == 0
    assert audit["sources"] == []


def test_audit_separates_reviewed_collisions_from_new_drift() -> None:
    rows = [
        _row(
            "static:listing_url:https://ea.com/careers",
            ["https://ea.com/careers"],
            listing_url="https://ea.com/careers",
        ),
        _row(
            "static:listing_url:https://www.ea.com/careers",
            ["https://www.ea.com/careers"],
            listing_url="https://www.ea.com/careers",
        ),
        _row(
            "static:listing_url:https://newgame.example/jobs",
            ["https://newgame.example/jobs"],
            listing_url="https://newgame.example/jobs",
        ),
        _row(
            "static:listing_url:https://www.newgame.example/jobs",
            ["https://www.newgame.example/jobs"],
            listing_url="https://www.newgame.example/jobs",
        ),
    ]

    reviewed_only = registry_hygiene_audit(rows, known_collision_urls={"ea.com/careers"})
    assert reviewed_only["duplicateGroupCount"] == 2
    assert reviewed_only["duplicateRowCount"] == 4
    assert reviewed_only["knownCollisionGroupCount"] == 1
    assert reviewed_only["uncoveredDuplicateGroupCount"] == 1
    assert reviewed_only["uncoveredDuplicateRowCount"] == 2
    reviewed = next(s for s in reviewed_only["sources"] if s["sourceId"].endswith("ea.com/careers"))
    drifted = next(s for s in reviewed_only["sources"] if "newgame" in s["sourceId"])
    assert reviewed["uncoveredDuplicateGroupCount"] == 0
    assert drifted["uncoveredDuplicateGroupCount"] == 1

    # No baseline available: everything is uncovered so drift is never hidden.
    unknown = registry_hygiene_audit(rows)
    assert unknown["knownCollisionGroupCount"] == 0
    assert unknown["uncoveredDuplicateGroupCount"] == 2
    assert unknown["uncoveredDuplicateRowCount"] == 4

    # A junk baseline must not raise and must not mask duplicates.
    assert (
        registry_hygiene_audit(rows, known_collision_urls="not-a-set")[
            "uncoveredDuplicateGroupCount"
        ]
        == 2
    )


def test_audit_is_zero_for_clean_rows() -> None:
    audit = registry_hygiene_audit(
        [
            _row(
                "static:listing_url:https://clean.example/careers",
                ["https://clean.example/careers"],
            )
        ]
    )

    assert audit == {
        "sourceCount": 0,
        "assetPageCount": 0,
        "duplicateGroupCount": 0,
        "duplicateRowCount": 0,
        "knownCollisionGroupCount": 0,
        "uncoveredDuplicateGroupCount": 0,
        "uncoveredDuplicateRowCount": 0,
        "unreachablePageCount": 0,
        "hostDriftCount": 0,
        "sources": [],
    }


def test_audit_bounds_sources_and_samples_but_keeps_counts_exact() -> None:
    rows = [
        _row(
            f"static:listing_url:https://board{index}.example/careers",
            [f"https://board{index}.example/js/{n}.js" for n in range(5)],
        )
        for index in range(REGISTRY_HYGIENE_SOURCE_LIMIT + 5)
    ]

    audit = registry_hygiene_audit(rows)

    assert audit["sourceCount"] == REGISTRY_HYGIENE_SOURCE_LIMIT + 5
    assert audit["assetPageCount"] == (REGISTRY_HYGIENE_SOURCE_LIMIT + 5) * 5
    assert len(audit["sources"]) == REGISTRY_HYGIENE_SOURCE_LIMIT
    assert len(audit["sources"][0]["sampleAssetPages"]) == REGISTRY_HYGIENE_SAMPLE_LIMIT


def test_audit_tolerates_junk_input() -> None:
    assert registry_hygiene_audit(None)["sourceCount"] == 0
    assert registry_hygiene_audit(42)["sources"] == []
    assert registry_hygiene_audit([None, 7, "x", {}])["sources"] == []


def test_contracts_normalizer_round_trips_and_clamps() -> None:
    audit = registry_hygiene_audit(
        [
            _row(
                "static:listing_url:https://board.example/careers",
                ["https://board.example/careers", "https://board.example/app.js"],
            )
        ]
    )
    normalized = normalize_runtime_payload({"registryHygieneAudit": audit}, selected_source_count=1)
    assert normalized["registryHygieneAudit"] == audit

    clamped = normalize_runtime_payload(
        {
            "registryHygieneAudit": {
                "sourceCount": "many",
                "assetPageCount": None,
                "extra": "dropped",
                "sources": [
                    {
                        "sourceId": 5,
                        "flags": ["asset_pages", "unknown", "host_drift_candidate"],
                        "sampleAssetPages": [1, "ok.js"],
                    },
                    "junk-row",
                ],
            }
        },
        selected_source_count=1,
    )["registryHygieneAudit"]
    assert clamped["sourceCount"] == 0
    assert clamped["sources"][0]["sourceId"] == "5"
    assert clamped["sources"][0]["flags"] == ["asset_pages", "host_drift_candidate"]
    assert clamped["sources"][0]["sampleAssetPages"] == ["ok.js"]


def test_runtime_setup_stamps_hygiene_audit() -> None:
    import src.jobs.pipeline_run_setup as run_setup

    assert run_setup.__dict__.get("registry_hygiene_audit") is registry_hygiene_audit
    assert callable(run_setup.__dict__.get("known_twin_career_urls"))


def test_live_seed_audit_returns_stable_shape() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    seed_path = repo_root / "data" / "defaults" / "source-registry-active.seed.json"
    if not seed_path.exists():
        return
    rows = json.loads(seed_path.read_text(encoding="utf-8"))
    audit = registry_hygiene_audit(rows)
    assert set(audit) == {
        "sourceCount",
        "assetPageCount",
        "duplicateGroupCount",
        "duplicateRowCount",
        "knownCollisionGroupCount",
        "uncoveredDuplicateGroupCount",
        "uncoveredDuplicateRowCount",
        "unreachablePageCount",
        "hostDriftCount",
        "sources",
    }
    assert len(audit["sources"]) <= REGISTRY_HYGIENE_SOURCE_LIMIT
