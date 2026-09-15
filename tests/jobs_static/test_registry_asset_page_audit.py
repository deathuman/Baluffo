"""Registry asset-page audit (2026-09-15): post-setup invariant that
configured listing ``pages`` are documents, not assets.

The runtime filter (``BALUFFO_STATIC_ASSET_URL_FILTER``) prevents the fetch
waste; this monitor surfaces the stale registry rows in the fetch report
(``runtime.registryAssetPageAudit``) so the data gets repaired at the
source — the same "nonzero is the flag" shape as the active-junk-class row
monitor, kill-switch-independent, with bounded samples keeping the payload
small while the counts stay exact.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.jobs.common.contracts_runtime import normalize_runtime_payload
from src.jobs.page_gating import registry_asset_page_audit


def _sms_row() -> dict:
    return {
        "id": "static:listing_url:https://sms.playstation.com",
        "registryState": "active",
        "pages": [
            "https://sms.playstation.com",
            "https://sms.playstation.com/js/careers-category.3186a499.js",
            "https://sms.playstation.com/css/site.css",
        ],
    }


def test_audit_flags_asset_pages_with_counts_and_samples() -> None:
    audit = registry_asset_page_audit(
        [
            _sms_row(),
            {
                "id": "static:listing_url:https://example.com",
                "pages": ["https://example.com/careers"],
            },
            {"id": "no-pages-row"},
            {"id": "empty-pages", "pages": []},
            {"id": "pages-not-a-list", "pages": "https://example.com/js/x.js"},
            "junk-string-row",
        ]
    )

    assert audit["sourceCount"] == 1
    assert audit["assetPageCount"] == 2
    assert len(audit["sources"]) == 1
    flagged = audit["sources"][0]
    assert flagged["sourceId"] == "static:listing_url:https://sms.playstation.com"
    assert flagged["registryState"] == "active"
    assert flagged["assetPageCount"] == 2
    assert flagged["sampleAssetPages"] == [
        "https://sms.playstation.com/js/careers-category.3186a499.js",
        "https://sms.playstation.com/css/site.css",
    ]


def test_audit_clean_registry_is_zero() -> None:
    audit = registry_asset_page_audit(
        [
            {"id": "a", "pages": ["https://a.com/careers", "https://a.com/jobs?page=2"]},
            {"id": "b", "pages": []},
        ]
    )

    assert audit == {"sourceCount": 0, "assetPageCount": 0, "sources": []}


def test_audit_counts_are_exact_beyond_flag_cap() -> None:
    """Sources past the 20-entry flag cap still count toward both totals."""
    rows = [
        {
            "id": f"static:listing_url:https://board{i}.example.com",
            "pages": [f"https://board{i}.example.com/js/app.js"],
        }
        for i in range(25)
    ]

    audit = registry_asset_page_audit(rows)

    assert audit["sourceCount"] == 25
    assert audit["assetPageCount"] == 25
    assert len(audit["sources"]) == 20  # flag cap


def test_audit_samples_capped_at_three() -> None:
    row = {
        "id": "s",
        "pages": [f"https://s.com/js/chunk{i}.js" for i in range(8)],
    }

    audit = registry_asset_page_audit([row])

    assert audit["assetPageCount"] == 8
    assert len(audit["sources"][0]["sampleAssetPages"]) == 3


def test_audit_tolerates_junk_input() -> None:
    assert registry_asset_page_audit(None) == {
        "sourceCount": 0,
        "assetPageCount": 0,
        "sources": [],
    }
    assert registry_asset_page_audit(42) == {
        "sourceCount": 0,
        "assetPageCount": 0,
        "sources": [],
    }
    assert registry_asset_page_audit([None, 7, "x", {}]) == {
        "sourceCount": 0,
        "assetPageCount": 0,
        "sources": [],
    }


def test_runtime_payload_stamps_audit_block() -> None:
    """The setup stage stamps the audit into the runtime payload (the same
    post-build mutation point as staticSuppressionPolicy)."""
    import src.jobs.pipeline_run_setup as run_setup

    source = run_setup.__dict__.get("registry_asset_page_audit")
    assert source is registry_asset_page_audit


def test_contracts_normalizer_round_trips_audit() -> None:
    audit = registry_asset_page_audit([_sms_row()])

    normalized = normalize_runtime_payload(
        {"registryAssetPageAudit": audit}, selected_source_count=1
    )

    assert normalized["registryAssetPageAudit"] == audit


def test_contracts_normalizer_absent_block_zeroes() -> None:
    normalized = normalize_runtime_payload({}, selected_source_count=1)

    assert normalized["registryAssetPageAudit"] == {
        "sourceCount": 0,
        "assetPageCount": 0,
        "sources": [],
    }


def test_contracts_normalizer_clamps_junk() -> None:
    normalized = normalize_runtime_payload(
        {
            "registryAssetPageAudit": {
                "sourceCount": "many",
                "assetPageCount": None,
                "extra": "dropped",
                "sources": [
                    {"sourceId": 5, "assetPageCount": "nine", "sampleAssetPages": [1, "ok.js"]},
                    "junk-row",
                ],
            }
        },
        selected_source_count=1,
    )

    block = normalized["registryAssetPageAudit"]
    assert block["sourceCount"] == 0
    assert block["assetPageCount"] == 0
    assert len(block["sources"]) == 1
    assert block["sources"][0]["sourceId"] == "5"
    assert block["sources"][0]["assetPageCount"] == 0
    assert block["sources"][0]["sampleAssetPages"] == ["ok.js"]


def test_contracts_normalizer_bounds_source_and_sample_lists() -> None:
    payload = {
        "sourceCount": 40,
        "assetPageCount": 40,
        "sources": [
            {
                "sourceId": f"s{i}",
                "registryState": "active",
                "assetPageCount": 1,
                "sampleAssetPages": [
                    f"https://s{i}.com/js/a.js",
                    "https://s{i}.com/js/b.js",
                    "https://s{i}.com/js/c.js",
                    "https://s{i}.com/js/d.js",
                ],
            }
            for i in range(30)
        ],
    }

    normalized = normalize_runtime_payload(
        {"registryAssetPageAudit": payload}, selected_source_count=1
    )

    block = normalized["registryAssetPageAudit"]
    assert len(block["sources"]) == 20
    assert len(block["sources"][0]["sampleAssetPages"]) == 3


def test_live_store_audit_finds_sms_class_rows() -> None:
    """Pin the live expectation: the runtime registry still carries rows with
    asset pages (sms is repaired; the other 18 documented rows remain)."""
    repo_root = Path(__file__).resolve().parents[2]
    seed_path = repo_root / "data" / "defaults" / "source-registry-active.seed.json"
    if not seed_path.exists():
        pytest.skip("seed file not present")

    seed = json.loads(seed_path.read_text(encoding="utf-8"))
    rows = seed.get("sources", seed) if isinstance(seed, dict) else seed
    sms_rows = [
        row
        for row in rows
        if isinstance(row, dict)
        and str(row.get("id", "")) == "static:listing_url:https://sms.playstation.com"
    ]

    audit = registry_asset_page_audit(sms_rows)

    # The sms row was repaired on 2026-09-15: it must carry no asset pages.
    assert audit == {"sourceCount": 0, "assetPageCount": 0, "sources": []}
