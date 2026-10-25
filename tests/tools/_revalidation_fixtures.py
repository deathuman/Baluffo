"""Fixtures the revalidation study's tests share.

The study has two test modules -- the arm/census/report surface and the tagless probe -- and
both need the same two-board registry and state row. One copy lives here so neither module
carries a near-duplicate of the other.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.pipeline import revalidation_study as study_mod  # noqa: E402

A_URL = "https://a.test/careers"
B_URL = "https://b.test/careers"
REGISTRY: list[dict[str, Any]] = [
    {
        "id": f"static:listing_url:{A_URL}",
        "name": "A (static)",
        "adapter": "static",
        "pages": [A_URL],
    },
    {
        "id": f"static:listing_url:{B_URL}",
        "name": "B (static)",
        "adapter": "static",
        "pages": [B_URL, "https://b.test/jobs"],
    },
    {"id": "greenhouse:board:c", "name": "C (GH)", "adapter": "greenhouse"},
]
A_NAME = study_mod.static_loader_name(REGISTRY[0])
B_NAME = study_mod.static_loader_name(REGISTRY[1])
OLD = (datetime.now(UTC) - timedelta(days=20)).isoformat()


def row(*, kept: int, streak: int, etag: str = "") -> dict[str, Any]:
    return {
        "lastStatus": "ok",
        "lastAdapter": "static",
        "lastKeptCount": kept,
        "consecutiveZeroKept": streak,
        "lastSuccessAt": OLD,
        "lastCheckedAt": OLD,
        "nextEligibleCheckAt": OLD,
        **({"lastHttpEtag": etag} if etag else {}),
    }


def write_study_inputs(tmp_path: Path) -> tuple[Path, Path]:
    registry = tmp_path / "source-registry-active.json"
    state = tmp_path / "jobs-source-state.json"
    registry.write_text(json.dumps(REGISTRY), encoding="utf-8")
    state.write_text(
        json.dumps(
            {
                A_NAME: row(kept=12, streak=0, etag='"v1"'),
                B_NAME: row(kept=0, streak=3),
            }
        ),
        encoding="utf-8",
    )
    return registry, state


def run_summary(*, wall_s: float, kept: int, output: int, ndm: int = 0) -> dict[str, Any]:
    return {
        "wallS": wall_s,
        "sourceWorkS": wall_s * 4,
        "sourceCount": 2,
        "kept": kept,
        "outputCount": output,
        "notModifiedCount": ndm,
        "revalidatedCount": 1,
        "cacheSkippedCount": 0,
        "failedSources": 0,
        "sources": [
            {"name": A_NAME, "durationMs": 5000},
            {"name": B_NAME, "durationMs": 0},
        ],
    }
