"""Study shapes the lane-parity tests need without running the audit.

One arm is the artifact the audit writes plus the record the tool keeps beside it. These
helpers build both, so a test describes a study as boards, failures and lanes rather than as
JSON, and the report tests never touch the network.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tools.measurements.discovery import lane_parity


def board(name: str, url: str, *, studio: str = "") -> dict[str, Any]:
    return {"name": name, "studio": studio or name, "careersUrl": url, "adapter": "static"}


def boards(*names: str) -> list[dict[str, Any]]:
    return [board(name, f"https://{name.lower()}.example/jobs") for name in names]


def write_arm(
    study: Path,
    name: str,
    lanes: int,
    *,
    boards: list[dict[str, Any]],
    errors: dict[str, int] | None = None,
    homepage_failures: int = 0,
    total_failures: int = 100,
    zero_job: list[dict[str, Any]] | None = None,
) -> Path:
    """Write one arm's artifact and record, shaped like a real study arm."""
    artifact_path = study / "arms" / name / "data" / lane_parity.ARTIFACT_NAME
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(
            {
                "completedUrls": [f"https://jobs.example/{index}" for index in range(10)],
                "activeCandidates": boards,
                "failureCounts": {lane_parity.HOMEPAGE_STAGE: homepage_failures},
                "failureErrorCounts": dict(errors or {}),
                "failures": [{"stage": "homepage_fetch", "name": "sampled"}],
                "rejectedForActivation": [],
                "zeroJobCandidates": list(zero_job or []),
                "summary": {
                    "failures": total_failures,
                    "phaseRuntime": {
                        "phases": [
                            {
                                "phase": lane_parity.HOMEPAGE_STAGE,
                                "occurrences": 1,
                                "wallMs": 1_000,
                                "items": 10,
                                "configuredLanes": lanes,
                                "peakInFlight": lanes,
                            }
                        ]
                    },
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    record_path = study / "arms" / name / "arm.json"
    record_path.parent.mkdir(parents=True, exist_ok=True)
    record_path.write_text(
        json.dumps(
            {
                "name": name,
                "lanes": lanes,
                "wallS": 10.0,
                "population": {"digest": "samestudy", "count": len(boards)},
                "csv": {"sha256": "csvsame"},
                "phases": [
                    {
                        "phase": lane_parity.HOMEPAGE_STAGE,
                        "wallMs": 1_000,
                        "configuredLanes": lanes,
                        "peakInFlight": lanes,
                    }
                ],
                "artifact": str(artifact_path),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return record_path


def counterbalanced(study: Path, **sets: list[dict[str, Any]]) -> list[str]:
    """Four arms whose same-lane churn is known, so the floor is predictable."""
    write_arm(study, "p1-32", 32, boards=sets.get("p1_32", []))
    write_arm(study, "p1-64", 64, boards=sets.get("p1_64", []))
    write_arm(study, "p2-32", 32, boards=sets.get("p2_32", []))
    write_arm(study, "p2-64", 64, boards=sets.get("p2_64", []))
    return ["p1:p1-32,p1-64", "p2:p2-32,p2-64"]
