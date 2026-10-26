"""What one lane-parity arm must prove before its numbers are comparable.

An arm is only evidence if its own receipt shows the lane count it was given, if its
population is pinned by a digest rather than by the arguments, and if it cannot be silently
replaced after the other half of its pair has already run. Those are the properties here; the
comparison between arms lives in `test_lane_parity_report.py`.
"""

from __future__ import annotations

import json
import sys
from argparse import Namespace
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.tools._lane_parity_fixtures import board, write_arm  # noqa: E402
from tools.measurements.discovery import lane_parity  # noqa: E402


def test_arm_record_pins_the_population_by_digest_in_any_order(tmp_path: Path) -> None:
    study = tmp_path / "study"
    write_arm(study, "p1-64", 64, boards=[], homepage_failures=2)
    artifact = json.loads(
        (study / "arms" / "p1-64" / "data" / lane_parity.ARTIFACT_NAME).read_text(encoding="utf-8")
    )
    args = Namespace(timeout=6, batch_size=1000, batches=3)

    record = lane_parity._arm_record(
        "p1-64", 64, args, artifact, {"sha256": "csv"}, 12.3, tmp_path / "artifact.json"
    )
    reordered = dict(artifact, completedUrls=list(reversed(artifact["completedUrls"])))
    same = lane_parity._arm_record(
        "p1-64", 64, args, reordered, {"sha256": "csv"}, 12.3, tmp_path / "artifact.json"
    )

    assert record["population"]["count"] == 10
    assert record["population"]["digest"] == same["population"]["digest"]
    assert record["population"]["digest"] == lane_parity._digest(artifact["completedUrls"])
    assert record["phases"][0]["configuredLanes"] == 64
    assert record["failureCounts"] == {lane_parity.HOMEPAGE_STAGE: 2}


@pytest.mark.parametrize(
    ("lanes", "configured", "peak", "message"),
    [(64, 64, 41, "never reached 64 lanes"), (32, 20, 20, "configured lanes, not 32")],
)
def test_an_arm_that_does_not_prove_its_lanes_is_discarded(
    lanes: int, configured: int, peak: int, message: str
) -> None:
    record = {
        "name": "arm",
        "lanes": lanes,
        "phases": [
            {
                "phase": lane_parity.HOMEPAGE_STAGE,
                "configuredLanes": configured,
                "peakInFlight": peak,
            }
        ],
    }

    with pytest.raises(SystemExit, match=message):
        lane_parity.assert_lane_proof(record)


def test_an_existing_arm_is_not_silently_replaced(tmp_path: Path) -> None:
    study = tmp_path / "study"
    record_path = write_arm(study, "p1-32", 32, boards=[board("A", "https://a.example/jobs")])
    args = Namespace(
        study=str(study),
        name="p1-32",
        lanes=32,
        timeout=6,
        batch_size=1_000,
        batches=3,
        force=False,
    )

    with pytest.raises(SystemExit, match="already ran"):
        lane_parity.run_arm(args)

    assert json.loads(record_path.read_text(encoding="utf-8"))["lanes"] == 32


def test_a_moved_study_still_reads_its_arm_data(tmp_path: Path) -> None:
    study = tmp_path / "study"
    record_path = write_arm(study, "p1-32", 32, boards=[board("A", "https://a.example/jobs")])
    write_arm(study, "p1-64", 64, boards=[])
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["artifact"] = str(tmp_path / "stale" / "artifact.json")
    record_path.write_text(json.dumps(record), encoding="utf-8")

    pair = lane_parity.build_report(study, ["p1:p1-32,p1-64"])["pairs"][0]

    assert pair["boards"]["low"] == 1
