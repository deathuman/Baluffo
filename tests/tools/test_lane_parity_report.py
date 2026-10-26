"""What the lane-parity report must refuse to claim.

Every property that could make the comparison lie is arithmetic over artifacts: boards keyed by
host + tenant rather than studio label, failure counts read from the whole run rather than the
artifact's 200-row sample, a tolerance taken from the study's own same-lane churn, and a
verdict that says `inconclusive` when no floor was measured. Attribution has to separate a host
that failed from a board that was merely decided differently, or a lane effect gets blamed for
a downstream change.
"""

from __future__ import annotations

import json
import sys
from argparse import Namespace
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.tools._lane_parity_fixtures import (  # noqa: E402
    board,
    boards,
    counterbalanced,
    write_arm,
)
from tools.measurements.discovery import lane_parity  # noqa: E402


def _report(study: Path, specs: list[str]) -> dict[str, Any]:
    return lane_parity.build_report(study, specs)


def test_boards_are_paired_by_host_and_tenant_not_by_studio_label(tmp_path: Path) -> None:
    study = tmp_path / "study"
    nvidia = board("Studio", "https://nvidia.wd5.myworkdayjobs.com/NV", studio="Shared")
    aristocrat = board("Studio", "https://aristocrat.wd5.myworkdayjobs.com/EX", studio="Shared")
    write_arm(study, "p1-32", 32, boards=[nvidia, aristocrat])
    write_arm(study, "p1-64", 64, boards=[])

    pair = _report(study, ["p1:p1-32,p1-64"])["pairs"][0]

    assert pair["boards"]["low"] == 2
    assert pair["boards"]["symmetricDifference"] == 2, "a shared label must not merge tenants"
    assert len(set(pair["boards"]["onlyLow"])) == 2


def test_a_pair_that_audited_different_rows_is_reported_as_unpinned(tmp_path: Path) -> None:
    study = tmp_path / "study"
    write_arm(study, "p1-32", 32, boards=[board("A", "https://a.example/jobs")])
    record_path = write_arm(study, "p1-64", 64, boards=[])
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["population"]["digest"] = "differentstudy"
    record_path.write_text(json.dumps(record), encoding="utf-8")

    report = _report(study, ["p1:p1-32,p1-64"])

    assert report["pairs"][0]["population"]["samePopulation"] is False
    assert report["totals"]["populationPinned"] is False
    assert report["pairs"][0]["csvSame"] is True


def test_failure_counts_come_from_the_whole_run_not_the_capped_sample(tmp_path: Path) -> None:
    study = tmp_path / "study"
    listed = [board("A", "https://a.example/jobs")]
    write_arm(
        study,
        "p1-32",
        32,
        boards=listed,
        errors={"homepage_fetch|a.example|/jobs|429": 12},
        homepage_failures=349,
        total_failures=4_599,
    )
    write_arm(study, "p1-64", 64, boards=listed)

    pair = _report(study, ["p1:p1-32,p1-64"])["pairs"][0]

    assert pair["failures"]["totalLow"] == 4_599
    assert pair["failures"]["homepageLow"] == 349
    assert pair["failures"]["identitiesLow"] == 1


def test_a_lost_board_is_attributed_to_its_host_failure_in_the_other_arm(
    tmp_path: Path,
) -> None:
    study = tmp_path / "study"
    write_arm(study, "p1-32", 32, boards=[board("Acme", "https://jobs.acme.example/careers")])
    write_arm(
        study,
        "p1-64",
        64,
        boards=[],
        errors={"homepage_fetch|jobs.acme.example|/careers|429": 3},
    )

    row = _report(study, ["p1:p1-32,p1-64"])["pairs"][0]["attribution"][0]

    assert row["presentIn"] == "32 only"
    assert row["failedOnHost"] is True
    assert row["homepageFailure"] is True
    assert row["hostFailureHits"][0]["identity"].endswith("429")


def test_a_board_missing_from_a_host_that_read_fine_is_not_blamed_on_lanes(
    tmp_path: Path,
) -> None:
    study = tmp_path / "study"
    write_arm(study, "p1-32", 32, boards=[board("Quiet", "https://quiet.example/jobs")])
    write_arm(
        study,
        "p1-64",
        64,
        boards=[],
        zero_job=[{"name": "Quiet", "studio": "Quiet", "jobsFound": 0}],
    )

    row = _report(study, ["p1:p1-32,p1-64"])["pairs"][0]["attribution"][0]

    assert row["failedOnHost"] is False
    assert row["zeroJob"]["jobsFound"] == 0


def test_a_probe_429_is_not_explained_by_a_siblings_zero_job(tmp_path: Path) -> None:
    """A provider rejection carries no studio or url; the board identity is what pins it.

    The artifact's probe-failure rows leave `studio` and `url` empty, so matching them by
    studio label finds nothing and falls through to the zero-job row of a *sibling*
    candidate — which reads as "probed empty" when the board was actually rate-limited.
    """
    study = tmp_path / "study"
    workable = board(
        "1TK (Workable)",
        "",
        studio="1TK",
        api_url="https://apply.workable.com/api/v1/widget/accounts/1tk?details=true",
        source_id="workable:account:1tk",
        adapter="workable",
    )
    write_arm(study, "p1-32", 32, boards=[workable])
    write_arm(
        study,
        "p1-64",
        64,
        boards=[],
        zero_job=[
            {
                "id": "smartrecruiters:company_id:1tk",
                "name": "1TK (SmartRecruiters)",
                "studio": "1TK",
                "jobsFound": 0,
            }
        ],
        rejections=[
            {
                "reason": "probe_failed",
                "reasonDetail": "probe_failed",
                "sourceId": "workable:account:1tk",
                "name": "1TK (Workable)",
                "adapter": "workable",
                "error": (
                    "https://apply.workable.com/1tk: Client error '429 Too Many Requests' "
                    "for url 'https://apply.workable.com/1tk'"
                ),
                "candidate": {
                    "api_url": "https://apply.workable.com/api/v1/widget/accounts/1tk?details=true"
                },
            }
        ],
    )

    row = _report(study, ["p1:p1-32,p1-64"])["pairs"][0]["attribution"][0]

    assert row["rejection"]["reason"] == "probe_failed"
    assert "429" in row["rejection"]["error"]
    assert row["zeroJob"] is None, "a sibling's zero is not this board's outcome"
    assert row["failedOnHost"] is True


def test_two_tenants_of_one_studio_do_not_borrow_each_others_zero(tmp_path: Path) -> None:
    study = tmp_path / "study"
    studio = "Beautiful Game Studios (Square-Enix Europe)"
    name = f"{studio} (Workable)"
    low = [
        board(
            name,
            "http://eu.square-enix.com/jobs",
            studio=studio,
            api_url=f"https://apply.workable.com/api/v1/widget/accounts/{tenant}?details=true",
            source_id=f"workable:account:{tenant}",
            adapter="workable",
        )
        for tenant in ("square-enix", "square-enix-america")
    ]
    write_arm(study, "p1-32", 32, boards=low)
    write_arm(
        study,
        "p1-64",
        64,
        boards=[],
        zero_job=[
            {
                "id": "workable:account:square-enix-america",
                "name": name,
                "studio": studio,
                "jobsFound": 0,
            }
        ],
    )

    rows = _report(study, ["p1:p1-32,p1-64"])["pairs"][0]["attribution"]
    by_tenant = {
        row["identity"].rsplit("/", 1)[-1]: row for row in rows if row["presentIn"] == "32 only"
    }

    assert by_tenant["square-enix-america"]["zeroJob"]["jobsFound"] == 0
    assert by_tenant["square-enix"]["zeroJob"] is None


def test_floor_is_the_same_lane_churn_and_an_excess_is_a_violation(tmp_path: Path) -> None:
    study = tmp_path / "study"
    specs = counterbalanced(
        study,
        p1_32=boards("A", "B", "C", "D"),
        p1_64=boards("A", "B", "C"),
        p2_32=boards("A", "B", "C", "D", "E"),
        p2_64=boards("A", "B"),
    )

    report = _report(study, specs)

    assert report["floor"]["boardSymmetricDifference"] == 1
    assert report["pairs"][0]["boards"]["symmetricDifference"] == 1
    assert any(
        "p2: board symmetric difference 3 > floor 1" in line for line in report["violations"]
    )
    assert report["verdict"] == "parity-not-supported"


def test_one_way_board_loss_is_not_parity_even_when_every_pair_is_inside_the_floor(
    tmp_path: Path,
) -> None:
    study = tmp_path / "study"
    specs = counterbalanced(
        study,
        p1_32=boards("A", "B", "C", "D", "E"),
        p1_64=boards("A", "B", "C", "D"),
        p2_32=boards("A", "B", "C", "D", "F"),
        p2_64=boards("A", "B", "C", "D"),
    )

    report = _report(study, specs)

    assert report["violations"] == []
    assert report["totals"]["pairsWithBoardLoss"] == 2
    assert report["verdict"] == "parity-not-supported"


def test_a_direction_flip_inside_the_floor_is_parity(tmp_path: Path) -> None:
    study = tmp_path / "study"
    specs = counterbalanced(
        study,
        p1_32=boards("A", "B", "C", "D", "E"),
        p1_64=boards("A", "B", "C", "D"),
        p2_32=boards("A", "B", "C", "D"),
        p2_64=boards("A", "B", "C", "D", "G"),
    )

    report = _report(study, specs)

    assert report["floor"]["boardSymmetricDifference"] == 1
    assert report["totals"] == {
        "pairs": 2,
        "pairsWithBoardLoss": 1,
        "pairsWithBoardGain": 1,
        "boardsLost": 1,
        "boardsGained": 1,
        "netBoardDelta": 0,
        "populationPinned": True,
        "csvPinned": True,
    }
    assert report["verdict"] == "parity-supported"


def test_a_single_arm_per_lane_cannot_judge_parity(tmp_path: Path) -> None:
    study = tmp_path / "study"
    write_arm(study, "p1-32", 32, boards=boards("A"))
    write_arm(study, "p1-64", 64, boards=boards("A"))

    report = _report(study, ["p1:p1-32,p1-64"])

    assert report["floorDefined"] is False
    assert report["verdict"] == "inconclusive"


def test_reversed_pairs_are_refused_rather_than_compared(tmp_path: Path) -> None:
    study = tmp_path / "study"
    write_arm(study, "p1-32", 32, boards=boards("A"))
    write_arm(study, "p1-64", 64, boards=boards("A"))

    with pytest.raises(SystemExit, match="pass the lower-lane arm first"):
        _report(study, ["p1:p1-64,p1-32"])


def test_study_runs_low_then_high_then_high_then_low(tmp_path: Path, monkeypatch: Any) -> None:
    order: list[str] = []
    specs: list[list[str]] = []

    def fake_run(command: list[str], **_kwargs: Any) -> Any:
        name = command[command.index("--name") + 1]
        lanes = command[command.index("--lanes") + 1]
        order.append(f"{name}:{lanes}")
        return Namespace(returncode=0, stdout="", stderr="")

    def fake_report(_study: Path, items: list[str]) -> dict[str, Any]:
        specs.append(items)
        return {}

    monkeypatch.setattr(lane_parity.subprocess, "run", fake_run)
    monkeypatch.setattr(lane_parity, "build_report", fake_report)
    monkeypatch.setattr(lane_parity, "_render_report", lambda _report: "")
    lane_parity.cmd_study(
        Namespace(
            study=str(tmp_path / "study"),
            pairs=2,
            low=32,
            high=64,
            batches=3,
            batch_size=1000,
            timeout=6,
            force=False,
        )
    )

    assert order == ["p1-32:32", "p1-64:64", "p2-64:64", "p2-32:32"]
    assert specs == [["p1:p1-32,p1-64", "p2:p2-32,p2-64"]]
