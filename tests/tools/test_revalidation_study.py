"""What the revalidation study must prove without a live run.

The study's job is to make a wall-clock claim attributable: the same seeded state, two code
trees, arms that cannot leak into each other, and a verdict that refuses when the arms did not
do the same work. Those properties are arithmetic and file layout, so they are tested here;
the live numbers come from running it.

Two traps get their own assertions because both would make a study lie quietly:

* a cohort priced from ``lastDurationMs`` prices every skipped board at zero -- the census must
  use the run's own ``durationMs`` and leave unpriced rows counted but not costed;
* an arm that inherits the pipeline CLI's ``force_refresh_all`` fetches everything and can never
  show a 304 -- the live arm must not pass it.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.tools._revalidation_fixtures import (  # noqa: E402
    A_NAME,
    A_URL,
    B_NAME,
    OLD,
    row,
    run_summary,
    write_study_inputs,
)
from tools.measurements.pipeline import live_run  # noqa: E402
from tools.measurements.pipeline import revalidation_study as study_mod


def _report_fixture() -> dict[str, Any]:
    return {
        "startedAt": "2026-10-25T10:00:00+00:00",
        "finishedAt": "2026-10-25T10:00:09+00:00",
        "workItems": [
            {
                "name": A_NAME,
                "status": "ok",
                "durationMs": 7000,
                "progress": {"counts": {"keptCount": 11, "fetchedCount": 20}},
            },
            {
                "name": B_NAME,
                "status": "excluded",
                "durationMs": 500,
                "progress": {"counts": {"keptCount": 0}},
            },
        ],
        "sources": [
            {
                "name": A_NAME,
                "details": [
                    {
                        "name": "A (static)",
                        "cacheDecision": "run_now",
                        "cacheDecisionReason": "static_refresh_due",
                    }
                ],
            },
            {
                "name": B_NAME,
                "details": [
                    {
                        "name": "B (static)",
                        "cacheDecision": "revalidate_only",
                        "cacheDecisionReason": "not_modified_304",
                        "httpStatus": 304,
                    }
                ],
            },
        ],
        "summary": {
            "notModifiedCount": 1,
            "revalidatedCount": 1,
            "cacheSkippedCount": 0,
            "failedSources": 0,
            "excludedSources": 1,
            "outputCount": 11,
        },
    }


def test_summary_joins_each_source_to_its_own_decision_and_duration() -> None:
    payload = live_run.summarise_report(
        _report_fixture(), elapsed_s=9.4, tag="run1", arm="a", code=Path(".")
    )

    assert payload["wallS"] == 9.0
    assert payload["sourceWorkS"] == 7.5
    assert payload["kept"] == 11
    assert payload["outputCount"] == 11
    assert payload["notModifiedItems"] == [B_NAME]
    assert payload["revalidatedItems"] == [B_NAME]
    assert payload["decisionReasonCounts"]["not_modified_304"] == 1
    decisions = {item["name"]: item["cacheDecision"] for item in payload["sources"]}
    assert decisions[A_NAME] == "run_now"
    state_names = {item["sourceName"] for item in payload["sources"] if item["sourceName"]}
    assert state_names == {"A (static)", "B (static)"}, (
        "a cohort selected from state is keyed by the display name, so the run summary keeps it"
    )


def test_the_live_arm_never_forces_a_refresh(tmp_path: Path) -> None:
    seen: dict[str, Any] = {}

    def fake_pipeline(**kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return _report_fixture()

    payload = live_run.execute(
        code=Path("."),
        data_dir=tmp_path / "data",
        output_dir=tmp_path / "arm",
        tag="run1",
        workers=4,
        names=[A_NAME, "static_source::static:listing_url:https://gone.test"],
        loader_fn=lambda **_kwargs: [(A_NAME, object()), (B_NAME, object())],
        run_pipeline_fn=fake_pipeline,
    )

    assert "force_refresh_all" not in seen, (
        "a forced arm can never answer the revalidation question"
    )
    assert seen["seed_from_existing_output"] is True
    assert [name for name, _loader in seen["source_loaders"]] == [A_NAME]
    assert payload["missingLoaders"] == ["static_source::static:listing_url:https://gone.test"]
    written = json.loads((tmp_path / "arm" / "run1.run.json").read_text(encoding="utf-8"))
    assert written["tag"] == "run1"


def test_seed_freezes_the_selected_population_and_ages_only_its_clocks(tmp_path: Path) -> None:
    registry_path, state_path = write_study_inputs(tmp_path)
    names = study_mod.select_names(study_mod.load_registry(registry_path))
    assert names == [A_NAME, B_NAME], "provider rows are not part of a static study"

    payload = study_mod.seed_study(
        tmp_path / "study",
        state_path=state_path,
        registry_path=registry_path,
        names=names,
        age_minutes=900,
    )

    assert payload["sourceCount"] == 2
    assert payload["withValidator"] == 1
    assert payload["multiPage"] == 1
    assert payload["ageMinutes"] == 900
    assert (tmp_path / "study" / "sources.txt").read_text(encoding="utf-8").split() == [
        A_NAME,
        B_NAME,
    ]
    seeded = study_mod.load_state(study_mod.state_artifact(tmp_path / "study" / "seed"))
    aged = datetime.fromisoformat(seeded[A_NAME]["lastSuccessAt"])
    assert datetime.fromisoformat(OLD) - aged == timedelta(minutes=900)
    assert seeded[A_NAME]["lastHttpEtag"] == '"v1"', "ageing must not touch validators"
    assert seeded[B_NAME]["consecutiveZeroKept"] == 3


def test_strip_validators_removes_only_the_validator_fields() -> None:
    rows = {
        A_NAME: {
            **row(kept=1, streak=0, etag='"v1"'),
            "lastHttpPageValidators": {A_URL: {"etag": "x"}},
        }
    }
    stripped = study_mod.strip_validators(rows)

    assert not study_mod.has_validator(stripped[A_NAME])
    assert stripped[A_NAME]["lastKeptCount"] == 1
    assert stripped[A_NAME]["lastSuccessAt"] == rows[A_NAME]["lastSuccessAt"]
    assert study_mod.has_validator(rows[A_NAME]), "the input row is not mutated"


def test_an_arm_gets_its_own_copy_and_cannot_be_run_twice(tmp_path: Path) -> None:
    registry_path, state_path = write_study_inputs(tmp_path)
    names = study_mod.select_names(study_mod.load_registry(registry_path))
    study_mod.seed_study(
        tmp_path / "study",
        state_path=state_path,
        registry_path=registry_path,
        names=names,
        age_minutes=0,
    )

    arm_dir = study_mod.prepare_arm(tmp_path / "study", "b", strip=True)
    seeded = study_mod.load_state(study_mod.state_artifact(tmp_path / "study" / "seed"))
    armed = study_mod.load_state(study_mod.state_artifact(arm_dir))

    assert arm_dir == tmp_path / "study" / "arms" / "b"
    assert study_mod.has_validator(seeded[A_NAME]), "the seed keeps what the tree produced"
    assert not study_mod.has_validator(armed[A_NAME]), "only the stripped arm loses it"
    with pytest.raises(SystemExit):
        study_mod.prepare_arm(tmp_path / "study", "b")


def test_arm_command_points_the_run_at_the_arm_and_its_own_sources(tmp_path: Path) -> None:
    command = study_mod.arm_command(
        tmp_path / "study", "a", code=tmp_path / "tree", workers=12, tag="run2"
    )

    assert command[1].endswith("live_run.py")
    assert command[command.index("--data-dir") + 1] == str(tmp_path / "study" / "arms" / "a")
    assert command[command.index("--output-dir") + 1] == str(tmp_path / "study" / "arms" / "a"), (
        "the pipeline resolves its state from the output root, so both must be the arm"
    )
    assert command[command.index("--sources-from") + 1] == str(tmp_path / "study" / "sources.txt")
    assert command[command.index("--tag") + 1] == "run2"
    assert command[command.index("--workers") + 1] == "12"


def test_census_prices_cohorts_from_the_runs_own_durations(tmp_path: Path) -> None:
    registry_path, state_path = write_study_inputs(tmp_path)
    registry = study_mod.load_registry(registry_path)
    rows = study_mod.load_state(state_path)
    run = {"sources": [{"name": A_NAME, "durationMs": 5000}, {"name": B_NAME, "durationMs": 0}]}

    payload = study_mod.census(rows, registry, run)

    assert payload["kept"]["count"] == 1
    assert payload["kept"]["readSeconds"] == 5.0
    assert payload["zeroKeptTagless"]["count"] == 1
    assert payload["zeroKeptTagless"]["withRunCost"] == 0
    assert payload["zeroKeptTagless"]["readSeconds"] == 0.0, (
        "a skipped row has no observed read cost; pricing it at zero is honest, inventing "
        "lastDurationMs as a source is not"
    )
    assert payload["multiPage"]["names"] == [B_NAME]
    assert payload["withValidator"] == 1


def test_the_tagless_cohort_excludes_failed_and_validator_bearing_rows() -> None:
    failed = {**row(kept=0, streak=1), "lastSuccessAt": ""}
    rows = {
        A_NAME: row(kept=0, streak=2),
        B_NAME: row(kept=0, streak=2, etag='"v1"'),
        "failed": failed,
        "kept": row(kept=4, streak=0),
    }
    run = {
        "sources": [
            {"name": A_NAME, "durationMs": 4000},
            {"name": B_NAME, "durationMs": 4000},
            {"name": "failed", "durationMs": 4000},
            {"name": "kept", "durationMs": 4000},
        ]
    }

    assert list(study_mod.tagless_cohort(rows, run)) == [A_NAME]


def _write_arm(
    study: Path, name: str, *, wall: float, kept: int, output: int, exit_code: int = 0
) -> None:
    arm_dir = study / "arms" / name
    arm_dir.mkdir(parents=True)
    summary = run_summary(wall_s=wall, kept=kept, output=output)
    (arm_dir / "arm.json").write_text(
        json.dumps({"arm": name, "tag": "run", "exitCode": exit_code, "run": summary}),
        encoding="utf-8",
    )


def test_report_names_the_faster_arm_only_when_the_work_matches(tmp_path: Path) -> None:
    study = tmp_path / "study"
    _write_arm(study, "a", wall=100.0, kept=11, output=11)
    _write_arm(study, "b", wall=60.0, kept=11, output=11)

    payload = study_mod.report(study, compare=["a:b"], min_effect_pct=2.0)

    assert payload["pairs"]["status"] == "ok"
    assert payload["pairs"]["verdict"] == "arm-b-faster"
    assert payload["pairs"]["medianPct"] == -40.0
    assert (study / "report.json").is_file()
    assert "verdict=arm-b-faster" in study_mod.render_markdown(payload)


def test_report_refuses_a_verdict_when_the_arms_produced_different_work(tmp_path: Path) -> None:
    study = tmp_path / "study"
    _write_arm(study, "a", wall=100.0, kept=11, output=11)
    _write_arm(study, "b", wall=60.0, kept=10, output=9)

    payload = study_mod.report(study, compare=["a:b"])

    assert payload["pairs"]["status"] == "parity-failed"
    assert payload["pairs"]["verdict"] == "inconclusive"
    assert {item["unit"] for item in payload["pairs"]["divergences"]} == {"kept", "outputCount"}


def test_report_treats_a_failed_arm_as_a_harness_error(tmp_path: Path) -> None:
    study = tmp_path / "study"
    _write_arm(study, "a", wall=100.0, kept=11, output=11)
    _write_arm(study, "b", wall=60.0, kept=11, output=11, exit_code=2)

    payload = study_mod.report(study, compare=["a:b"])

    assert payload["pairs"]["status"] == "harness-error"
    assert payload["pairs"]["verdict"] == "inconclusive"
