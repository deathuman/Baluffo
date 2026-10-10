"""What one replay's receipts mean, and the configuration that replay is launched under.

Two things a reader would otherwise have to trust. **A miss is read out of the run's own ledger,
not inferred from the exit code** -- ``note_miss`` fires before the ``CorpusMiss`` is raised, so a
source that swallowed the exception must still be caught -- and **every way an epoch can fail to
certify has its own verdict** instead of collapsing into "failed": a corpus hole, an unresolvable
offer, a finished run that reported an error, a guard block, an unreadable outcome, and a fidelity
drift are six different findings with six different remedies.

The second half pins that each replay reproduces its *record's* configuration, including the two
driver defaults that are non-empty (`--sources 8`, `--families google_sheets`), where omitting a
recorded zero would silently sample boards the record never fetched.

Nothing here starts a subprocess: ``judge_sample`` reads fabricated receipts from disk.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.helpers.corpus_replay_fixtures import (  # noqa: E402
    REQUESTED,
    UNREQUESTED,
    epoch_dir,
    judge,
    listing_row,
    miss,
    parity,
    recorded_summary,
    run_tree,
    sample,
)
from tools.measurements.corpus import harness, truncation  # noqa: E402
from tools.measurements.corpus import replay_check as check


def test_a_completed_replay_that_reproduces_the_record_verifies(tmp_path: Path) -> None:
    judged = judge(tmp_path, parity=parity())

    assert judged["verdict"] == check.VERDICT_VERIFIED
    assert judged["problems"] == []
    assert all(judged["matches"].values()) is True
    assert judged["ledger"] == {
        "requestsOffered": 4,
        "requestsHit": 4,
        "requestsMissed": 0,
        "unattributedCount": 0,
    }
    assert judged["wallMs"] == 1234


def test_a_miss_is_read_from_the_ledger_even_when_the_run_called_itself_clean(
    tmp_path: Path,
) -> None:
    """The reason this reads the ledger: a swallowed ``CorpusMiss`` cannot hide behind exit 0."""

    judged = judge(
        tmp_path,
        parity=parity(
            ledger={
                "requestsOffered": 4,
                "requestsHit": 2,
                "requestsMissed": 2,
                "unattributedCount": 0,
            },
            misses=[miss(REQUESTED), miss(UNREQUESTED, surface="fetch_text")],
        ),
    )

    assert judged["verdict"] == check.VERDICT_MISSED
    assert judged["problems"][0]["urls"] == sorted({REQUESTED, UNREQUESTED})
    assert judged["problems"][0]["surfaces"] == {"fetch_text": 1, "httpx": 1}


def test_a_corpus_miss_status_names_the_urls_without_a_ledger(tmp_path: Path) -> None:
    """An aborted replay writes only the named misses, so that list is the evidence there."""

    judged = judge(
        tmp_path,
        parity={
            "status": "corpus-miss",
            "error": f"corpus miss for {UNREQUESTED!r}",
            "misses": [miss(UNREQUESTED)],
            "guard": {"guarded": True, "blocked": 0, "blockedRemote": 0},
        },
        exit_code=2,
    )

    assert judged["verdict"] == check.VERDICT_MISSED
    assert judged["problems"][0]["urls"] == [UNREQUESTED]
    assert judged["problems"][0]["detail"].startswith("1 request(s)")


def test_an_offer_the_corpus_never_resolved_is_not_silence(tmp_path: Path) -> None:
    """A lost body or malformed entry never becomes a miss; it lands in ``unattributedCount``."""

    judged = judge(
        tmp_path,
        parity=parity(
            ledger={
                "requestsOffered": 3,
                "requestsHit": 3,
                "requestsMissed": 0,
                "unattributedCount": 1,
                "unattributedUrls": [UNREQUESTED],
            }
        ),
    )

    assert judged["verdict"] == check.VERDICT_UNANSWERED
    assert judged["problems"][0]["urls"] == [UNREQUESTED]


def test_a_pipeline_error_a_finished_run_reported_is_a_finding(tmp_path: Path) -> None:
    judged = judge(tmp_path, parity=parity(pipelineError="transport exploded"))

    assert judged["verdict"] == check.VERDICT_PIPELINE_ERROR
    assert "transport exploded" in judged["problems"][0]["detail"]


def test_an_egress_block_is_not_reported_as_a_corpus_hole(tmp_path: Path) -> None:
    """The corpus was never asked, so this is a harness finding -- and it maps to the infra code."""

    judged = judge(
        tmp_path,
        parity={
            "status": "egress-blocked",
            "error": "egress blocked during replay: example.com:443",
            "guard": {"guarded": True, "blocked": 1, "blockedRemote": 1},
        },
        exit_code=2,
    )

    assert judged["verdict"] == check.VERDICT_EGRESS
    assert judged["verdict"] not in check.FALSIFYING
    assert judged["verdict"] in check.UNJUDGEABLE
    assert "example.com:443" in judged["problems"][0]["detail"]


def test_a_divergence_from_the_record_refuses_the_certification(tmp_path: Path) -> None:
    """``ab.py`` suppresses a study on a divergence, so an epoch that diverges is not a referee."""

    record = recorded_summary(tmp_path)
    run = run_tree(tmp_path / "run", links=(REQUESTED,), parity=parity())
    judged = check.judge_sample(run_dir=run, exit_code=1, recorded=record)

    assert judged["verdict"] == check.VERDICT_DIVERGED
    assert "rowCount" in judged["problems"][0]["units"]
    assert str(record["rowCount"]) in judged["problems"][0]["detail"]


def test_missing_receipts_are_a_harness_error_not_a_pass(tmp_path: Path) -> None:
    """ "I could not read the outcome" must never render as verified, whatever the exit code was."""

    run = run_tree(tmp_path / "run", parity=None)
    judged = check.judge_sample(run_dir=run, exit_code=0, recorded=recorded_summary(tmp_path))

    assert judged["verdict"] == check.VERDICT_HARNESS
    assert harness.PARITY_FILE in judged["problems"][0]["detail"]


def test_an_unexplainable_parity_failure_is_unjudged_rather_than_verified(tmp_path: Path) -> None:
    """The driver said parity failed; if no re-derived unit can show it, that is not a pass."""

    run = run_tree(tmp_path / "run", parity=parity())
    judged = check.judge_sample(run_dir=run, exit_code=1, recorded=harness.run_summary(run))

    assert judged["verdict"] == check.VERDICT_HARNESS
    assert "parity-failed" in judged["problems"][0]["detail"]
    assert judged["divergences"] == []


def test_a_driver_that_never_ran_is_a_harness_error_with_no_exit_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise harness.HarnessError("driver timed out after 5s")

    monkeypatch.setattr(check.harness, "run_driver", boom)
    result = check.run_sample(
        epoch_dir=tmp_path,
        run_dir=tmp_path / "run",
        scope="subset",
        config={},
        recorded={"rowCount": 1},
        index=0,
        label="check",
        timeout_s=5,
    )

    assert result["exitCode"] == check._NO_PROCESS_EXIT
    assert result["verdict"] == check.VERDICT_HARNESS
    assert "timed out" in result["problems"][0]["detail"]


def test_a_recorded_configuration_is_reproduced_flag_for_flag(tmp_path: Path) -> None:
    command, env_extra = check.build_epoch_command(
        epoch_dir=tmp_path / "epoch",
        run_dir=tmp_path / "run",
        scope="subset",
        config={
            "families": ["google_sheets"],
            "onlySources": [],
            "allSources": False,
            "staticSources": 8,
            "maxWorkers": 2,
            "adapterHttpConcurrency": 32,
            "googleSheetsRedirectConcurrency": 64,
            "surfaces": list(check.ALL_SURFACES),
            "staticSourceTimeBudgetS": "1800",
        },
        label="check",
    )

    assert command[command.index("--mode") + 1] == "replay"
    assert command[command.index("--corpus-dir") + 1] == str(tmp_path / "epoch")
    assert command[command.index("--sources") + 1] == "8"
    assert command[command.index("--families") + 1] == "google_sheets"
    assert command[command.index("--max-workers") + 1] == "2"
    assert command[command.index("--adapter-http-concurrency") + 1] == "32"
    assert command[command.index("--google-sheets-redirect-concurrency") + 1] == "64"
    assert "--no-surfaces" not in command
    # The clock travels as the environment variable its own knob names, so the check cannot
    # replay under a different clock than the one it reports.
    assert env_extra == {truncation.KNOB_STATIC_SOURCE_CLOCK: "1800"}


def test_a_recorded_zero_is_passed_explicitly_because_the_defaults_are_not_zero(
    tmp_path: Path,
) -> None:
    """Omitting ``--sources`` means 8 and omitting ``--families`` means google_sheets.

    A record that held zero/empty values would otherwise be replayed against a *different* source
    selection, and the misses that followed would be reported as a corpus hole that is not there.
    """

    command, env_extra = check.build_epoch_command(
        epoch_dir=tmp_path,
        run_dir=tmp_path / "run",
        scope="subset",
        config={"families": [], "staticSources": 0},
        label="check",
    )

    sources_at = command.index("--sources")
    assert ["--sources", "0"] == command[sources_at : sources_at + 2]
    assert command[command.index("--families") + 1] == ""
    assert env_extra == {}


def test_a_surface_the_record_left_unpatched_stays_unpatched_in_the_replay(
    tmp_path: Path,
) -> None:
    command, _ = check.build_epoch_command(
        epoch_dir=tmp_path,
        run_dir=tmp_path / "run",
        scope="subset",
        config={"families": ["google_sheets"], "surfaces": ["fetch_text", "urlopen"]},
        label="check",
    )

    assert command[command.index("--no-surfaces") + 1] == "httpx"


def test_a_config_less_record_replays_on_the_defaults_and_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Epochs recorded before ``recordedConfig`` existed: a weaker statement, labelled."""

    seen: dict[str, Any] = {}

    def spy(**kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return sample()

    monkeypatch.setattr(check, "run_sample", spy)
    directory = epoch_dir(tmp_path, "corpus-old-20261009-120000")

    result = check.check_epoch(listing_row(directory), out_dir=tmp_path / "out")

    assert result["configSource"] == check.CONFIG_DEFAULTS
    assert seen["config"] == {}
    assert result["verdict"] == check.VERDICT_VERIFIED
