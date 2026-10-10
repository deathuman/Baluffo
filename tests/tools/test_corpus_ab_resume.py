"""``--resume``: a study that was interrupted must be continued, not re-measured.

Long studies get cut off. Re-running finished arms would cost the machine time twice and, worse,
fold two machine states into one study, so ``--resume`` rebuilds a finished pair from its own
artifacts — the run report's window is the metric the verdict already uses — and re-runs anything
partial or configured differently in full. These tests pin the two halves of that contract: what a
reused pair is allowed to claim, and what disqualifies one.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.corpus import ab, run_fetch  # noqa: E402

# ── resume ──────────────────────────────────────────────────────────────────


def _fake_arm(
    root: Path,
    *,
    index: int,
    arm: str,
    wall_ms: int,
    concurrency: int,
    blocked_remote: int = 0,
    rows: int = 3,
    status: str = ab.PARITY_OK,
    epoch: str = "corpus-subset-x",
    mode: str = "replay",
    scope: str = "subset",
    max_workers: int = 1,
    families: list[str] | None = None,
    all_sources: bool = False,
) -> Path:
    """One arm's receipts on disk, in the shape the driver actually writes them."""

    run_dir = ab.pair_dir(root, index, arm)
    data = run_dir / "data"
    data.mkdir(parents=True, exist_ok=True)
    (data / "jobs-unified.json").write_text(
        json.dumps([{"jobLink": f"https://a.example/{n}"} for n in range(rows)]),
        encoding="utf-8",
    )
    started = datetime(2026, 10, 8, 19, 0, 0, tzinfo=UTC)
    (data / "jobs-fetch-report.json").write_text(
        json.dumps(
            {
                "startedAt": started.isoformat(),
                "finishedAt": (started + timedelta(milliseconds=wall_ms)).isoformat(),
                "sources": [{"name": "a", "status": "ok"}],
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "replay-parity.json").write_text(
        json.dumps(
            {
                "epochId": epoch,
                "epochScope": scope,
                "status": status,
                "guard": {"blockedRemote": blocked_remote},
                "runConfig": {
                    "mode": mode,
                    "scope": scope,
                    "maxWorkers": max_workers,
                    "adapterHttpConcurrency": concurrency,
                    "staticSources": 4,
                    "families": ["google_sheets"] if families is None else families,
                    "allSources": all_sources,
                },
            }
        ),
        encoding="utf-8",
    )
    return run_dir


def _reuse(
    root: Path,
    *,
    index: int = 0,
    concurrency: dict[str, int] | None = None,
    all_sources: bool = False,
) -> Any:
    return ab.reused_pair(
        index=index,
        out_dir=root,
        epoch_id="corpus-subset-x",
        labels={"a": "pool-64", "b": "pool-96"},
        scope="subset",
        sources=4,
        families="google_sheets",
        all_sources=all_sources,
        max_workers=1,
        concurrency=concurrency or {"a": 32, "b": 48},
    )


def test_resume_rebuilds_a_complete_pair_from_its_own_report_window(tmp_path: Path) -> None:
    """A reused pair must carry the metric the verdict uses -- not a placeholder.

    The process wall clock and exit code do not survive on disk, so they are marked as read from
    the artifacts; the report window does survive, and it is what the delta is computed from.
    """

    _fake_arm(tmp_path, index=0, arm="a", wall_ms=73000, concurrency=32)
    _fake_arm(tmp_path, index=0, arm="b", wall_ms=70000, concurrency=48)

    pair, reason = _reuse(tmp_path)

    assert reason == ""
    assert pair is not None and pair["resumed"] is True
    assert pair["order"] == ["a", "b"]
    assert pair["a"]["reportWallMs"] == 73000 and pair["b"]["reportWallMs"] == 70000
    assert pair["a"]["wallMs"] == 0
    assert pair["a"]["exitCode"] == 0 and "artifacts" in pair["a"]["exitCodeSource"]
    assert pair["a"]["rows"] == 3
    # The units are the run's own summary read back from its artifacts, not a copied placeholder.
    summary = run_fetch.run_summary(ab.pair_dir(tmp_path, 0, "a") / "data")
    assert pair["a"]["units"] == {unit: summary[unit] for unit in ab.PARITY_UNITS}
    assert pair["deltaPct"] == ab.paired_deltas([pair])[0] < 0


def test_resume_refuses_anything_that_is_not_this_experiment(tmp_path: Path) -> None:
    """A resumed study must not splice two experiments together."""

    # The candidate arm ran with the baseline's pool size: not the comparison being asked for.
    mismatch = tmp_path / "mismatch"
    _fake_arm(mismatch, index=0, arm="a", wall_ms=73000, concurrency=32)
    _fake_arm(mismatch, index=0, arm="b", wall_ms=70000, concurrency=32)
    pair, reason = _reuse(mismatch)
    assert pair is None and "adapterHttpConcurrency" in reason

    # An arm that reached the network is not a hermetic measurement.
    leaky = tmp_path / "leaky"
    _fake_arm(leaky, index=0, arm="a", wall_ms=73000, concurrency=32)
    _fake_arm(leaky, index=0, arm="b", wall_ms=70000, concurrency=48, blocked_remote=2)
    pair, reason = _reuse(leaky)
    assert pair is None and "not hermetic" in reason

    # A pair left by another epoch is a different corpus, whatever the layout says.
    other = tmp_path / "other"
    _fake_arm(other, index=0, arm="a", wall_ms=73000, concurrency=32, epoch="corpus-subset-other")
    _fake_arm(other, index=0, arm="b", wall_ms=70000, concurrency=48, epoch="corpus-subset-other")
    pair, reason = _reuse(other)
    assert pair is None and "recorded epoch" in reason

    # Same epoch, different worker count: a different pipeline configuration.
    workers = tmp_path / "workers"
    _fake_arm(workers, index=0, arm="a", wall_ms=73000, concurrency=32, max_workers=4)
    _fake_arm(workers, index=0, arm="b", wall_ms=70000, concurrency=48, max_workers=4)
    pair, reason = _reuse(workers)
    assert pair is None and "maxWorkers" in reason

    # A stride-sampled arm cannot stand in for a full-registry one: same epoch, same
    # workers, but the source *set* differs, so the two arms would measure different work.
    sampled = tmp_path / "sampled"
    _fake_arm(sampled, index=0, arm="a", wall_ms=73000, concurrency=32, all_sources=False)
    _fake_arm(sampled, index=0, arm="b", wall_ms=70000, concurrency=48, all_sources=False)
    pair, reason = _reuse(sampled, all_sources=True)
    assert pair is None and "allSources" in reason


def test_resume_skips_half_a_pair_and_says_nothing_about_an_absent_one(tmp_path: Path) -> None:
    _fake_arm(tmp_path, index=0, arm="a", wall_ms=73000, concurrency=32)

    pair, reason = _reuse(tmp_path)

    assert pair is None and reason.startswith("arm b")
    assert _reuse(tmp_path, index=4) == (None, "")


def test_run_ab_resume_reuses_pairs_and_runs_only_what_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The study-level half: pair 1 comes off disk, pair 2 actually runs."""

    out = tmp_path / "study"
    _fake_arm(out, index=0, arm="a", wall_ms=73000, concurrency=32)
    _fake_arm(out, index=0, arm="b", wall_ms=70000, concurrency=48)
    # The epoch's recorded units are the fake artifacts' own, so parity is graded on real values.
    units = {
        unit: run_fetch.run_summary(ab.pair_dir(out, 0, "a") / "data")[unit]
        for unit in ab.PARITY_UNITS
    }
    epoch = tmp_path / "epoch"
    epoch.mkdir()
    (epoch / "EPOCH.json").write_text(
        json.dumps(
            {
                "epochId": "corpus-subset-x",
                "scope": "subset",
                "recordedSummary": units,
                "deltaReferee": {
                    "eligible": True,
                    "accepted": False,
                    "blocking": [],
                    "knobs": [],
                    "notes": [],
                },
            }
        ),
        encoding="utf-8",
    )
    calls: list[dict[str, Any]] = []

    def fake_verify(*_: Any, **__: Any) -> dict[str, Any]:
        return {
            "epochId": "corpus-subset-x",
            "verdict": "verified",
            "receipt": str(out / "referee-verification" / ab.VERIFICATION_FILE),
        }

    monkeypatch.setattr(ab, "verify_referee", fake_verify)

    def fake_run_arm(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        run_dir = ab.pair_dir(out, int(kwargs["index"]), str(kwargs["arm"]))
        return {
            "label": kwargs["labels"][kwargs["arm"]],
            "runDir": str(run_dir),
            "command": "fake",
            "exitCode": 0,
            "wallMs": 74000,
            "reportWallMs": 72000,
            "rows": 3,
            "units": dict(units),
            "repeatedFetchSharePct": 0.0,
            "guard": {"blockedRemote": 0},
            "stderrTail": "",
        }

    monkeypatch.setattr(ab, "_run_arm", fake_run_arm)
    report = ab.run_ab(
        epoch_dir=epoch,
        out_dir=out,
        pairs=2,
        sources=4,
        families="google_sheets",
        max_workers=1,
        arm_a_concurrency=32,
        arm_b_concurrency=48,
        resume=True,
    )

    assert report["resume"] == {
        "requested": True,
        "reusedArms": 2,
        "reusedPairs": 1,
        "rerunPairs": 0,
    }
    assert [bool(pair.get("resumed")) for pair in report["pairs"]] == [True, False]
    assert report["arms"]["a"]["samples"] == [73000, 72000]
    assert report["arms"]["b"]["samples"] == [70000, 72000]
    # Only the missing pair ran, and only its own two arms.
    assert [(call["index"], call["arm"]) for call in calls] == [(1, "b"), (1, "a")]
    assert report["status"] == "ok" and report["parity"]["divergences"] == []
    assert report["referee"]["verdict"] == "verified"
    assert any("reused from disk" in note for note in report["notes"])
    assert "**Resumed:**" in ab.render_markdown(report)
