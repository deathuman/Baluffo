"""Shared fixtures for the replay check's tests: fabricated run receipts and epoch manifests.

The check reads two things -- a run's artifacts and the epoch listing -- and both halves of its
suite build them. Kept here so the receipts module and the gate module cannot disagree about what a
receipt looks like: a divergence in the fixtures would show up as one suite passing while the other
tests a shape the driver never writes.

Everything is written on disk and read back through the real readers (``harness.run_summary``,
``replay_check.judge_sample``), so a fixture cannot pass while standing for something the driver
could not produce.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tools.measurements.corpus import epoch as epoch_mod
from tools.measurements.corpus import harness, referees, truncation
from tools.measurements.corpus import replay_check as check

__all__ = [
    "LINKS",
    "REQUESTED",
    "UNREQUESTED",
    "epoch_dir",
    "judge",
    "listing_row",
    "miss",
    "parity",
    "recorded_summary",
    "run_tree",
    "sample",
]

LINKS = ("https://boards.example/job/1", "https://boards.example/job/2")
REQUESTED = LINKS[0]
UNREQUESTED = LINKS[1]


def run_tree(
    path: Path,
    *,
    links: tuple[str, ...] = LINKS,
    statuses: tuple[str, ...] = ("ok", "ok"),
    parity: dict[str, Any] | None = None,
) -> Path:
    """A run directory as the driver writes one: artifacts under ``data/``, parity beside it."""

    data = path / "data"
    data.mkdir(parents=True, exist_ok=True)
    (data / "jobs-unified.json").write_text(
        json.dumps([{"jobLink": link, "title": "Job"} for link in links]) + "\n",
        encoding="utf-8",
    )
    (data / "jobs-fetch-report.json").write_text(
        json.dumps(
            {
                "runtime": {"googleSheetsRedirectConcurrency": 16},
                "sources": [
                    {"name": f"source-{index}", "status": status}
                    for index, status in enumerate(statuses)
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    if parity is not None:
        (path / "replay-parity.json").write_text(json.dumps(parity) + "\n", encoding="utf-8")
    return path


def recorded_summary(root: Path) -> dict[str, Any]:
    """What a record's summary looks like, computed by the same reader the driver used."""

    return harness.run_summary(run_tree(root / "record"))


def parity(**overrides: Any) -> dict[str, Any]:
    """A clean ``replay-parity.json``; overrides express the finding under test."""

    payload: dict[str, Any] = {
        "status": "parity-ok",
        "ledger": {
            "requestsOffered": 4,
            "requestsHit": 4,
            "requestsMissed": 0,
            "unattributedCount": 0,
        },
        "misses": [],
        "guard": {"guarded": True, "blocked": 0, "blockedRemote": 0, "blockedLoopback": 0},
        "pipelineError": "",
    }
    payload.update(overrides)
    return payload


def miss(url: str, *, surface: str = "httpx") -> dict[str, str]:
    return {
        "url": url,
        "key": "k-" + url.rsplit("/", 1)[-1],
        "reason": "no_entry",
        "surface": surface,
    }


def judge(
    root: Path, *, parity: dict[str, Any] | None, exit_code: int = 0, wall_ms: int = 1234
) -> dict[str, Any]:
    """One sample's verdict, read from receipts written under ``root``."""

    run = run_tree(root / "run", parity=parity)
    return check.judge_sample(
        run_dir=run, exit_code=exit_code, recorded=recorded_summary(root), wall_ms=wall_ms
    )


def epoch_dir(
    root: Path,
    name: str,
    *,
    recorded_config: dict[str, Any] | None = None,
    eligible: bool = True,
    scope: str = "subset",
    when: datetime | None = None,
) -> Path:
    """One epoch directory, written by the epoch module's own writer."""

    extra = {"recordedConfig": recorded_config} if recorded_config is not None else None
    directory = root / name
    manifest = epoch_mod.create_epoch(
        directory,
        scope=scope,
        counts={"entries": 1, "uniqueBodies": 1},
        now=when or datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC),
        extra=extra,
    )
    manifest["deltaReferee"] = (
        {"eligible": True, "accepted": False, "blocking": [], "knobs": [], "notes": []}
        if eligible
        else {
            "eligible": False,
            "accepted": False,
            "blocking": [
                {"reason": "redirect_pool", "knob": truncation.KNOB_REDIRECT_POOL, "detail": "d"}
            ],
            "knobs": [truncation.KNOB_REDIRECT_POOL],
            "notes": [],
        }
    )
    epoch_mod.write_epoch(directory, manifest)
    return directory


def listing_row(directory: Path) -> dict[str, Any]:
    """The listing's row for an epoch, built by the listing itself."""

    return referees.epoch_row(directory, corpus_parent=directory.parent)


def sample(**overrides: Any) -> dict[str, Any]:
    """One replay sample's record, as ``run_sample`` returns it."""

    payload: dict[str, Any] = {
        "index": 0,
        "runDir": "/tmp/run",
        "command": ["python", "run_fetch.py"],
        "envExtra": {},
        "exitCode": 0,
        "wallMs": 5,
        "status": "parity-ok",
        "verdict": check.VERDICT_VERIFIED,
        "problems": [],
        "matches": {},
        "divergences": [],
        "misses": [],
        "ledger": {},
        "guard": {},
        "guardBlockedSources": [],
        "pipelineError": "",
        "recordedRowCount": 2,
        "replayedRowCount": 2,
        "stderrTail": "",
    }
    payload.update(overrides)
    return payload
