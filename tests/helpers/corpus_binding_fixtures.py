"""Fixtures for the conflict-class binding: fabricated record/replay runs and the epoch around them.

The binding reads three things -- the epoch's conflict report, the record's rows and the replay's --
so both halves of its suite build the same three from disk and read them back through the real
readers (``binding.bind``, ``harness.run_summary``). Kept here rather than in one of the suites so a
fixture shape cannot drift from the module that judges it: a divergence in the fixtures would show
up as one suite passing while the other tests an epoch the record never writes.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from tools.measurements.corpus import conflicts
from tools.measurements.corpus import epoch as epoch_mod

__all__ = [
    "LINKEDIN",
    "SHEET",
    "declare",
    "diverging_pair",
    "epoch_dir",
    "row",
    "run_tree",
]

LINKEDIN = "www.linkedin.com"
SHEET = f"static_source::static:listing_url:https://{LINKEDIN}/company/studio/jobs/"
BOARDS = "static_source::static:listing_url:https://boards.example.com/jobs"


def row(
    link: str,
    *,
    source: str = SHEET,
    company: str = "Studio",
    source_job_id: str = "static:Studio-1",
    sector: str = "Game",
    bundles: tuple[str, ...] = (),
) -> dict[str, Any]:
    """One kept row, with the fields the parity comparison reads and a source that names a host."""

    payload: dict[str, Any] = {
        "jobLink": link,
        "title": "Artist",
        "company": company,
        "city": "Remote",
        "country": "Remote",
        "workType": "Remote",
        "contractType": "Full-time",
        "sector": sector,
        "profession": "Art",
        "source": source,
        "sourceJobId": source_job_id,
        "status": "active",
        "postedAt": "2026-03-01",
    }
    if bundles:
        payload["sourceBundle"] = [
            {"source": bundle, "sourceJobId": source_job_id, "jobLink": link} for bundle in bundles
        ]
    return payload


def run_tree(root: Path, rows: list[dict[str, Any]]) -> Path:
    """A run directory as the driver writes one: rows under ``data/`` beside the run's receipts.

    The parity receipt is written too: a run whose outcome is not on disk is a harness error before
    any divergence is judged, which would make a binding test pass for the wrong reason.
    """

    data = root / "data"
    data.mkdir(parents=True, exist_ok=True)
    (data / "jobs-unified.json").write_text(json.dumps(rows) + "\n", encoding="utf-8")
    (data / "jobs-fetch-report.json").write_text(
        json.dumps({"sources": [{"name": "studio", "status": "ok"}]}) + "\n", encoding="utf-8"
    )
    (root / "replay-parity.json").write_text(
        json.dumps(
            {
                "status": "parity-failed",
                "ledger": {
                    "requestsOffered": 4,
                    "requestsHit": 4,
                    "requestsMissed": 0,
                    "unattributedCount": 0,
                },
                "misses": [],
                "guard": {
                    "guarded": True,
                    "blocked": 0,
                    "blockedRemote": 0,
                    "blockedLoopback": 0,
                },
                "pipelineError": "",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return root


def declare(epoch_dir: Path, *hosts: str) -> None:
    """A conflict report naming exactly these hosts, written where the audit writes one."""

    report: dict[str, Any] = {
        "schemaVersion": 1,
        "scope": "subset",
        "corpusDir": str(epoch_dir),
        "entries": len(hosts),
        "bodies": len(hosts),
        "replacedKeys": len(hosts),
        "replacements": len(hosts),
        "replacementsByKind": {"body-changed": len(hosts)} if hosts else {},
        "orphaningReplacements": len(hosts),
        "orphanedBodies": len(hosts),
        "attributedOrphans": len(hosts),
        "unattributedOrphans": 0,
        "replacementLog": "present" if hosts else "absent",
        "hostCount": len(hosts),
        "hosts": [
            {
                "host": host,
                "replacedKeys": 1,
                "orphaningReplacements": 1,
                "orphanedBodies": 1,
                "kinds": {"body-changed": 1},
                "samples": [f"https://{host}/jobs"],
            }
            for host in hosts
        ],
        "status": "flagged" if hosts else "clean",
        "interpretation": conflicts.INTERPRETATION,
    }
    conflicts.write_report(report, epoch_dir / conflicts.REPORT_NAME)


def epoch_dir(root: Path, name: str, *, record_run: Path | None) -> Path:
    """One epoch directory: a manifest naming the record's run, written by the epoch writer."""

    directory = root / name
    manifest = epoch_mod.create_epoch(
        directory,
        scope="subset",
        counts={"entries": 1, "uniqueBodies": 1},
        extra={"recordedRunDir": str(record_run) if record_run is not None else ""},
    )
    manifest["deltaReferee"] = {
        "eligible": True,
        "accepted": False,
        "blocking": [],
        "knobs": [],
        "notes": [],
    }
    epoch_mod.write_epoch(directory, manifest)
    return directory


def diverging_pair(tmp_path: Path, *, declared: tuple[str, ...]) -> tuple[Path, Path]:
    """A record/replay pair that differs three ways on one host, plus the epoch declaring it.

    Record-only, replay-only and moved-field drifts in one fixture: the three shapes the residue
    distinguishes, all attributable to the same source host.
    """

    record_rows = [
        row("https://linkedin.com/job/1", company="Record Studio"),
        row("https://linkedin.com/job/2", source_job_id="static:Studio-2"),
        row("https://linkedin.com/job/3", company="Drifted", sector="Tech"),
    ]
    replay_rows = [
        row("https://linkedin.com/job/1", company="Record Studio"),
        row("https://linkedin.com/job/4", source_job_id="static:Studio-4"),
        row("https://linkedin.com/job/3", company="Other", sector="Tech"),
    ]
    record_run = run_tree(tmp_path / "record", record_rows)
    replay_run = run_tree(tmp_path / "replay", replay_rows)
    epoch = epoch_dir(tmp_path, "corpus-subset-20261010-170000", record_run=record_run)
    declare(epoch, *declared)
    return epoch, replay_run
