import json
from argparse import Namespace
from pathlib import Path

from scripts import jobs_yield_gate as gate


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_collect_dead_source_candidates_ranks_pending_and_repeated_zero_static(
    tmp_path: Path,
) -> None:
    pending_dead = {
        "name": "Dead Pending",
        "studio": "Dead Pending",
        "adapter": "static",
        "pendingReason": "unsupported_static_source",
        "pages": ["https://dead.example/careers"],
        "listing_url": "https://dead.example/careers",
    }
    active_dead = {
        "name": "Dead Active",
        "studio": "Dead Active",
        "adapter": "static",
        "pages": ["https://active-dead.example/jobs"],
        "listing_url": "https://active-dead.example/jobs",
    }
    active_blocked = {
        "name": "Blocked Active",
        "studio": "Blocked Active",
        "adapter": "static",
        "pages": ["https://blocked.example/jobs"],
        "listing_url": "https://blocked.example/jobs",
    }
    tombstoned = {
        "name": "Already Deleted",
        "studio": "Already Deleted",
        "adapter": "static",
        "pendingReason": "stale_or_dead_static_source",
        "pages": ["https://deleted.example/jobs"],
        "listing_url": "https://deleted.example/jobs",
    }
    active_path = _write_json(tmp_path / "active.json", [active_dead, active_blocked])
    pending_path = _write_json(tmp_path / "pending.json", [pending_dead, tombstoned])
    deleted_source_name = gate.static_source_name_for_registry_row(tombstoned)
    tombstones_path = _write_json(tmp_path / "tombstones.json", {deleted_source_name: {}})
    active_source_name = gate.static_source_name_for_registry_row(active_dead)
    blocked_source_name = gate.static_source_name_for_registry_row(active_blocked)
    state_path = _write_json(
        tmp_path / "state.json",
        {
            "sources": {
                active_source_name: {
                    "lastKeptCount": 0,
                    "consecutiveZeroKept": 4,
                    "lastFailureBucket": "site_changed",
                },
                blocked_source_name: {
                    "lastKeptCount": 0,
                    "consecutiveZeroKept": 5,
                    "lastFailureBucket": "blocked_or_challenge",
                    "browserEscalationEligible": True,
                },
            }
        },
    )

    candidates = gate.collect_dead_source_candidates(
        active_path=active_path,
        pending_path=pending_path,
        tombstones_path=tombstones_path,
        state_path=state_path,
    )

    assert [row["name"] for row in candidates] == ["Dead Pending", "Dead Active"]
    assert candidates[0]["bucket"] == "pending"
    assert candidates[0]["sourceRow"]["pendingReason"] == "unsupported_static_source"
    assert candidates[1]["consecutiveZeroKept"] == 4


def test_dead_source_registry_emits_temporary_active_rows(tmp_path: Path, capsys) -> None:
    candidates_path = _write_json(
        tmp_path / "candidates.json",
        [
            {
                "sourceName": "static_source::dead",
                "sourceRow": {
                    "name": "Dead Pending",
                    "adapter": "static",
                    "pendingReason": "stale_or_dead_static_source",
                    "pages": ["https://dead.example/jobs"],
                },
            }
        ],
    )

    assert gate.dead_source_registry(Namespace(candidates=str(candidates_path))) == 0

    rows = json.loads(capsys.readouterr().out)
    assert rows == [
        {
            "name": "Dead Pending",
            "adapter": "static",
            "pages": ["https://dead.example/jobs"],
            "enabledByDefault": True,
            "registryState": "active",
        }
    ]


def test_collect_dead_source_decisions_requires_two_zero_nonrecoverable_passes(
    tmp_path: Path,
) -> None:
    candidates_path = _write_json(
        tmp_path / "candidates.json",
        [
            {"sourceName": "static_source::dead", "reason": "stale_or_dead_static_source"},
            {"sourceName": "static_source::live", "reason": "stale_or_dead_static_source"},
            {"sourceName": "static_source::blocked", "reason": "site_changed_static_source"},
        ],
    )
    first_report = {
        "sources": [
            {
                "name": "static_source::dead",
                "status": "ok",
                "keptCount": 0,
                "classification": "site_changed",
            },
            {
                "name": "static_source::live",
                "status": "ok",
                "keptCount": 0,
                "classification": "site_changed",
            },
            {
                "name": "static_source::blocked",
                "status": "error",
                "keptCount": 0,
                "classification": "blocked_or_challenge",
                "browserFallbackRecommended": True,
            },
        ]
    }
    second_report = {
        "sources": [
            {
                "name": "static_source::dead",
                "status": "ok",
                "keptCount": 0,
                "failureBucket": "dead_listing_page",
            },
            {
                "name": "static_source::live",
                "status": "ok",
                "keptCount": 1,
                "classification": "ok_with_jobs",
            },
            {
                "name": "static_source::blocked",
                "status": "error",
                "keptCount": 0,
                "classification": "blocked_or_challenge",
                "browserFallbackRecommended": True,
            },
        ]
    }

    decisions = gate.collect_dead_source_decisions(
        first_report=first_report,
        second_report=second_report,
        candidates_path=candidates_path,
    )
    by_source = {row["sourceName"]: row for row in decisions}

    assert by_source["static_source::dead"]["deleteEligible"] is True
    assert by_source["static_source::live"]["decision"] == "defer_nonzero_yield"
    assert by_source["static_source::blocked"]["decision"] == "defer_recoverable"
