from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from src.jobs.common.source_bundle_reconciliation import (
    apply_source_bundle_reconciliation,
    plan_source_bundle_reconciliation,
)
from tools.source_bundle_reconcile import main

_SOURCE_LINK = "https://epicgames.com/careers/jobs/6148799004?gh_jid=6148799004"
_TARGET_LINK = "https://epicgames.com/careers/jobs/6173450004?gh_jid=6173450004"
_CADENCE_LINK = (
    "https://cadence.wd1.myworkdayjobs.com/en-US/External_Careers/job/AUSTIN/"
    "DFT-Design-Engineer_R53708"
)


def _entry(source: str, source_job_id: str, link: str) -> dict[str, Any]:
    return {
        "source": source,
        "sourceJobId": source_job_id,
        "jobLink": link,
        "postedAt": "",
        "adapter": "",
        "studio": "",
    }


def _row(
    row_id: int,
    *,
    link: str,
    bundle: list[dict[str, Any]],
    count: int | None = None,
) -> dict[str, Any]:
    return {
        "id": row_id,
        "title": "Level Design Intern",
        "company": "Epic Games",
        "jobLink": link,
        "sourceJobId": f"epic:{row_id}",
        "sourceBundle": bundle,
        "sourceBundleCount": len(bundle) if count is None else count,
    }


def _epic_rows() -> list[dict[str, Any]]:
    return [
        _row(
            7346,
            link=_SOURCE_LINK,
            bundle=[
                _entry("google_sheets_1er2oaxo", "sheet-3719", _SOURCE_LINK),
                _entry("google_sheets_1er2oaxo", "sheet-4405", _TARGET_LINK),
                _entry("epic_games_careers", "epic:6148799004", _SOURCE_LINK),
            ],
        ),
        _row(
            7401,
            link=_TARGET_LINK,
            bundle=[_entry("epic_games_careers", "epic:6173450004", _TARGET_LINK)],
        ),
        {
            **_row(
                31382,
                link=_CADENCE_LINK,
                bundle=[_entry("google_sheets", "sheet-4405", _CADENCE_LINK)],
            ),
            "title": "DFT Design Engineer",
            "company": "Cadence",
        },
    ]


def test_plan_is_read_only_and_apply_moves_entry_to_owning_row() -> None:
    rows = _epic_rows()
    plan = plan_source_bundle_reconciliation(rows)

    assert plan.mismatch_count == 1
    assert len(plan.moves) == 1
    assert not plan.ambiguous
    assert rows[0]["sourceBundleCount"] == 3
    assert {entry["sourceJobId"] for entry in rows[0]["sourceBundle"]} == {
        "sheet-3719",
        "sheet-4405",
        "epic:6148799004",
    }

    apply_source_bundle_reconciliation(rows, plan)

    assert rows[0]["sourceBundleCount"] == 2
    assert {entry["sourceJobId"] for entry in rows[0]["sourceBundle"]} == {
        "sheet-3719",
        "epic:6148799004",
    }
    assert rows[1]["sourceBundleCount"] == 2
    assert {entry["sourceJobId"] for entry in rows[1]["sourceBundle"]} == {
        "sheet-4405",
        "epic:6173450004",
    }
    assert rows[2]["sourceBundleCount"] == 1
    assert rows[2]["sourceBundle"][0]["sourceJobId"] == "sheet-4405"
    assert rows[2]["jobLink"] == _CADENCE_LINK


def test_apply_deduplicates_an_entry_already_present_on_target() -> None:
    entry = _entry("google_sheets_1er2oaxo", "sheet-4405", _TARGET_LINK)
    rows = [
        _row(1, link=_SOURCE_LINK, bundle=[entry], count=1),
        _row(2, link=_TARGET_LINK, bundle=[dict(entry)], count=1),
    ]

    plan = plan_source_bundle_reconciliation(rows)
    assert plan.moves[0].target_has_entry is True

    apply_source_bundle_reconciliation(rows, plan)

    assert rows[0]["sourceBundleCount"] == 0
    assert rows[0]["sourceBundle"] == []
    assert rows[1]["sourceBundleCount"] == 1
    assert len(rows[1]["sourceBundle"]) == 1


def test_ambiguous_primary_target_is_reported_not_moved() -> None:
    entry = _entry("google_sheets_1er2oaxo", "sheet-4405", _TARGET_LINK)
    rows = [
        _row(1, link=_SOURCE_LINK, bundle=[entry], count=1),
        _row(2, link=_TARGET_LINK, bundle=[], count=0),
        _row(3, link=_TARGET_LINK, bundle=[], count=0),
    ]

    plan = plan_source_bundle_reconciliation(rows)
    assert not plan.moves
    assert len(plan.ambiguous) == 1
    assert plan.ambiguous[0].candidate_indices == (1, 2)

    apply_source_bundle_reconciliation(rows, plan)

    assert rows[0]["sourceBundleCount"] == 1
    assert rows[0]["sourceBundle"] == [entry]


def test_entries_that_match_their_own_row_or_have_no_link_are_ignored() -> None:
    rows = [
        _row(
            1,
            link=_SOURCE_LINK,
            bundle=[
                _entry("google_sheets_1er2oaxo", "sheet-self", _SOURCE_LINK),
                _entry("google_sheets_1er2oaxo", "sheet-empty", ""),
            ],
        )
    ]

    plan = plan_source_bundle_reconciliation(rows)

    assert plan.mismatch_count == 0


def test_cli_json_output_is_ascii_safe_for_legacy_consoles(tmp_path: Path, capsys: Any) -> None:
    rows = _epic_rows()
    rows[0]["title"] = "Level Design Intern 東京"
    feed = tmp_path / "jobs.json"
    feed.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")

    assert main(["--feed", str(feed), "--json"]) == 1

    output = capsys.readouterr().out
    output.encode("ascii")
    report = json.loads(output)
    assert report["moveCount"] == 1
    assert report["moves"][0]["sourceTitle"] == "Level Design Intern 東京"


def test_cli_dry_run_then_apply_jsonl(tmp_path: Path) -> None:
    feed = tmp_path / "jobs.jsonl"
    feed.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in _epic_rows()) + "\n",
        encoding="utf-8",
    )
    backup_dir = tmp_path / "backup"

    assert main(["--feed", str(feed)]) == 1
    before = [json.loads(line) for line in feed.read_text(encoding="utf-8").splitlines()]
    assert before[0]["sourceBundleCount"] == 3

    assert (
        main(
            [
                "--feed",
                str(feed),
                "--apply",
                "--backup-dir",
                str(backup_dir),
                "--json",
            ]
        )
        == 0
    )
    after = [json.loads(line) for line in feed.read_text(encoding="utf-8").splitlines()]
    assert after[0]["sourceBundleCount"] == 2
    assert after[1]["sourceBundleCount"] == 2
    assert after[2]["sourceBundleCount"] == 1
    assert (backup_dir / feed.name).exists()
