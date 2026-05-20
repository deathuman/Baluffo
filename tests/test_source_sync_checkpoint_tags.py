from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.source_sync_checkpoint_tags import build_checkpoint_tag_plan, checkpoint_tag_names

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "source-sync-checkpoints.yml"


def test_checkpoint_tag_names_use_utc_date() -> None:
    validated_at = datetime(2026, 5, 4, 22, 15, tzinfo=UTC)

    assert checkpoint_tag_names(validated_at) == (
        "last-known-good",
        "rollback-2026-05-04",
    )


def test_checkpoint_plan_publishes_only_for_successful_main_pushes() -> None:
    validated_at = datetime(2026, 5, 4, 22, 15, tzinfo=UTC)

    plan = build_checkpoint_tag_plan(
        branch_name="refs/heads/main",
        validation_conclusion="success",
        commit_sha="a" * 40,
        validated_at=validated_at,
    )

    assert plan.publish is True
    assert plan.branch_name == "main"
    assert plan.validation_conclusion == "success"
    assert plan.commit_sha == "a" * 40
    assert plan.tag_names == ("last-known-good", "rollback-2026-05-04")
    assert plan.skip_reason == ""

    rerun = build_checkpoint_tag_plan(
        branch_name="main",
        validation_conclusion="success",
        commit_sha="b" * 40,
        validated_at=validated_at,
    )

    assert rerun.publish is True
    assert rerun.tag_names == plan.tag_names
    assert rerun.commit_sha == "b" * 40


@pytest.mark.parametrize(
    ("branch_name", "validation_conclusion", "skip_reason"),
    [
        ("feature/checkpoints", "success", "branch_not_main"),
        ("main", "failure", "validation_not_success"),
    ],
)
def test_checkpoint_plan_skips_non_main_or_failed_validation(
    branch_name: str,
    validation_conclusion: str,
    skip_reason: str,
) -> None:
    plan = build_checkpoint_tag_plan(
        branch_name=branch_name,
        validation_conclusion=validation_conclusion,
        commit_sha="a" * 40,
        validated_at=datetime(2026, 5, 4, 22, 15, tzinfo=UTC),
    )

    assert plan.publish is False
    assert plan.tag_names == ()
    assert plan.skip_reason == skip_reason


def test_checkpoint_plan_skips_invalid_commit_sha() -> None:
    plan = build_checkpoint_tag_plan(
        branch_name="main",
        validation_conclusion="success",
        commit_sha="abc123",
        validated_at=datetime(2026, 5, 4, 22, 15, tzinfo=UTC),
    )

    assert plan.publish is False
    assert plan.tag_names == ()
    assert plan.skip_reason == "invalid_commit_sha"


def test_checkpoint_workflow_targets_validate_source_sync_and_writes_tags() -> None:
    text = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "workflow_run" in text
    assert "validate-source-sync" in text
    assert "contents: write" in text
    assert "github.event.workflow_run.head_branch == 'main'" in text
    assert "github.event.workflow_run.event == 'push'" in text
    assert "ref: main" in text
