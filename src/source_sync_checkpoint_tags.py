from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime

CHECKPOINT_TAG_NAME = "last-known-good"
CHECKPOINT_ROLLBACK_TAG_PREFIX = "rollback-"
_FULL_SHA_RE = re.compile(r"\A[0-9a-fA-F]{40}\Z")


@dataclass(frozen=True)
class CheckpointTagPlan:
    publish: bool
    branch_name: str
    validation_conclusion: str
    commit_sha: str
    checkpoint_date: date
    tag_names: tuple[str, ...] = ()
    skip_reason: str = ""


def _checkpoint_date(value: date | datetime | None = None) -> date:
    if value is None:
        return datetime.now(UTC).date()
    if isinstance(value, datetime):
        normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
        return normalized.astimezone(UTC).date()
    if isinstance(value, date):
        return value
    raise TypeError("validated_at must be a date, datetime, or None")


def _normalize_branch_name(value: object) -> str:
    text = str(value or "").strip()
    if text.startswith("refs/heads/"):
        text = text.removeprefix("refs/heads/")
    return text


def _normalize_conclusion(value: object) -> str:
    return str(value or "").strip().lower()


def checkpoint_rollback_tag_name(validated_at: date | datetime | None = None) -> str:
    return f"{CHECKPOINT_ROLLBACK_TAG_PREFIX}{_checkpoint_date(validated_at).isoformat()}"


def checkpoint_tag_names(validated_at: date | datetime | None = None) -> tuple[str, ...]:
    return CHECKPOINT_TAG_NAME, checkpoint_rollback_tag_name(validated_at)


def build_checkpoint_tag_plan(
    *,
    branch_name: object,
    validation_conclusion: object,
    commit_sha: object,
    validated_at: date | datetime | None = None,
) -> CheckpointTagPlan:
    normalized_branch = _normalize_branch_name(branch_name)
    normalized_conclusion = _normalize_conclusion(validation_conclusion)
    normalized_sha = str(commit_sha or "").strip()
    checkpoint_date = _checkpoint_date(validated_at)
    if normalized_branch != "main":
        return CheckpointTagPlan(
            False,
            normalized_branch,
            normalized_conclusion,
            normalized_sha,
            checkpoint_date,
            (),
            "branch_not_main",
        )
    if normalized_conclusion != "success":
        return CheckpointTagPlan(
            False,
            normalized_branch,
            normalized_conclusion,
            normalized_sha,
            checkpoint_date,
            (),
            "validation_not_success",
        )
    if not normalized_sha:
        return CheckpointTagPlan(
            False,
            normalized_branch,
            normalized_conclusion,
            normalized_sha,
            checkpoint_date,
            (),
            "missing_commit_sha",
        )
    if not _FULL_SHA_RE.fullmatch(normalized_sha):
        return CheckpointTagPlan(
            False,
            normalized_branch,
            normalized_conclusion,
            normalized_sha,
            checkpoint_date,
            (),
            "invalid_commit_sha",
        )
    return CheckpointTagPlan(
        True,
        normalized_branch,
        normalized_conclusion,
        normalized_sha,
        checkpoint_date,
        checkpoint_tag_names(checkpoint_date),
        "",
    )
