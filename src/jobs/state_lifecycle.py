"""Lifecycle-state helpers for the jobs pipeline.

AI boundary owns: jobs lifecycle state rows, terminal events, and lifecycle archive helpers.
AI boundary implement in: this file for lifecycle persistence semantics; bridge task lifecycle stays in bridge modules.
AI boundary search before contracts: pipeline finalization, runtime writers, DATA_CONTRACT.md, and lifecycle tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused lifecycle state tests.
"""

from __future__ import annotations

from .common import config as common_config

LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS = common_config.LIFECYCLE_REMOVE_TO_ARCHIVE_DAYS
LIFECYCLE_ARCHIVE_RETENTION_DAYS = common_config.LIFECYCLE_ARCHIVE_RETENTION_DAYS
AVAILABILITY_OVERDUE_FAILURE_COUNT = common_config.AVAILABILITY_OVERDUE_FAILURE_COUNT
AVAILABILITY_OVERDUE_DAYS = common_config.AVAILABILITY_OVERDUE_DAYS
AVAILABILITY_HISTORY_DAYS = common_config.AVAILABILITY_HISTORY_DAYS

from src.jobs.state_lifecycle_availability import (
    build_availability_history_payload,
    build_lifecycle_source_evidence,
)
from src.jobs.state_lifecycle_identity import (
    availability_id_for_job,
    job_identity_aliases,
)
from src.jobs.state_lifecycle_normalization import (
    lifecycle_archive_state_path,
    lifecycle_counts,
    lifecycle_state_fingerprint,
    normalize_job_lifecycle_payload,
    read_job_lifecycle_archive_state,
    read_job_lifecycle_state,
    write_job_lifecycle_archive_state,
    write_job_lifecycle_state,
)
from src.jobs.state_lifecycle_orchestration import apply_job_lifecycle_state
from src.jobs.state_lifecycle_transitions import apply_direct_availability_evidence
