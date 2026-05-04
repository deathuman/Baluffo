"""Stable compatibility surface for jobs source-state and lifecycle helpers."""

from __future__ import annotations

from src.jobs.browser_fallback import BROWSER_FALLBACK_STATE_KEY

from . import state_incremental as state_incremental_mod
from . import state_lifecycle as state_lifecycle_mod
from . import state_source_state as state_source_state_mod

source_rows_fingerprint = state_source_state_mod.source_rows_fingerprint
normalize_source_state_payload = state_source_state_mod.normalize_source_state_payload
should_skip_static_source_for_structured_migration = (
    state_source_state_mod.should_skip_static_source_for_structured_migration
)
read_source_state = state_source_state_mod.read_source_state
write_source_state = state_source_state_mod.write_source_state
circuit_breaker_until = state_source_state_mod.circuit_breaker_until
apply_circuit_breaker_exclusions = state_source_state_mod.apply_circuit_breaker_exclusions
append_excluded_default_sources = state_source_state_mod.append_excluded_default_sources
update_source_state_rows = state_source_state_mod.update_source_state_rows
read_previously_successful_sources = state_source_state_mod.read_previously_successful_sources
read_success_cache = state_source_state_mod.read_success_cache
browser_fallback_state_row = state_source_state_mod.browser_fallback_state_row
build_browser_fallback_circuit_breaker = (
    state_source_state_mod.build_browser_fallback_circuit_breaker
)
set_browser_fallback_state = state_source_state_mod.set_browser_fallback_state
write_success_cache = state_source_state_mod.write_success_cache

normalize_job_lifecycle_payload = state_lifecycle_mod.normalize_job_lifecycle_payload
read_job_lifecycle_state = state_lifecycle_mod.read_job_lifecycle_state
write_job_lifecycle_state = state_lifecycle_mod.write_job_lifecycle_state
lifecycle_archive_state_path = state_lifecycle_mod.lifecycle_archive_state_path
read_job_lifecycle_archive_state = state_lifecycle_mod.read_job_lifecycle_archive_state
write_job_lifecycle_archive_state = state_lifecycle_mod.write_job_lifecycle_archive_state
lifecycle_counts = state_lifecycle_mod.lifecycle_counts
apply_job_lifecycle_state = state_lifecycle_mod.apply_job_lifecycle_state

should_skip_source_by_ttl = state_incremental_mod.should_skip_source_by_ttl
should_skip_source_by_cadence = state_incremental_mod.should_skip_source_by_cadence
get_incremental_cache_decision = state_incremental_mod.get_incremental_cache_decision

__all__ = [
    "BROWSER_FALLBACK_STATE_KEY",
    "source_rows_fingerprint",
    "normalize_source_state_payload",
    "should_skip_static_source_for_structured_migration",
    "read_source_state",
    "write_source_state",
    "circuit_breaker_until",
    "apply_circuit_breaker_exclusions",
    "append_excluded_default_sources",
    "update_source_state_rows",
    "read_previously_successful_sources",
    "read_success_cache",
    "browser_fallback_state_row",
    "build_browser_fallback_circuit_breaker",
    "set_browser_fallback_state",
    "write_success_cache",
    "normalize_job_lifecycle_payload",
    "read_job_lifecycle_state",
    "write_job_lifecycle_state",
    "lifecycle_archive_state_path",
    "read_job_lifecycle_archive_state",
    "write_job_lifecycle_archive_state",
    "lifecycle_counts",
    "apply_job_lifecycle_state",
    "should_skip_source_by_ttl",
    "should_skip_source_by_cadence",
    "get_incremental_cache_decision",
]
