"""Active audit recovery fetch-result application.

AI boundary owns: adapting directory recovery fetch results into the active-audit recovery application result.
AI boundary implement in: this file for recovery result adaptation; recovery fetching stays in GameDevMap batch plumbing.
AI boundary search before contracts: directory page recovery appliers and active audit recovery tests.
AI boundary verify: `python -m pytest tests/source_discovery/test_active_audit_runtime.py -q`.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from . import directory_page_recovery as directory_recovery_helpers
from .active_audit_contracts import (
    ActiveAuditRecoveryApplicationResult as ActiveAuditRecoveryApplicationResult,
)


def apply_active_audit_recovery_fetch_results(
    recovery_fetch_results: list[dict[str, Any]],
    *,
    grouped: dict[str, dict[str, Any]] | None = None,
    finalize: bool = True,
    apply_payload: directory_recovery_helpers.RecoveryPayloadApplier,
    finalize_group: directory_recovery_helpers.RecoveryGroupFinalizer,
    progress_label: str = "",
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> ActiveAuditRecoveryApplicationResult:
    output = directory_recovery_helpers.apply_recovery_fetch_results(
        recovery_fetch_results,
        grouped=grouped,
        finalize=finalize,
        apply_payload=apply_payload,
        finalize_group=finalize_group,
        progress_label=progress_label,
        progress_callback=progress_callback,
    )
    return ActiveAuditRecoveryApplicationResult(
        provider_candidates=list(output.provider_candidates),
        static_candidates=list(output.static_candidates),
        rejected_rows=list(output.rejected_rows),
        failures=list(output.failures),
        pages_fetched=int(output.pages_fetched),
        grouped_state=dict(output.grouped),
        recovered_homepages=set(output.recovered_homepages),
    )
