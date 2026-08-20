"""Read-only deduplication evidence for fetch reports.

AI boundary owns: dedup evidence report rows, source overlap summaries, and read-only fetch-report diagnostics.
AI boundary implement in: this file for evidence presentation; dedup policy and canonical identity stay in dedup/canonicalize.
AI boundary search before contracts: fetch-report contracts, bridge report normalization, and dedup evidence tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused dedup evidence tests.
"""

from __future__ import annotations

from src.jobs.common.dedup_evidence_audit_gate_payload import build_dedup_audit_gate
from src.jobs.common.dedup_evidence_bundle_report import build_dedup_evidence
