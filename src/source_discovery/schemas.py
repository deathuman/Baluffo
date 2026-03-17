"""
Pydantic schemas for source discovery report and summary.
Used at discovery output boundary to validate shape before writing report/candidates.
See docs/DATA_CONTRACT.md §7 for the source discovery contract.
"""
from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict, Field


class DiscoveryLossAccountingSchema(BaseModel):
    """Nested schema for summary.lossAccounting."""

    model_config = ConfigDict(extra="ignore")

    generated: int = 0
    dedupSkipped: int = 0
    dedupSkippedReasons: Dict[str, int] = Field(default_factory=dict)
    validationSkipped: int = 0
    lowEvidenceSkipped: int = 0
    probeFailed: int = 0
    queueFiltered: int = 0
    deferredByCap: int = 0
    queued: int = 0


class DiscoveryReportSummarySchema(BaseModel):
    """Schema for the summary object in source-discovery-report.json."""

    model_config = ConfigDict(extra="ignore")

    phase: str = ""
    phaseLabel: str = ""
    probedCount: int = 0
    healthyCount: int = 0
    newCandidateCount: int = 0
    taEnvCandidateCount: int = 0
    nlCandidateCount: int = 0
    failedProbeCount: int = 0
    probeMissCount: int = 0
    foundEndpointCount: int = 0
    probedCandidateCount: int = 0
    queuedCandidateCount: int = 0
    discoverableButDeferredCount: int = 0
    skippedDuplicateCount: int = 0
    skippedInvalidCount: int = 0
    skippedLowEvidenceProbeCount: int = 0
    adapterCounts: Dict[str, int] = Field(default_factory=dict)
    methodCounts: Dict[str, int] = Field(default_factory=dict)
    generatedCountByStage: Dict[str, int] = Field(default_factory=dict)
    survivedDedupeCountByStage: Dict[str, int] = Field(default_factory=dict)
    probedCountByStage: Dict[str, int] = Field(default_factory=dict)
    queuedCountByStage: Dict[str, int] = Field(default_factory=dict)
    duplicateReasons: Dict[str, int] = Field(default_factory=dict)
    deferredReasons: Dict[str, int] = Field(default_factory=dict)
    thresholds: Dict[str, Any] = Field(default_factory=dict)
    lossAccounting: DiscoveryLossAccountingSchema = Field(
        default_factory=DiscoveryLossAccountingSchema
    )


class DiscoveryReportSchema(BaseModel):
    """Schema for the full source-discovery-report.json."""

    model_config = ConfigDict(extra="ignore")

    schemaVersion: str = Field(default="1.0", description="Report schema version")
    mode: str = ""
    startedAt: str = ""
    finishedAt: str = ""
    summary: DiscoveryReportSummarySchema = Field(
        default_factory=DiscoveryReportSummarySchema
    )
    candidates: List[Dict[str, Any]] = Field(default_factory=list)
    failures: List[Dict[str, Any]] = Field(default_factory=list)
    topFailures: List[Dict[str, Any]] = Field(default_factory=list)
    outputs: Dict[str, str] = Field(default_factory=dict)
