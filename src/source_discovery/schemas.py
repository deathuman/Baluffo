"""
Pydantic schemas for source discovery report and summary.
Used at discovery output boundary to validate shape before writing report/candidates.
See docs/DATA_CONTRACT.md §7 for the source discovery contract.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class DiscoveryLossAccountingSchema(BaseModel):
    """Nested schema for summary.lossAccounting."""

    model_config = ConfigDict(extra="ignore")

    generated: int = 0
    dedupSkipped: int = 0
    dedupSkippedReasons: dict[str, int] = Field(default_factory=dict)
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
    suppressedStaticCount: int = 0
    skippedDuplicateCount: int = 0
    skippedInvalidCount: int = 0
    skippedLowEvidenceProbeCount: int = 0
    adapterCounts: dict[str, int] = Field(default_factory=dict)
    queuedByAdapter: dict[str, int] = Field(default_factory=dict)
    deferredByAdapter: dict[str, int] = Field(default_factory=dict)
    healthyButDeferredByAdapter: dict[str, int] = Field(default_factory=dict)
    suppressedStaticByReason: dict[str, int] = Field(default_factory=dict)
    suppressedStaticByStage: dict[str, int] = Field(default_factory=dict)
    queuedProviderCount: int = 0
    queuedStaticCount: int = 0
    methodCounts: dict[str, int] = Field(default_factory=dict)
    generatedCountByStage: dict[str, int] = Field(default_factory=dict)
    survivedDedupeCountByStage: dict[str, int] = Field(default_factory=dict)
    probedCountByStage: dict[str, int] = Field(default_factory=dict)
    queuedCountByStage: dict[str, int] = Field(default_factory=dict)
    duplicateReasons: dict[str, int] = Field(default_factory=dict)
    deferredReasons: dict[str, int] = Field(default_factory=dict)
    thresholds: dict[str, Any] = Field(default_factory=dict)
    lossAccounting: DiscoveryLossAccountingSchema = Field(
        default_factory=DiscoveryLossAccountingSchema
    )


class DiscoveryTimingRowSchema(BaseModel):
    """Schema for discovery timing row payloads."""

    model_config = ConfigDict(extra="ignore")

    stage: str = ""
    adapter: str = ""
    durationMs: int = 0
    generatedCount: int = 0
    failureCount: int = 0
    probedCount: int = 0
    healthyCount: int = 0
    queuedCount: int = 0


class DiscoveryRuntimeSchema(BaseModel):
    """Schema for discovery runtime/timing details."""

    model_config = ConfigDict(extra="ignore")

    totalDurationMs: int = 0
    stageTimingsMs: dict[str, int] = Field(default_factory=dict)
    stageTop: list[dict[str, Any]] = Field(default_factory=list)
    adapterTimings: list[DiscoveryTimingRowSchema] = Field(default_factory=list)
    slowestAdapters: list[DiscoveryTimingRowSchema] = Field(default_factory=list)


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
    runtime: DiscoveryRuntimeSchema = Field(default_factory=DiscoveryRuntimeSchema)
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    failures: list[dict[str, Any]] = Field(default_factory=list)
    topFailures: list[dict[str, Any]] = Field(default_factory=list)
    outputs: dict[str, str] = Field(default_factory=dict)
