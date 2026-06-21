"""Advisory contracts for read-only dedup evidence payloads.

AI boundary owns: dedup evidence payload contract normalization and advisory row shape helpers.
AI boundary implement in: this file for payload contracts only; evidence computation stays in dedup_evidence_* and reporting leaves.
AI boundary search before contracts: DATA_CONTRACT.md, fetch-report contracts, bridge report normalization, and dedup evidence tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused jobs contract tests.
"""

from __future__ import annotations

from typing import Any, TypedDict

type JsonObject = dict[str, Any]
type IntCountMap = dict[str, int]
type MergeExampleMap = dict[str, list[DedupMergeExampleRow]]


class DedupMergeExampleRow(TypedDict, total=False):
    title: str
    company: str
    dedupKey: str
    mergeReason: str
    mergeGateTier: str
    sourceBundleCount: int
    bundleEvidenceOrigin: str
    sampleSources: list[str]
    sampleLocations: list[str]
    primaryUrl: str
    targetUrl: str
    incomingUrl: str
    evidence: list[str]
    blockedMergeReason: str
    guardReason: str


class DedupReviewQueueRow(TypedDict, total=False):
    title: str
    company: str
    dedupKey: str
    sourceBundleCount: int
    bundleEvidenceOrigin: str
    recommendedReviewAction: str
    suspectedCause: str
    causeEvidence: list[str]
    identityShape: str
    identityQuality: str
    identityQualityEvidence: list[str]
    nonProviderIdentityProvenance: str
    nonProviderIdentityProvenanceEvidence: list[str]
    googleSheetsBundleShape: str
    googleSheetsRoleBucketAudit: str
    googleSheetsRoleBucketAuditEvidence: list[str]
    googleSheetsBucketIntent: str
    googleSheetsBucketIntentEvidence: list[str]
    googleSheetsWeakGroupingAudit: str
    googleSheetsWeakGroupingEvidence: list[str]
    sampleSources: list[str]
    sampleLocations: list[str]
    dedupReviewStatus: str
    dedupReviewAction: str
    dedupReviewUpdatedAt: str
    dedupReviewUpdatedBy: str
    reviewEvidence: list[str]


class ProviderStaticDisagreementRow(TypedDict, total=False):
    title: str
    company: str
    dedupKey: str
    bundleEvidenceOrigin: str
    sourceBundleCount: int
    providerSources: list[str]
    staticSources: list[str]
    providerSourceJobIds: list[str]
    staticSourceJobIds: list[str]
    providerUrls: list[str]
    staticUrls: list[str]
    providerUrlHosts: list[str]
    staticUrlHosts: list[str]
    providerUrlPathPrefixes: list[str]
    staticUrlPathPrefixes: list[str]
    sharedIdentifierTokens: list[str]
    concreteSharedIdentifierTokens: list[str]
    providerStaticOnly: bool
    distinctLocationCount: int
    sampleLocations: list[str]
    identityQuality: str
    outlierReason: str
    disagreementClassification: str
    disagreementClassificationEvidence: list[str]
    collisionReviewHint: str
    disagreementEvidence: list[str]
    disagreementGateDisposition: str
    disagreementGateEvidence: list[str]
    dedupReviewStatus: str
    dedupReviewAction: str
    dedupReviewUpdatedAt: str
    dedupReviewUpdatedBy: str
    reviewEvidence: list[str]
    carriedLocationPollutionAudit: str
    carriedLocationPollutionEvidence: list[str]


class GoogleSheetsRoleBucketAuditPayload(TypedDict, total=False):
    unresolvedRoleBucketCount: int
    blockedByDifferentPrimaryUrlCount: int
    likelyHistoricalCollisionCount: int
    fixedByGenericRoleGuardCount: int
    classificationCounts: IntCountMap
    examples: list[JsonObject]
    guardExamples: list[JsonObject]
    counts: IntCountMap


class DedupAuditGateDetail(TypedDict, total=False):
    key: str
    label: str
    count: int
    whyBlocked: str
    nextAction: str
    counts: IntCountMap
    examples: list[JsonObject]


class DedupAuditGatePayload(TypedDict, total=False):
    status: str
    lifecycleUxReady: bool
    currentRunMergedCount: int
    currentRunNonPrimaryMergeCounts: IntCountMap
    currentRunMergeGateTierCounts: IntCountMap
    sourceBundleCollisionCount: int
    currentRunSourceBundleCollisionCount: int
    carriedSourceBundleCollisionCount: int
    highRiskReviewQueueCount: int
    currentRunHighRiskReviewQueueCount: int
    carriedHighRiskReviewQueueCount: int
    blockingReviewQueueCount: int
    currentRunBlockingReviewQueueCount: int
    carriedBlockingReviewQueueCount: int
    monitorReviewQueueCount: int
    currentRunMonitorReviewQueueCount: int
    carriedMonitorReviewQueueCount: int
    providerStaticDisagreementCount: int
    providerStaticDisagreementCurrentRunCount: int
    providerStaticDisagreementCarriedCount: int
    providerStaticDisagreementBlockedCount: int
    providerStaticDisagreementWarningCount: int
    googleSheetsGenericRoleGuardActive: bool
    googleSheetsRoleBucketUnresolvedCount: int
    googleSheetsRoleBucketGuardBlockedCount: int
    googleSheetsRoleBucketHistoricalCount: int
    carriedCollisionLikelyHistoricalCount: int
    reviewQueueCauseCounts: IntCountMap
    currentRunBlockingReviewQueueCauseCounts: IntCountMap
    carriedBlockingReviewQueueCauseCounts: IntCountMap
    currentRunMonitorReviewQueueCauseCounts: IntCountMap
    carriedMonitorReviewQueueCauseCounts: IntCountMap
    blockers: list[str]
    warnings: list[str]
    blockerDetails: list[DedupAuditGateDetail]
    warningDetails: list[DedupAuditGateDetail]
    examples: list[JsonObject]
    nonzeroReviewQueueCauseCounts: IntCountMap


class DedupEvidencePayload(TypedDict, total=False):
    schemaVersion: int
    inputCount: int
    outputCount: int
    duplicateCount: int
    mergedCount: int
    duplicateRate: float
    sourceBundleCollisionCount: int
    currentRunSourceBundleCollisionCount: int
    carriedSourceBundleCollisionCount: int
    currentRunHighRiskReviewQueueCount: int
    carriedHighRiskReviewQueueCount: int
    currentRunBlockingReviewQueueCount: int
    carriedBlockingReviewQueueCount: int
    currentRunMonitorReviewQueueCount: int
    carriedMonitorReviewQueueCount: int
    mergeReasonCounts: IntCountMap
    sourceBundleComposition: IntCountMap
    riskReasonCounts: IntCountMap
    outlierReasonCounts: IntCountMap
    identityShapeCounts: IntCountMap
    identityQualityCounts: IntCountMap
    nonProviderIdentityProvenanceCounts: IntCountMap
    googleSheetsBundleShapeCounts: IntCountMap
    googleSheetsRoleBucketAuditCounts: IntCountMap
    googleSheetsRoleBucketAudit: GoogleSheetsRoleBucketAuditPayload
    googleSheetsBucketIntentCounts: IntCountMap
    googleSheetsWeakGroupingAuditCounts: IntCountMap
    reviewQueueCounts: IntCountMap
    reviewQueueCauseCounts: IntCountMap
    currentRunBlockingReviewQueueCauseCounts: IntCountMap
    carriedBlockingReviewQueueCauseCounts: IntCountMap
    currentRunMonitorReviewQueueCauseCounts: IntCountMap
    carriedMonitorReviewQueueCauseCounts: IntCountMap
    currentRunNonPrimaryMergeReasonCounts: IntCountMap
    currentRunBlockingNonPrimaryMergeReasonCounts: IntCountMap
    currentRunMonitorNonPrimaryMergeReasonCounts: IntCountMap
    currentRunMergeGateTierCounts: IntCountMap
    providerStaticDisagreementCounts: IntCountMap
    providerStaticDisagreementGateCounts: IntCountMap
    providerStaticDisagreementClassificationCounts: IntCountMap
    providerStaticTitleCompanyCollisionCounts: IntCountMap
    providerStaticTitleCompanyCollisionAuditCounts: IntCountMap
    providerStaticDisagreementExamples: list[ProviderStaticDisagreementRow]
    providerStaticTitleCompanyCollisionExamples: list[ProviderStaticDisagreementRow]
    reviewQueue: list[DedupReviewQueueRow]
    currentRunMergeExamples: list[DedupMergeExampleRow]
    currentRunMergeExamplesByReason: MergeExampleMap
    currentRunBlockingMergeExamplesByReason: MergeExampleMap
    carriedBundleExamples: list[DedupReviewQueueRow]
    topMergedJobs: list[JsonObject]
    topSourceBundleOutliers: list[JsonObject]
    locationDivergenceExamples: list[JsonObject]
    riskyMergeExamples: list[JsonObject]
    dedupAuditGate: DedupAuditGatePayload
