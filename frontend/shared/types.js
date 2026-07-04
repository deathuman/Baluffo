/**
 * @typedef {Object} CanonicalJob
 * @property {string|number} id - Unique identifier.
 * @property {string} title - Job title.
 * @property {string} company - Company name.
 * @property {string} city - City location.
 * @property {string} country - Country name.
 * @property {string} workType - Remote, Hybrid, or Onsite.
 * @property {string} contractType - Full-time, Internship, Temporary, or Unknown.
 * @property {string} jobLink - URL to apply.
 * @property {string} sector - Game or Tech.
 * @property {string} profession - Normalized profession key.
 * @property {string} companyType - Game or Tech.
 * @property {string} description - Fallback description.
 * @property {string} source - Scraper or board name.
 * @property {string} sourceJobId - Originating ATS ID.
 * @property {string} fetchedAt - ISO 8601 timestamp.
 * @property {string} postedAt - ISO 8601 timestamp (optional).
 * @property {string} status - e.g. active, likely_removed, archived.
 * @property {string} firstSeenAt - ISO 8601 timestamp.
 * @property {string} lastSeenAt - ISO 8601 timestamp.
 * @property {string} removedAt - ISO 8601 timestamp (optional).
 * @property {string} [lifecycleEvent] - e.g. reappeared, preserved.
 * @property {string} [lifecycleReason] - e.g. source_failed, source_skipped.
 * @property {string} dedupKey - Content hash for deduplication.
 * @property {number} qualityScore - [0-100] scale.
 * @property {number} focusScore - [0-100] scale.
 * @property {number} sourceBundleCount - Number of collapsed rows.
 * @property {Array<Object>} sourceBundle - Raw ATS payloads.
 * @property {string} adapter - Python adapter module.
 * @property {string} studio - Pipeline studio group.
 * @property {number} [freshnessAgeDays] - Derived field (calculated on frontend).
 * @property {number} [freshnessScore] - Derived field (calculated on frontend).
 * @property {string} [freshnessSource] - Derived field (calculated on frontend).
 */

/**
 * @typedef {Object} SavedJobSnapshot
 * @property {string} title
 * @property {string} company
 * @property {string} sector
 * @property {string} companyType
 * @property {string} city
 * @property {string} country
 * @property {string} workType
 * @property {string} contractType
 * @property {string} jobLink
 */

/**
 * @typedef {Object} SavedJob
 * @property {string} jobKey - Primary key (job_hash).
 * @property {SavedJobSnapshot} snapshot - Flattened CanonicalJob subset.
 * @property {string} createdAt - ISO 8601 timestamp.
 * @property {string} updatedAt - ISO 8601 timestamp.
 * @property {string} applicationStatus - Legacy mirror derived from tracking fields.
 * @property {string} pipelinePhase - Current active pipeline phase.
 * @property {string} outcomeStatus - Current outcome; active means still in flight.
 * @property {Object<string,string>} phaseTimestamps - ISO timestamps keyed by phase.
 * @property {Object<string,string>} outcomeTimestamps - ISO timestamps keyed by terminal outcome.
 * @property {string} contentUpdatedAt - ISO timestamp for job content edits.
 * @property {string} trackingUpdatedAt - ISO timestamp for phase/outcome changes.
 * @property {string} notesUpdatedAt - ISO timestamp for notes changes.
 * @property {string} lastActivityAt - ISO timestamp for the latest saved-job activity row.
 * @property {string} notes - User text.
 * @property {boolean} isCustom - True if manually created.
 * @property {string} customSourceLabel - Display name for custom rows.
 * @property {string} [reminderAt] - ISO 8601 timestamp (optional).
 * @property {number} attachments - Count of local files.
 */

/**
 * @typedef {Object} RunFetcherRequest
 * @property {"default"|"incremental"|"retry_failed"|"force_full"|"uncapped"} [preset]
 * @property {number} [maxWorkers]
 * @property {number} [maxPerDomain]
 * @property {"auto"|"http"|"browser"} [fetchStrategy]
 * @property {number} [adapterHttpConcurrency]
 * @property {number} [sourceTtlMinutes]
 * @property {number} [hotSourceCadenceMinutes]
 * @property {number} [coldSourceCadenceMinutes]
 * @property {number} [circuitBreakerFailures]
 * @property {number} [circuitBreakerCooldownMinutes]
 * @property {number} [browserFallbackCooldownMinutes]
 * @property {boolean} [skipSuccessfulSources]
 * @property {boolean} [respectSourceCadence]
 * @property {boolean} [ignoreCircuitBreaker]
 * @property {boolean} [quiet]
 * @property {boolean} [socialEnabled]
 * @property {Array<string>} [onlySources]
 */

/**
 * @typedef {Object} TaskStartResponse
 * @property {boolean} [started]
 * @property {boolean} [alreadyRunning]
 * @property {boolean} [alreadyCompleted]
 * @property {string} [runId]
 * @property {string} [task]
 * @property {string} [taskType]
 * @property {string} [preset]
 * @property {string} [coverageScope]
 * @property {Array<string>} [args]
 * @property {number} [pid]
 * @property {string} [startedAt]
 * @property {string} [status]
 * @property {string} [error]
 */

/**
 * @typedef {Object} LiveTaskProgress
 * @property {boolean} [active]
 * @property {string} [phaseKey]
 * @property {string} [phaseLabel]
 * @property {"determinate"|"indeterminate"|string} [mode]
 * @property {number} [ratio]
 * @property {Object<string, string|number|boolean|Array<string>>} [counts]
 * @property {string} [targetLabel]
 * @property {string} [targetUrl]
 * @property {string} [waitReason]
 * @property {string} [updatedAt]
 */

/**
 * @typedef {Object} LiveTaskWorkItem
 * @property {string} [id]
 * @property {string} [name]
 * @property {string} [status]
 * @property {string} [startedAt]
 * @property {string} [finishedAt]
 * @property {number} [durationMs]
 * @property {string} [heartbeatAt]
 * @property {string} [error]
 * @property {LiveTaskProgress} [progress]
 */

/**
 * @typedef {Object} LiveTaskEvent
 * @property {number} [schemaVersion]
 * @property {string} [timestamp]
 * @property {string} [level]
 * @property {string} [event]
 * @property {string} [taskType]
 * @property {string} [runId]
 * @property {string} [workItemId]
 * @property {string} [phaseKey]
 * @property {string} [message]
 * @property {string} [target]
 * @property {string} [targetUrl]
 */

/**
 * @typedef {Object} LiveTaskPayload
 * @property {string} [taskType]
 * @property {string} [status]
 * @property {boolean} [active]
 * @property {string} [runId]
 * @property {string} [startedAt]
 * @property {string} [finishedAt]
 * @property {string} [heartbeatAt]
 * @property {LiveTaskProgress} [taskProgress]
 * @property {Object<string, *>} [summary]
 * @property {Array<LiveTaskWorkItem>} [workItems]
 * @property {Array<LiveTaskEvent>} [recentEvents]
 * @property {Object<string, *>} [outputs]
 */

/**
 * @typedef {LiveTaskPayload & Object} TaskStateRow
 * @property {string} [type]
 * @property {string} [id]
 * @property {string} [lifecycleStatus]
 * @property {string} [stage]
 * @property {string} [parentRunId]
 * @property {string} [parentTaskType]
 * @property {string} [ownerKind]
 * @property {number} [ownerPid]
 * @property {LiveTaskProgress} [progress]
 * @property {number} [workItemCount]
 * @property {boolean} [workItemsTruncated]
 * @property {number} [recentEventCount]
 * @property {boolean} [recentEventsTruncated]
 */

/**
 * @typedef {Object} TaskStatePayload
 * @property {Array<TaskStateRow>} [tasks]
 * @property {number} [count]
 * @property {Array<Object>} [diagnostics]
 * @property {boolean} [summary]
 */

/**
 * @typedef {Object<string, number>} DedupIntCountMap
 */

/**
 * @typedef {Object} DedupMergeExampleRow
 * @property {string} [title]
 * @property {string} [company]
 * @property {string} [dedupKey]
 * @property {string} [existingDedupKey]
 * @property {string} [incomingSource]
 * @property {string} [incomingJobLink]
 * @property {string} [mergeReason]
 * @property {string} [mergeGateTier]
 * @property {boolean} [blocksLifecycle]
 * @property {string} [nonBlockingReason]
 * @property {string} [recommendedReviewAction]
 * @property {string} [suspectedCause]
 * @property {string} [bundleEvidenceOrigin]
 * @property {number} [sourceBundleCount]
 * @property {Array<string>} [sampleSources]
 * @property {Array<string>} [sampleLocations]
 * @property {Array<string>} [evidence]
 * @property {string} [blockedMergeReason]
 * @property {string} [guardReason]
 */

/**
 * @typedef {Object} DedupReviewQueueRow
 * @property {string} [title]
 * @property {string} [company]
 * @property {string} [dedupKey]
 * @property {number} [sourceBundleCount]
 * @property {string} [bundleEvidenceOrigin]
 * @property {string} [recommendedReviewAction]
 * @property {string} [suspectedCause]
 * @property {Array<string>} [causeEvidence]
 * @property {string} [identityShape]
 * @property {string} [identityQuality]
 * @property {Array<string>} [identityQualityEvidence]
 * @property {string} [nonProviderIdentityProvenance]
 * @property {Array<string>} [nonProviderIdentityProvenanceEvidence]
 * @property {string} [googleSheetsBundleShape]
 * @property {string} [googleSheetsRoleBucketAudit]
 * @property {Array<string>} [googleSheetsRoleBucketAuditEvidence]
 * @property {string} [googleSheetsBucketIntent]
 * @property {Array<string>} [googleSheetsBucketIntentEvidence]
 * @property {string} [googleSheetsWeakGroupingAudit]
 * @property {Array<string>} [googleSheetsWeakGroupingEvidence]
 * @property {Array<string>} [sampleSources]
 * @property {Array<string>} [sampleLocations]
 * @property {string} [dedupReviewStatus]
 * @property {string} [dedupReviewAction]
 * @property {string} [dedupReviewUpdatedAt]
 * @property {string} [dedupReviewUpdatedBy]
 * @property {Array<string>} [reviewEvidence]
 */

/**
 * @typedef {Object} ProviderStaticDisagreementRow
 * @property {string} [title]
 * @property {string} [company]
 * @property {string} [dedupKey]
 * @property {string} [bundleEvidenceOrigin]
 * @property {number} [sourceBundleCount]
 * @property {Array<string>} [providerSources]
 * @property {Array<string>} [staticSources]
 * @property {Array<string>} [providerSourceJobIds]
 * @property {Array<string>} [staticSourceJobIds]
 * @property {Array<string>} [providerUrls]
 * @property {Array<string>} [staticUrls]
 * @property {Array<string>} [providerUrlHosts]
 * @property {Array<string>} [staticUrlHosts]
 * @property {Array<string>} [providerUrlPathPrefixes]
 * @property {Array<string>} [staticUrlPathPrefixes]
 * @property {Array<string>} [sharedIdentifierTokens]
 * @property {Array<string>} [concreteSharedIdentifierTokens]
 * @property {boolean} [providerStaticOnly]
 * @property {number} [distinctLocationCount]
 * @property {Array<string>} [sampleLocations]
 * @property {string} [identityQuality]
 * @property {string} [outlierReason]
 * @property {string} [disagreementClassification]
 * @property {Array<string>} [disagreementClassificationEvidence]
 * @property {string} [collisionReviewHint]
 * @property {Array<string>} [disagreementEvidence]
 * @property {string} [disagreementGateDisposition]
 * @property {Array<string>} [disagreementGateEvidence]
 * @property {string} [dedupReviewStatus]
 * @property {string} [dedupReviewAction]
 * @property {string} [dedupReviewUpdatedAt]
 * @property {string} [dedupReviewUpdatedBy]
 * @property {Array<string>} [reviewEvidence]
 * @property {string} [operatorReviewReason]
 * @property {string} [carriedLocationPollutionAudit]
 * @property {Array<string>} [carriedLocationPollutionEvidence]
 */

/**
 * @typedef {Object} GoogleSheetsRoleBucketAuditPayload
 * @property {number} [unresolvedRoleBucketCount]
 * @property {number} [blockedByDifferentPrimaryUrlCount]
 * @property {number} [likelyHistoricalCollisionCount]
 * @property {number} [fixedByGenericRoleGuardCount]
 * @property {DedupIntCountMap} [classificationCounts]
 * @property {Array<Object>} [examples]
 * @property {Array<Object>} [guardExamples]
 * @property {DedupIntCountMap} [counts]
 */

/**
 * @typedef {Object} DedupAuditGateDetail
 * @property {string} [key]
 * @property {string} [label]
 * @property {number} [count]
 * @property {string} [whyBlocked]
 * @property {string} [nextAction]
 * @property {DedupIntCountMap} [counts]
 * @property {Array<Object>} [examples]
 */

/**
 * @typedef {Object} DedupAuditGatePayload
 * @property {string} [status]
 * @property {boolean} [lifecycleUxReady]
 * @property {number} [currentRunMergedCount]
 * @property {DedupIntCountMap} [currentRunNonPrimaryMergeCounts]
 * @property {DedupIntCountMap} [currentRunMergeGateTierCounts]
 * @property {number} [sourceBundleCollisionCount]
 * @property {number} [currentRunSourceBundleCollisionCount]
 * @property {number} [carriedSourceBundleCollisionCount]
 * @property {number} [highRiskReviewQueueCount]
 * @property {number} [currentRunHighRiskReviewQueueCount]
 * @property {number} [carriedHighRiskReviewQueueCount]
 * @property {number} [blockingReviewQueueCount]
 * @property {number} [currentRunBlockingReviewQueueCount]
 * @property {number} [carriedBlockingReviewQueueCount]
 * @property {number} [monitorReviewQueueCount]
 * @property {number} [currentRunMonitorReviewQueueCount]
 * @property {number} [carriedMonitorReviewQueueCount]
 * @property {number} [providerStaticDisagreementCount]
 * @property {number} [providerStaticDisagreementCurrentRunCount]
 * @property {number} [providerStaticDisagreementCarriedCount]
 * @property {number} [providerStaticDisagreementBlockedCount]
 * @property {number} [providerStaticDisagreementWarningCount]
 * @property {boolean} [googleSheetsGenericRoleGuardActive]
 * @property {number} [googleSheetsRoleBucketUnresolvedCount]
 * @property {number} [googleSheetsRoleBucketGuardBlockedCount]
 * @property {number} [googleSheetsRoleBucketHistoricalCount]
 * @property {number} [carriedCollisionLikelyHistoricalCount]
 * @property {DedupIntCountMap} [reviewQueueCauseCounts]
 * @property {DedupIntCountMap} [currentRunBlockingReviewQueueCauseCounts]
 * @property {DedupIntCountMap} [carriedBlockingReviewQueueCauseCounts]
 * @property {DedupIntCountMap} [currentRunMonitorReviewQueueCauseCounts]
 * @property {DedupIntCountMap} [carriedMonitorReviewQueueCauseCounts]
 * @property {Array<string>} [blockers]
 * @property {Array<string>} [warnings]
 * @property {Array<DedupAuditGateDetail>} [blockerDetails]
 * @property {Array<DedupAuditGateDetail>} [warningDetails]
 * @property {Array<Object>} [examples]
 * @property {DedupIntCountMap} [nonzeroReviewQueueCauseCounts]
 */

/**
 * @typedef {Object} DedupEvidencePayload
 * @property {number} [schemaVersion]
 * @property {number} [inputCount]
 * @property {number} [outputCount]
 * @property {number} [duplicateCount]
 * @property {number} [mergedCount]
 * @property {number} [duplicateRate]
 * @property {number} [sourceBundleCollisionCount]
 * @property {number} [currentRunSourceBundleCollisionCount]
 * @property {number} [carriedSourceBundleCollisionCount]
 * @property {DedupIntCountMap} [mergeReasonCounts]
 * @property {DedupIntCountMap} [sourceBundleComposition]
 * @property {DedupIntCountMap} [riskReasonCounts]
 * @property {DedupIntCountMap} [outlierReasonCounts]
 * @property {DedupIntCountMap} [identityShapeCounts]
 * @property {DedupIntCountMap} [identityQualityCounts]
 * @property {DedupIntCountMap} [nonProviderIdentityProvenanceCounts]
 * @property {DedupIntCountMap} [googleSheetsBundleShapeCounts]
 * @property {DedupIntCountMap} [googleSheetsRoleBucketAuditCounts]
 * @property {GoogleSheetsRoleBucketAuditPayload} [googleSheetsRoleBucketAudit]
 * @property {DedupIntCountMap} [googleSheetsBucketIntentCounts]
 * @property {DedupIntCountMap} [googleSheetsWeakGroupingAuditCounts]
 * @property {DedupIntCountMap} [reviewQueueCounts]
 * @property {DedupIntCountMap} [reviewQueueCauseCounts]
 * @property {DedupIntCountMap} [currentRunBlockingReviewQueueCauseCounts]
 * @property {DedupIntCountMap} [carriedBlockingReviewQueueCauseCounts]
 * @property {DedupIntCountMap} [currentRunMonitorReviewQueueCauseCounts]
 * @property {DedupIntCountMap} [carriedMonitorReviewQueueCauseCounts]
 * @property {DedupIntCountMap} [currentRunNonPrimaryMergeReasonCounts]
 * @property {DedupIntCountMap} [currentRunBlockingNonPrimaryMergeReasonCounts]
 * @property {DedupIntCountMap} [currentRunMonitorNonPrimaryMergeReasonCounts]
 * @property {DedupIntCountMap} [currentRunMergeGateTierCounts]
 * @property {DedupIntCountMap} [providerStaticDisagreementCounts]
 * @property {DedupIntCountMap} [providerStaticDisagreementGateCounts]
 * @property {DedupIntCountMap} [providerStaticDisagreementClassificationCounts]
 * @property {DedupIntCountMap} [providerStaticTitleCompanyCollisionCounts]
 * @property {DedupIntCountMap} [providerStaticTitleCompanyCollisionAuditCounts]
 * @property {Array<ProviderStaticDisagreementRow>} [providerStaticDisagreementExamples]
 * @property {Array<ProviderStaticDisagreementRow>} [providerStaticTitleCompanyCollisionExamples]
 * @property {Array<DedupReviewQueueRow>} [reviewQueue]
 * @property {Array<DedupMergeExampleRow>} [currentRunMergeExamples]
 * @property {Object<string, Array<DedupMergeExampleRow>>} [currentRunMergeExamplesByReason]
 * @property {Object<string, Array<DedupMergeExampleRow>>} [currentRunBlockingMergeExamplesByReason]
 * @property {Array<DedupReviewQueueRow>} [carriedBundleExamples]
 * @property {Array<Object>} [topMergedJobs]
 * @property {Array<Object>} [topSourceBundleOutliers]
 * @property {Array<Object>} [locationDivergenceExamples]
 * @property {Array<Object>} [riskyMergeExamples]
 * @property {DedupAuditGatePayload} [dedupAuditGate]
 */

/**
 * @typedef {Object} FetcherMetricsLatestRunPayload
 * @property {number} [inputCount]
 * @property {number} [outputCount]
 * @property {number} [duplicateRate]
 * @property {number} [outputYieldRate]
 * @property {number} [sourceFailureRate]
 * @property {number} [failedSources]
 * @property {number} [sourceCount]
 * @property {number} [durationMs]
 * @property {number} [medianSourceDurationMs]
 * @property {number} [p95SourceDurationMs]
 * @property {Array<Object>} [slowestSources]
 * @property {Array<Object>} [stageTop]
 * @property {Array<Object>} [highCostLowYieldSources]
 * @property {Object<string, *>} [sourceHealth]
 * @property {DedupEvidencePayload} [dedupEvidence]
 * @property {Object<string, *>} [dedupReviewStateSummary]
 * @property {string} [dedupReviewStateReadWarning]
 * @property {Object<string, *>} [providerCoverage]
 * @property {Object<string, *>} [providerStaticOverlap]
 * @property {Object<string, *>} [staticSuppressionPolicy]
 * @property {Object<string, *>} [redundantStaticProposals]
 * @property {Object<string, *>} [conservativeStaticCleanupProposals]
 * @property {Object<string, *>} [sourcePolicyRecommendationExport]
 */

/**
 * @typedef {Object} FetcherMetricsPayload
 * @property {FetcherMetricsLatestRunPayload} [latestRun]
 * @property {Object<string, *>} [history]
 * @property {Object<string, *>} [frontendPerfCounters]
 */

// Keep this file as a module for JSDoc import() consumers without exporting a dead symbol.
export {};
