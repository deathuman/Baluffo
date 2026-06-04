const OPS_FETCHER_METRIC_SECTION_DEFINITIONS = [
  {
    key: "runtime",
    title: "Runtime",
    description: "Latest run performance, yield, and source-cost signals."
  },
  {
    key: "failures",
    title: "Failures",
    description: "Fetcher failure counts, buckets, and source examples."
  },
  {
    key: "taskFailures",
    title: "Task Failure Attempts",
    description: "Bounded fetch and discovery failure-attempt classification."
  },
  {
    key: "frontendPerf",
    title: "Frontend Performance",
    description: "Browser-side fetch and render counters from this Admin session."
  },
  {
    key: "dedup",
    title: "Dedup Review",
    description: "Read-only gate, review-state, and blocker evidence before lifecycle UX."
  },
  {
    key: "sourceHealth",
    title: "Source Health",
    description: "Source reliability, zero-kept, fallback, and productivity signals."
  },
  {
    key: "sourcePolicy",
    title: "Source Policy Signals",
    description: "Provider coverage, static suppression, cleanup proposals, and review context."
  },
  {
    key: "diagnostics",
    title: "Diagnostics",
    description: "Supporting evidence that does not own an operator action queue."
  },
  {
    key: "auditArtifacts",
    title: "Audit Artifacts",
    description: "Bounded discovery audit artifact evidence from the active data directory."
  }
];

const OPS_TASK_LANE_TYPES = [
  { type: "discovery", label: "Discovery" },
  { type: "fetch", label: "Fetch" },
  { type: "sync", label: "Sync" }
];

export function getOpsFetcherMetricSectionDefinitions() {
  return OPS_FETCHER_METRIC_SECTION_DEFINITIONS.map(section => ({ ...section }));
}

export function buildOpsFetcherMetricSections(sectionContentByKey = {}, diagnosticsByKey = {}) {
  const content = sectionContentByKey && typeof sectionContentByKey === "object"
    ? sectionContentByKey
    : {};
  const diagnostics = diagnosticsByKey && typeof diagnosticsByKey === "object"
    ? diagnosticsByKey
    : {};
  return getOpsFetcherMetricSectionDefinitions()
    .map(section => ({
      ...section,
      html: String(content[section.key] || ""),
      diagnostics: diagnostics[section.key] || null
    }))
    .filter(section => section.html.trim());
}

function normalizeRunType(row) {
  return String(row?.taskType || row?.type || "").trim().toLowerCase();
}

function normalizeRunStatus(row) {
  return String(row?.lifecycleStatus || row?.displayStatus || row?.status || "unknown").trim().toLowerCase() || "unknown";
}

function findLatestRunForType(rows, type) {
  return rows.find(row => normalizeRunType(row) === type) || null;
}

function toNumber(value) {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatCount(value) {
  return toNumber(value).toLocaleString();
}

function formatDiscoverySummary(row) {
  const summary = row?.summary && typeof row.summary === "object" ? row.summary : {};
  const queued = formatCount(summary?.queuedCandidateCount);
  const failed = formatCount(summary?.failedProbeCount);
  if (toNumber(summary?.queuedCandidateCount) || toNumber(summary?.failedProbeCount)) {
    return `Review queue ${queued}; failed probes ${failed}`;
  }
  return "No discovery queue summary yet.";
}

function formatFetchSummary(row) {
  const liveDetail = String(row?.taskProgress?.phaseLabel || row?.taskProgress?.message || "").trim();
  if (row?.isLive && liveDetail) return liveDetail;
  const summary = row?.summary && typeof row.summary === "object" ? row.summary : {};
  return `${formatCount(summary?.outputCount)} jobs; ${formatCount(summary?.failedSources)} failed sources`;
}

function formatSyncSummary(row) {
  const summary = row?.summary && typeof row.summary === "object" ? row.summary : {};
  const action = String(summary?.action || "sync").trim().toLowerCase() || "sync";
  const counts = [
    `active ${formatCount(summary?.activeCount)}`,
    `pending ${formatCount(summary?.pendingCount)}`,
    `rejected ${formatCount(summary?.rejectedCount)}`
  ].join(", ");
  return `${action.replaceAll("_", " ")}: ${counts}`;
}

function formatTaskLaneSummary(type, row) {
  if (!row) return "Waiting for the next run.";
  if (type === "discovery") return formatDiscoverySummary(row);
  if (type === "fetch") return formatFetchSummary(row);
  if (type === "sync") return formatSyncSummary(row);
  return "No task summary yet.";
}

export function buildOpsTaskLaneRows(runModel = {}) {
  const currentRows = Array.isArray(runModel?.currentRows) ? runModel.currentRows : [];
  const completedRows = [
    ...(Array.isArray(runModel?.visibleCompletedRows) ? runModel.visibleCompletedRows : []),
    ...(Array.isArray(runModel?.olderCompletedRows) ? runModel.olderCompletedRows : [])
  ];
  return OPS_TASK_LANE_TYPES.map(({ type, label }) => {
    const liveRow = findLatestRunForType(currentRows, type);
    const completedRow = findLatestRunForType(completedRows, type);
    const row = liveRow || completedRow;
    const status = row ? normalizeRunStatus(row) : "waiting";
    return {
      type,
      label,
      status,
      lifecycleStatus: String(row?.lifecycleStatus || "").trim().toLowerCase(),
      hasRun: Boolean(row),
      isLive: Boolean(liveRow),
      elapsedMs: toNumber(row?.elapsedMs ?? row?.durationMs),
      summary: formatTaskLaneSummary(type, row)
    };
  });
}

function truncateString(value, maxLength = 180) {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  if (text.length <= maxLength) return text;
  return `${text.slice(0, Math.max(0, maxLength - 1)).trim()}…`;
}

function boundedList(rows, mapper, limit = 5) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  return sourceRows
    .slice(0, limit)
    .map(row => mapper(row))
    .filter(row => row && typeof row === "object");
}

function cleanCounts(counts) {
  if (!counts || typeof counts !== "object" || Array.isArray(counts)) return {};
  return Object.fromEntries(
    Object.entries(counts)
      .filter(([, value]) => Number(value || 0) !== 0)
      .map(([key, value]) => [String(key), toNumber(value)])
  );
}

function compactSourceRow(row) {
  return {
    name: truncateString(row?.name || row?.sourceName || row?.id || "unknown"),
    status: truncateString(row?.status || row?.providerCoverageStatus || row?.classification || "unknown"),
    keptCount: toNumber(row?.keptCount || row?.providerCoverageLatestKeptCount),
    reason: truncateString(row?.failureBucket || row?.classification || row?.zeroKeptClassification || row?.error || row?.providerReplacementReadiness || "")
  };
}

function compactPairRow(row) {
  return {
    staticSource: truncateString(row?.staticSourceName || row?.staticSourceId || "static"),
    providerSource: truncateString(row?.providerSourceName || row?.providerSourceId || "provider"),
    status: truncateString(row?.auditStatus || row?.lastAuditStatus || row?.proposal || row?.reason || "unknown"),
    reason: truncateString(row?.recommendedAction || row?.proposalReadinessReason || row?.proposalBlockedReason || "")
  };
}

function compactDedupExample(row) {
  return {
    title: truncateString(row?.title || "Untitled"),
    company: truncateString(row?.company || "Unknown company"),
    cause: truncateString(row?.suspectedCause || row?.mergeReason || row?.recommendedReviewAction || "unknown"),
    origin: truncateString(row?.bundleEvidenceOrigin || row?.disagreementGateDisposition || ""),
    review: truncateString(row?.reviewStatus || row?.recommendedReviewAction || "")
  };
}

function compactSummaryObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value)
      .filter(([, item]) => item === null || ["string", "number", "boolean"].includes(typeof item))
      .slice(0, 16)
      .map(([key, item]) => [String(key), typeof item === "string" ? truncateString(item, 120) : item])
  );
}

function compactAuditArtifact(row) {
  return {
    name: truncateString(row?.name || ""),
    exists: Boolean(row?.exists),
    relativePath: truncateString(row?.relativePath || ""),
    pathDisplay: truncateString(row?.pathDisplay || ""),
    sizeBytes: toNumber(row?.sizeBytes),
    modifiedAt: truncateString(row?.modifiedAt || ""),
    sha256: truncateString(row?.sha256 || "", 80),
    topLevelKeys: (Array.isArray(row?.topLevelKeys) ? row.topLevelKeys : [])
      .slice(0, 12)
      .map(value => truncateString(value, 80)),
    warnings: (Array.isArray(row?.warnings) ? row.warnings : [])
      .slice(0, 8)
      .map(value => truncateString(value, 80)),
    summary: compactSummaryObject(row?.summary)
  };
}

function compactFailureBucket(row) {
  return {
    key: truncateString(row?.key || "unknown"),
    count: toNumber(row?.count),
    classification: truncateString(row?.classification || "")
  };
}

function compactTaskFailureAttempts(payload) {
  const fetch = payload?.fetch && typeof payload.fetch === "object" ? payload.fetch : {};
  const discovery = payload?.discovery && typeof payload.discovery === "object" ? payload.discovery : {};
  return {
    generatedAt: truncateString(payload?.generatedAt || ""),
    fetch: {
      runId: truncateString(fetch?.runId || ""),
      hardFailureCount: toNumber(fetch?.hardFailureCount),
      partialWarningCount: toNumber(fetch?.partialWarningCount),
      expectedExclusionCount: toNumber(fetch?.expectedExclusionCount),
      failedSources: toNumber(fetch?.failedSources),
      excludedSources: toNumber(fetch?.excludedSources),
      failureBuckets: boundedList(fetch?.failureBuckets, compactFailureBucket, 8),
      partialWarnings: boundedList(fetch?.partialWarnings, compactSourceRow, 5),
      hardFailures: boundedList(fetch?.hardFailures, compactSourceRow, 5)
    },
    discovery: {
      runId: truncateString(discovery?.runId || ""),
      failureRecordCount: toNumber(discovery?.failureRecordCount),
      expectedSkipCount: toNumber(discovery?.expectedSkipCount),
      expectedNegativeCount: toNumber(discovery?.expectedNegativeCount),
      actionableDiagnosticCount: toNumber(discovery?.actionableDiagnosticCount),
      highPriorityBuckets: boundedList(discovery?.highPriorityBuckets, compactFailureBucket, 8)
    },
    warnings: (Array.isArray(payload?.warnings) ? payload.warnings : [])
      .slice(0, 8)
      .map(value => truncateString(value, 100))
  };
}

function baseDiagnostics(key, title, generatedAt) {
  return {
    key,
    title,
    generatedAt,
    version: 1
  };
}

export function buildOpsFetcherDiagnosticsSections({
  latest,
  history,
  failureSummary,
  taskLaneRows,
  auditArtifacts,
  taskFailureAttempts,
  generatedAt = new Date().toISOString()
} = {}) {
  const latestRun = latest && typeof latest === "object" ? latest : {};
  const historySummary = history && typeof history === "object" ? history : {};
  const failures = failureSummary && typeof failureSummary === "object" ? failureSummary : {};
  const sourceHealth = latestRun?.sourceHealth && typeof latestRun.sourceHealth === "object" ? latestRun.sourceHealth : {};
  const providerCoverage = latestRun?.providerCoverage && typeof latestRun.providerCoverage === "object" ? latestRun.providerCoverage : {};
  const providerStaticOverlap = latestRun?.providerStaticOverlap && typeof latestRun.providerStaticOverlap === "object" ? latestRun.providerStaticOverlap : {};
  const staticSuppressionPolicy = latestRun?.staticSuppressionPolicy && typeof latestRun.staticSuppressionPolicy === "object" ? latestRun.staticSuppressionPolicy : {};
  const conservativeStaticCleanupProposals = latestRun?.conservativeStaticCleanupProposals && typeof latestRun.conservativeStaticCleanupProposals === "object" ? latestRun.conservativeStaticCleanupProposals : {};
  const sourcePolicyRecommendationExport = latestRun?.sourcePolicyRecommendationExport && typeof latestRun.sourcePolicyRecommendationExport === "object" ? latestRun.sourcePolicyRecommendationExport : {};
  const dedup = latestRun?.dedupEvidence && typeof latestRun.dedupEvidence === "object" ? latestRun.dedupEvidence : {};
  const gate = dedup?.dedupAuditGate && typeof dedup.dedupAuditGate === "object" ? dedup.dedupAuditGate : {};
  const reviewState = latestRun?.dedupReviewStateSummary && typeof latestRun.dedupReviewStateSummary === "object" ? latestRun.dedupReviewStateSummary : {};
  const auditArtifactsPayload = auditArtifacts && typeof auditArtifacts === "object" && !Array.isArray(auditArtifacts)
    ? auditArtifacts
    : {};
  const taskFailureAttemptsPayload = taskFailureAttempts && typeof taskFailureAttempts === "object" && !Array.isArray(taskFailureAttempts)
    ? taskFailureAttempts
    : {};

  return {
    taskStatus: {
      ...baseDiagnostics("taskStatus", "Task Status", generatedAt),
      rows: boundedList(taskLaneRows, row => ({
        type: truncateString(row?.type || ""),
        status: truncateString(row?.status || "unknown"),
        isLive: Boolean(row?.isLive),
        summary: truncateString(row?.summary || "")
      }), 3)
    },
    runtime: {
      ...baseDiagnostics("runtime", "Runtime", generatedAt),
      latestDurationMs: toNumber(latestRun?.durationMs),
      medianDurationMs: toNumber(historySummary?.medianDurationMs),
      averageDurationMs: toNumber(historySummary?.averageDurationMs),
      windowRuns: toNumber(historySummary?.windowRuns),
      duplicateRate: toNumber(latestRun?.duplicateRate),
      outputYieldRate: toNumber(latestRun?.outputYieldRate),
      sourceFailureRate: toNumber(latestRun?.sourceFailureRate),
      examples: boundedList(latestRun?.slowestSources, compactSourceRow)
    },
    failures: {
      ...baseDiagnostics("failures", "Failures", generatedAt),
      topLevelFailedSources: toNumber(failures?.topLevelFailedSources),
      detailFailureCount: toNumber(failures?.detailFailureCount),
      buckets: boundedList(failures?.buckets, row => ({
        key: truncateString(row?.key || "unknown"),
        count: toNumber(row?.count),
        examples: (Array.isArray(row?.examples) ? row.examples : []).slice(0, 5).map(value => truncateString(value))
      }))
    },
    taskFailures: {
      ...baseDiagnostics("taskFailures", "Task Failure Attempts", generatedAt),
      ...compactTaskFailureAttempts(taskFailureAttemptsPayload)
    },
    dedup: {
      ...baseDiagnostics("dedup", "Dedup Review", generatedAt),
      gate: {
        status: truncateString(gate?.status || "unknown"),
        lifecycleUxReady: gate?.lifecycleUxReady === true,
        blockers: (Array.isArray(gate?.blockers) ? gate.blockers : []).slice(0, 5).map(value => truncateString(value)),
        warnings: (Array.isArray(gate?.warnings) ? gate.warnings : []).slice(0, 5).map(value => truncateString(value))
      },
      counts: {
        mergeReasons: cleanCounts(dedup?.mergeReasonCounts),
        reviewQueue: cleanCounts(dedup?.reviewQueueCounts),
        reviewCauses: cleanCounts(dedup?.reviewQueueCauseCounts)
      },
      reviewState: {
        status: truncateString(reviewState?.status || "unknown"),
        reviewedPairCount: toNumber(reviewState?.reviewedPairCount),
        reviewedSafeCount: toNumber(reviewState?.reviewedSafeCount),
        confirmedBlockingCount: toNumber(reviewState?.confirmedBlockingCount),
        unresolvedBlockingCount: toNumber(reviewState?.unresolvedBlockingCount)
      },
      examples: boundedList(gate?.examples || dedup?.currentRunMergeExamples || dedup?.reviewQueue, compactDedupExample)
    },
    sourceHealth: {
      ...baseDiagnostics("sourceHealth", "Source Health", generatedAt),
      counts: {
        attention: Array.isArray(sourceHealth?.sourcesNeedingAttention) ? sourceHealth.sourcesNeedingAttention.length : 0,
        zeroKeptNeedsReview: Array.isArray(sourceHealth?.zeroKeptNeedsReview) ? sourceHealth.zeroKeptNeedsReview.length : 0,
        browserFallbackRecommended: Array.isArray(sourceHealth?.browserFallbackRecommended) ? sourceHealth.browserFallbackRecommended.length : 0
      },
      examples: boundedList(sourceHealth?.sourcesNeedingAttention || sourceHealth?.zeroKeptNeedsReview, compactSourceRow)
    },
    sourcePolicy: {
      ...baseDiagnostics("sourcePolicy", "Source Policy Signals", generatedAt),
      counts: {
        validatedProviders: Array.isArray(providerCoverage?.validatedProviders) ? providerCoverage.validatedProviders.length : 0,
        providersNeedingReview: Array.isArray(providerCoverage?.needsReviewProviders) ? providerCoverage.needsReviewProviders.length : 0,
        overlapNeedsReview: toNumber(providerStaticOverlap?.needsReviewPairCount),
        suppressedPairs: toNumber(staticSuppressionPolicy?.suppressedCount),
        cleanupProposalReady: toNumber(conservativeStaticCleanupProposals?.proposalCount),
        cleanupBlocked: toNumber(conservativeStaticCleanupProposals?.blockedCount),
        reviewStatePairs: toNumber(sourcePolicyRecommendationExport?.reviewStatePairCount)
      },
      examples: boundedList(
        providerCoverage?.needsReviewProviders
        || providerStaticOverlap?.pairs
        || conservativeStaticCleanupProposals?.blockedExamples,
        row => (row?.staticSourceId || row?.providerSourceId ? compactPairRow(row) : compactSourceRow(row))
      )
    },
    auditArtifacts: {
      ...baseDiagnostics("auditArtifacts", "Audit Artifacts", generatedAt),
      artifacts: boundedList(auditArtifactsPayload?.artifacts, compactAuditArtifact, 8)
    }
  };
}
