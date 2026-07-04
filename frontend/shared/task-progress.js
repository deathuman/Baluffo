export function normalizeTaskProgressPayload(progress) {
  if (!progress || typeof progress !== "object" || Array.isArray(progress)) return null;
  const topLevelCounts = {};
  [
    "currentStep",
    "totalSteps",
    "baselineOutputCount",
    "finalOutputCount",
    "jobsPageLoadedCount"
  ].forEach(key => {
    if (Object.prototype.hasOwnProperty.call(progress, key)) {
      topLevelCounts[key] = progress[key];
    }
  });
  const mode = String(progress.mode || "").trim().toLowerCase() === "determinate"
    ? "determinate"
    : "indeterminate";
  const nestedCounts = progress.counts && typeof progress.counts === "object" && !Array.isArray(progress.counts)
    ? progress.counts
    : {};
  const counts = {
    ...topLevelCounts,
    ...nestedCounts
  };
  const ratioValue = Number(progress.ratio);
  return {
    active: Boolean(progress.active),
    phaseKey: String(progress.phaseKey || "").trim(),
    phaseLabel: String(progress.phaseLabel || progress.label || "").trim(),
    mode,
    ratio: Number.isFinite(ratioValue) ? Math.max(0, Math.min(1, ratioValue)) : 0,
    counts,
    targetLabel: String(progress.targetLabel || "").trim(),
    targetUrl: String(progress.targetUrl || "").trim(),
    updatedAt: String(progress.updatedAt || "").trim()
  };
}

function compactCount(value) {
  return Number(value || 0).toLocaleString("en-US");
}

const GAMEDEVMAP_ACTIVE_AUDIT_FETCH_PHASE_LABELS = {
  homepage_fetch: "homepage fetch",
  recovery_wave1_fetch: "recovery wave 1 fetch",
  recovery_wave2_fetch: "recovery wave 2 fetch"
};

const FETCH_PREP_PHASE_KEYS = new Set([
  "loading_state",
  "seeding_existing_output",
  "selecting_sources",
  "applying_exclusions",
  "initializing_runtime"
]);

function formatElapsedSeconds(ms) {
  const value = Math.max(0, Number(ms || 0));
  if (!Number.isFinite(value) || value <= 0) return "";
  return `${compactCount(Math.max(1, Math.round(value / 1000)))}s`;
}

function formatDurationShort(ms) {
  const value = Math.max(0, Number(ms || 0));
  if (!Number.isFinite(value) || value <= 0) return "";
  if (value < 90_000) return `${compactCount(Math.max(1, Math.round(value / 1000)))}s`;
  if (value < 3_600_000) return `${compactCount(Math.max(1, Math.round(value / 60_000)))}m`;
  return `${compactCount(Math.max(1, Math.round(value / 3_600_000)))}h`;
}

function formatRatePerMinute(value) {
  const rate = Number(value || 0);
  if (!Number.isFinite(rate) || rate <= 0) return "";
  return Number.isInteger(rate)
    ? compactCount(rate)
    : rate.toLocaleString("en-US", { maximumFractionDigits: 1 });
}

export function formatDiscoverySubtaskProgress(counts) {
  const subtaskKey = String(counts?.subtaskKey || "").trim();
  if (subtaskKey !== "gamedevmap_active_audit") return "";
  const subtaskLabel = String(counts?.subtaskLabel || "").trim();
  const auditPhaseKey = String(counts?.activeAuditPhase || "").trim();
  const auditPhase = auditPhaseKey.replace(/_/g, " ");
  const auditCompleted = Math.max(0, Number(counts?.activeAuditCompletedUrls || 0));
  const auditTotal = Math.max(0, Number(counts?.activeAuditTotalUrls || 0));
  const auditBatch = Math.max(0, Number(counts?.activeAuditBatch || 0));
  const auditPhaseCompleted = Math.max(0, Number(counts?.activeAuditPhaseCompleted || 0));
  const auditPhaseTotal = Math.max(0, Number(counts?.activeAuditPhaseTotal || 0));
  const auditFetchPhaseLabel = GAMEDEVMAP_ACTIVE_AUDIT_FETCH_PHASE_LABELS[auditPhaseKey] || "";
  if (auditFetchPhaseLabel) {
    return [
      "GameDevMap active dry run",
      `${auditFetchPhaseLabel}${auditPhaseTotal > 0 ? ` ${compactCount(auditPhaseCompleted)}/${compactCount(auditPhaseTotal)} pages` : ""}`
    ].filter(Boolean).join(" | ");
  }
  return [
    subtaskLabel || "GameDevMap active audit",
    auditBatch > 0 ? `batch ${compactCount(auditBatch)}` : "",
    auditTotal > 0 ? `${compactCount(auditCompleted)}/${compactCount(auditTotal)} URLs` : "",
    auditPhase ? `${auditPhase}${auditPhaseTotal > 0 ? ` ${compactCount(auditPhaseCompleted)}/${compactCount(auditPhaseTotal)}` : ""}` : ""
  ].filter(Boolean).join(" | ");
}

export function formatScrapyStaticSourcesTailBadge(workItems) {
  if (!Array.isArray(workItems) || workItems.length === 0) return "";
  const activeQueueItem = workItems.find(item => {
    const itemId = String(item?.id || item?.name || "").trim();
    const status = String(item?.status || "").trim().toLowerCase();
    return itemId === "scrapy_static_sources" && status === "running";
  });
  if (!activeQueueItem) return "";
  const progress = normalizeTaskProgressPayload(activeQueueItem?.progress);
  const counts = progress?.counts && typeof progress.counts === "object" ? progress.counts : {};
  const completedSources = Number(counts.completedSources);
  const totalSources = Number(counts.totalSources);
  if (!Number.isFinite(completedSources) || completedSources < 0) return "";
  if (!Number.isFinite(totalSources) || totalSources <= 0) return "";
  return `Browser fallback ${compactCount(completedSources)}/${compactCount(totalSources)}`;
}

function formatFetcherCounts(counts, progress) {
  const phaseKey = String(progress?.phaseKey || "").trim();
  const isPrepPhase = FETCH_PREP_PHASE_KEYS.has(phaseKey);
  const hasPrepCounts = [
    "sourceStateRows",
    "lifecycleRows",
    "seededOutputRows",
    "selectedSourceCount",
    "excludedSourceCount",
    "setupElapsedMs"
  ].some(key => Object.prototype.hasOwnProperty.call(counts || {}, key));
  if (isPrepPhase || hasPrepCounts) {
    const setupElapsed = formatElapsedSeconds(counts?.setupElapsedMs);
    const parts = [
      Object.prototype.hasOwnProperty.call(counts || {}, "sourceStateRows")
        ? `state rows ${compactCount(counts?.sourceStateRows)}`
        : "",
      Object.prototype.hasOwnProperty.call(counts || {}, "lifecycleRows")
        ? `lifecycle rows ${compactCount(counts?.lifecycleRows)}`
        : "",
      Object.prototype.hasOwnProperty.call(counts || {}, "seededOutputRows")
        ? `seeded ${compactCount(counts?.seededOutputRows)} jobs`
        : "",
      Object.prototype.hasOwnProperty.call(counts || {}, "selectedSourceCount")
        ? `selected ${compactCount(counts?.selectedSourceCount)} sources`
        : "",
      Object.prototype.hasOwnProperty.call(counts || {}, "excludedSourceCount")
        ? `excluded ${compactCount(counts?.excludedSourceCount)}`
        : "",
      setupElapsed ? `setup ${setupElapsed}` : ""
    ];
    const label = parts.filter(Boolean).join(" | ");
    if (label) return label;
  }
  const resolved = Math.max(0, Number(counts?.resolvedSources || 0));
  const total = Math.max(0, Number(counts?.sourceCount || 0));
  const running = Math.max(0, Number(counts?.runningTasks || counts?.running || 0));
  const queued = Math.max(0, Number(counts?.queuedTasks || counts?.queued || 0));
  const output = Math.max(0, Number(counts?.outputCount || 0));
  const failed = Math.max(0, Number(counts?.failedSources || counts?.error || 0));
  const excluded = Math.max(0, Number(counts?.excludedSources || counts?.excluded || 0));
  const okWarnings = Math.max(0, Number(counts?.okWithWarningSources || 0));
  const showTotal = String(progress?.mode || "").toLowerCase() === "determinate" && total > 0;
  const resolvedLabel = showTotal
    ? `${compactCount(resolved)}/${compactCount(total)} sources resolved`
    : `${compactCount(resolved)} sources resolved`;
  const warningLabel = okWarnings > 0 ? ` | ok warnings ${compactCount(okWarnings)}` : "";
  const rateLabel = formatRatePerMinute(counts?.completedSourcesPerMinute);
  const etaLabel = formatDurationShort(counts?.estimatedRemainingMs);
  const runningNames = Array.isArray(counts?.runningSourceNames)
    ? counts.runningSourceNames.map(item => String(item || "").trim()).filter(Boolean)
    : [];
  const runningNamesLabel = runningNames.length
    ? ` | current ${runningNames.join(", ")}${counts?.runningSourceNamesTruncated ? ", +more" : ""}`
    : "";
  return `${resolvedLabel} | running ${compactCount(running)} | queued ${compactCount(queued)} | output ${compactCount(output)} | failed ${compactCount(failed)} | excluded ${compactCount(excluded)}${warningLabel}${rateLabel ? ` | rate ${rateLabel}/min` : ""}${etaLabel ? ` | ETA ${etaLabel}` : ""}${runningNamesLabel}`;
}

function formatDiscoveryCounts(counts, progress) {
  const found = Math.max(0, Number(counts?.foundEndpoints || 0));
  const generated = Math.max(0, Number(counts?.generatedCandidates || 0));
  const survived = Math.max(0, Number(counts?.survivedDedupeCandidates || 0));
  const probed = Math.max(0, Number(counts?.probedCandidates || 0));
  const probeTotal = Math.max(0, Number(counts?.probeTotal || 0));
  const queued = Math.max(0, Number(counts?.queuedCandidates || 0));
  const deferred = Math.max(0, Number(counts?.deferredCandidates || 0));
  const failed = Math.max(0, Number(counts?.failedProbes || 0));
  const stageIndex = Math.max(0, Number(counts?.stageIndex || 0));
  const stageTotal = Math.max(0, Number(counts?.stageTotal || 0));
  const stageLabel = stageIndex > 0 && stageTotal > 0
    ? `stage ${compactCount(stageIndex)}/${compactCount(stageTotal)}`
    : "";
  const auditSubtask = formatDiscoverySubtaskProgress(counts);
  const probedLabel = String(progress?.mode || "").toLowerCase() === "determinate" && probeTotal > 0
    ? `${compactCount(probed)}/${compactCount(probeTotal)}`
    : compactCount(probed);
  return [
    stageLabel,
    auditSubtask,
    `generated ${compactCount(generated)}`,
    `endpoints ${compactCount(found)}`,
    `survived ${compactCount(survived)}`,
    `probed ${probedLabel}`,
    `queued ${compactCount(queued)}`,
    `deferred ${compactCount(deferred)}`,
    `failed ${compactCount(failed)}`
  ].filter(Boolean).join(" | ");
}

function formatSyncCounts(counts, summary) {
  const sourceCounts = counts && typeof counts === "object" && !Array.isArray(counts) ? counts : {};
  const sourceSummary = summary && typeof summary === "object" && !Array.isArray(summary) ? summary : {};
  const hasShardProgress = [
    "shardCount",
    "changedShardCount",
    "completedShardCount",
    "verifiedShardCount",
    "shardsReadBytes",
    "totalShardBytes",
    "manifestCommitted",
    "gcDeletedCount"
  ].some(key => Object.prototype.hasOwnProperty.call(sourceCounts, key));
  if (hasShardProgress) {
    const action = String(sourceCounts?.action || sourceSummary?.action || "").trim().toLowerCase();
    const hasReadProgress = Object.prototype.hasOwnProperty.call(sourceCounts, "shardsReadBytes");
    if (action === "pull" || hasReadProgress) {
      const total = Math.max(0, Number(sourceCounts?.shardCount || 0));
      const completed = Math.max(0, Number(sourceCounts?.completedShardCount || 0));
      const current = String(sourceCounts?.currentShardLabel || "").trim();
      const skipped = Boolean(sourceCounts?.skipped || sourceSummary?.skipped);
      const parts = [
        skipped
          ? "remote manifest unchanged"
          : total > 0 ? `read ${compactCount(completed)}/${compactCount(total)}` : "",
        current && !skipped ? `current ${current}` : "",
        skipped && total > 0 ? `shards skipped ${compactCount(total)}` : ""
      ];
      return parts.filter(Boolean).join(" | ");
    }
    const changed = Math.max(0, Number(sourceCounts?.changedShardCount || 0));
    const total = changed || Math.max(0, Number(sourceCounts?.shardCount || 0));
    const completed = Math.max(0, Number(sourceCounts?.completedShardCount || 0));
    const verified = Math.max(0, Number(sourceCounts?.verifiedShardCount || 0));
    const current = String(sourceCounts?.currentShardLabel || "").trim();
    const parts = [
      total > 0 ? `shards ${compactCount(completed)}/${compactCount(total)}` : "",
      total > 0 ? `verified ${compactCount(verified)}/${compactCount(total)}` : "",
      current ? `current ${current}` : "",
      sourceCounts?.manifestCommitted ? "manifest committed" : "",
      (sourceCounts?.manifestCommitted || Number(sourceCounts?.gcDeletedCount || 0) > 0)
        ? `gc deleted ${compactCount(sourceCounts?.gcDeletedCount)}`
        : ""
    ];
    return parts.filter(Boolean).join(" | ");
  }
  const hasLifecycleCounts = ["activeCount", "pendingCount", "rejectedCount"].some(
    key => Object.prototype.hasOwnProperty.call(sourceCounts, key)
      || Object.prototype.hasOwnProperty.call(sourceSummary, key)
  );
  const hasChanged = Object.prototype.hasOwnProperty.call(sourceCounts, "changed")
    || Object.prototype.hasOwnProperty.call(sourceSummary, "changed");
  if (!hasLifecycleCounts && !hasChanged) return "";
  if (!hasLifecycleCounts) return `changed ${(sourceCounts?.changed ?? sourceSummary?.changed) ? "yes" : "no"}`;
  const active = Math.max(0, Number(sourceCounts?.activeCount ?? sourceSummary?.activeCount ?? 0));
  const pending = Math.max(0, Number(sourceCounts?.pendingCount ?? sourceSummary?.pendingCount ?? 0));
  const rejected = Math.max(0, Number(sourceCounts?.rejectedCount ?? sourceSummary?.rejectedCount ?? 0));
  const changed = hasChanged
    ? ` | changed ${(sourceCounts?.changed ?? sourceSummary?.changed) ? "yes" : "no"}`
    : "";
  return `active ${compactCount(active)} | pending ${compactCount(pending)} | rejected ${compactCount(rejected)}${changed}`;
}

function formatPipelineCounts(counts, progress) {
  const currentStep = Math.max(0, Number(counts?.currentStep || 0));
  const totalSteps = Math.max(0, Number(counts?.totalSteps || 0));
  const baseline = Math.max(0, Number(counts?.baselineOutputCount || 0));
  const final = Math.max(0, Number(counts?.finalOutputCount || 0));
  void progress;
  if (currentStep <= 0 && totalSteps <= 0 && baseline <= 0 && final <= 0) return "";
  const stepLabel = totalSteps > 0
    ? `step ${compactCount(currentStep)}/${compactCount(totalSteps)}`
    : `step ${compactCount(currentStep)}`;
  if (baseline <= 0 && final <= 0) return stepLabel;
  return `${stepLabel} | output ${compactCount(final)} (baseline ${compactCount(baseline)})`;
}

export function formatTaskProgressCounts(taskType, counts, progress, summary = {}) {
  const normalizedType = String(taskType || "").trim().toLowerCase();
  if (normalizedType === "fetch") return formatFetcherCounts(counts, progress);
  if (normalizedType === "discovery") return formatDiscoveryCounts(counts, progress);
  if (normalizedType === "sync") return formatSyncCounts(counts, summary);
  if (normalizedType === "pipeline") return formatPipelineCounts(counts, progress);
  return "";
}

export function formatTaskProgressDetail(taskType, progress, summary = {}, options = {}) {
  const normalized = normalizeTaskProgressPayload(progress);
  if (!normalized) return "";
  const phaseLabel = String(normalized.phaseLabel || normalized.phaseKey || "").trim();
  const progressPct = normalized.mode === "determinate"
    ? `${Math.round(Math.max(0, Math.min(1, Number(normalized.ratio || 0))) * 100)}%`
    : "";
  const phaseText = phaseLabel
    ? `${phaseLabel}${progressPct ? ` (${progressPct})` : ""}`
    : "";
  if (options?.includeCounts === false) return phaseText;
  const countsText = formatTaskProgressCounts(taskType, normalized.counts, normalized, summary);
  return [phaseText, countsText].filter(Boolean).join(" | ");
}
