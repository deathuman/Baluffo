export function normalizeTaskProgressPayload(progress) {
  if (!progress || typeof progress !== "object" || Array.isArray(progress)) return null;
  const mode = String(progress.mode || "").trim().toLowerCase() === "determinate"
    ? "determinate"
    : "indeterminate";
  const counts = progress.counts && typeof progress.counts === "object" && !Array.isArray(progress.counts)
    ? progress.counts
    : {};
  const ratioValue = Number(progress.ratio);
  return {
    active: Boolean(progress.active),
    phaseKey: String(progress.phaseKey || "").trim(),
    phaseLabel: String(progress.phaseLabel || "").trim(),
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
  return `${resolvedLabel} | running ${compactCount(running)} | queued ${compactCount(queued)} | output ${compactCount(output)} | failed ${compactCount(failed)} | excluded ${compactCount(excluded)}${warningLabel}`;
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
  const active = Math.max(0, Number(counts?.activeCount ?? summary?.activeCount ?? 0));
  const pending = Math.max(0, Number(counts?.pendingCount ?? summary?.pendingCount ?? 0));
  const rejected = Math.max(0, Number(counts?.rejectedCount ?? summary?.rejectedCount ?? 0));
  const changed = Object.prototype.hasOwnProperty.call(counts || {}, "changed") || Object.prototype.hasOwnProperty.call(summary || {}, "changed")
    ? ` | changed ${(counts?.changed ?? summary?.changed) ? "yes" : "no"}`
    : "";
  return `active ${compactCount(active)} | pending ${compactCount(pending)} | rejected ${compactCount(rejected)}${changed}`;
}

function formatPipelineCounts(counts, progress) {
  const currentStep = Math.max(0, Number(counts?.currentStep || 0));
  const totalSteps = Math.max(0, Number(counts?.totalSteps || 0));
  const baseline = Math.max(0, Number(counts?.baselineOutputCount || 0));
  const final = Math.max(0, Number(counts?.finalOutputCount || 0));
  void progress;
  const stepLabel = totalSteps > 0
    ? `step ${compactCount(currentStep)}/${compactCount(totalSteps)}`
    : `step ${compactCount(currentStep)}`;
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
