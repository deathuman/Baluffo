import {
  formatTaskProgressCounts,
  normalizeTaskProgressPayload
} from "../../shared/task-progress.js";
import { buildTaskRunView } from "../../shared/task-run-view-model.js";
import {
  coerceReportDetailRow,
  normalizeSourceStatusToken
} from "./sources.js";

function normalizeTaskProgressContract(progress) {
  return normalizeTaskProgressPayload(progress);
}

function deriveSharedTaskRunProgressModel(report, {
  taskLabel,
  taskType,
  running = false
} = {}) {
  const safeReport = report && typeof report === "object" && !Array.isArray(report) ? report : null;
  if (!safeReport || !safeReport.taskProgress) return null;
  const finished = Boolean(String(safeReport.finishedAt || "").trim());
  if (finished && !running) return null;
  const normalizedType = String(safeReport.taskType || taskType || "").trim().toLowerCase();
  if (!normalizedType) return null;
  const view = buildTaskRunView({
    ...safeReport,
    taskType: normalizedType,
    active: Boolean(running || safeReport.active)
  });
  const progressLabel = String(view.progressLabel || view.primaryLabel || view.secondaryLabel || "In progress").trim();
  return {
    active: view.status !== "waiting",
    determinate: view.progressMode === "determinate",
    ratio: view.progressMode === "determinate" ? view.progressRatio : 0,
    label: `${taskLabel}: ${progressLabel}`
  };
}

function inferDiscoveryPhaseKeyFromLabel(label, fallback = "") {
  const normalized = String(label || "").trim().toLowerCase();
  if (!normalized) return String(fallback || "").trim();
  if (/prob/i.test(normalized)) return "probing_candidates";
  if (/final/i.test(normalized)) return "finalizing";
  if (/scan/i.test(normalized)) return "scanning_sources";
  if (/seed|provider-pattern|web-search|generating/i.test(normalized)) return "generating_candidates";
  return String(fallback || "").trim() || "starting";
}

function deriveTaskProgressView(progress, {
  taskLabel,
  fallbackPhaseLabel = "",
  formatCountsLabel
} = {}) {
  const normalized = normalizeTaskProgressContract(progress);
  if (!normalized || !normalized.active) {
    return {
      active: false,
      determinate: false,
      ratio: 0,
      label: ""
    };
  }
  const phaseLabel = normalized.phaseLabel || fallbackPhaseLabel || "In progress";
  const countsLabel = typeof formatCountsLabel === "function"
    ? String(formatCountsLabel(normalized.counts, normalized) || "").trim()
    : "";
  return {
    active: true,
    determinate: normalized.mode === "determinate",
    ratio: normalized.mode === "determinate" ? normalized.ratio : 0,
    label: `${taskLabel}: ${phaseLabel}${countsLabel ? ` | ${countsLabel}` : ""}`
  };
}

function mapDiscoveryHybridRatio(progress) {
  const normalized = normalizeTaskProgressContract(progress);
  if (!normalized || !normalized.active) return null;
  const phaseKey = String(normalized.phaseKey || "").trim().toLowerCase();
  const ratio = Number(normalized.ratio || 0);
  switch (phaseKey) {
    case "starting":
      return 0.05;
    case "generating_candidates":
      return 0.28;
    case "scanning_sources":
      return 0.5;
    case "probing_candidates":
      return 0.6 + (Math.max(0, Math.min(1, ratio)) * 0.32);
    case "finalizing":
      return 0.96;
    case "completed":
      return 1;
    default:
      return null;
  }
}

function deriveDiscoveryHybridProgressView(progress, {
  taskLabel,
  fallbackPhaseLabel = "",
  formatCountsLabel
} = {}) {
  const normalized = normalizeTaskProgressContract(progress);
  if (!normalized || !normalized.active) {
    return {
      active: false,
      determinate: false,
      ratio: 0,
      label: ""
    };
  }
  const phaseLabel = normalized.phaseLabel || fallbackPhaseLabel || "In progress";
  const countsLabel = typeof formatCountsLabel === "function"
    ? String(formatCountsLabel(normalized.counts, normalized) || "").trim()
    : "";
  const hybridRatio = mapDiscoveryHybridRatio(normalized);
  const determinate = Number.isFinite(hybridRatio) && hybridRatio !== null;
  return {
    active: true,
    determinate,
    ratio: determinate ? hybridRatio : 0,
    label: `${taskLabel}: ${phaseLabel}${countsLabel ? ` | ${countsLabel}` : ""}`
  };
}

function deriveLegacyFetcherTaskProgress(report, { running = false } = {}) {
  const summary = report?.summary || {};
  const runtime = report?.runtime || {};
  const runtimeSelectedSourceCount = Math.max(0, Number(runtime.selectedSourceCount || 0));
  const summarySourceCount = Math.max(0, Number(summary.sourceCount || 0));
  const successfulSources = Math.max(0, Number(summary.successfulSources || 0));
  const okWithWarningSources = Math.max(0, Number(summary.okWithWarningSources || 0));
  const failedSources = Math.max(0, Number(summary.failedSources || 0));
  const excludedSources = Math.max(0, Number(summary.excludedSources || 0));
  const resolvedSources = successfulSources + failedSources + excludedSources;
  const outputCount = Math.max(0, Number(summary.outputCount || 0));
  const active = Boolean(running) || (!String(report?.finishedAt || "").trim() && (resolvedSources > 0 || outputCount > 0));
  if (!active) return null;
  const finished = Boolean(String(report?.finishedAt || "").trim());
  const runtimeCountMatchesResolvedUnit = (
    runtimeSelectedSourceCount > 0
    && resolvedSources <= runtimeSelectedSourceCount
    && (summarySourceCount <= 0 || summarySourceCount <= runtimeSelectedSourceCount)
  );
  const totalSources = finished
    ? Math.max(summarySourceCount, resolvedSources)
    : (runtimeCountMatchesResolvedUnit ? runtimeSelectedSourceCount : 0);
  return {
    active: true,
    phaseKey: finished ? "completed" : "executing_sources",
    phaseLabel: finished ? "Completed" : "Executing sources",
    mode: totalSources > 0 ? "determinate" : "indeterminate",
    ratio: totalSources > 0 ? Math.max(0, Math.min(1, resolvedSources / totalSources)) : 0,
    counts: {
      resolvedSources,
      sourceCount: totalSources || summarySourceCount || resolvedSources,
      outputCount,
      okWithWarningSources,
      failedSources,
      excludedSources
    }
  };
}

function deriveLegacyDiscoveryTaskProgress(report, { running = false, phaseHint = "" } = {}) {
  const summary = report?.summary || {};
  const phaseLabel = String(phaseHint || summary.phaseLabel || summary.phase || "").trim();
  const foundCount = Math.max(0, Number(summary.foundEndpointCount ?? 0));
  const probedCount = Math.max(0, Number(summary.probedCandidateCount ?? summary.probedCount ?? 0));
  const queuedCount = deriveDiscoveryQueuedCount(report);
  const deferredCount = Math.max(0, Number(summary.discoverableButDeferredCount ?? 0));
  const failedCount = Math.max(0, Number(summary.failedProbeCount || 0));
  const active = Boolean(running) || (!String(report?.finishedAt || "").trim() && (foundCount > 0 || probedCount > 0 || queuedCount > 0 || failedCount > 0 || Boolean(phaseLabel)));
  if (!active) return null;
  const loss = summary?.lossAccounting && typeof summary.lossAccounting === "object"
    ? summary.lossAccounting
    : {};
  const probeTotal = Math.max(
    0,
    Number(loss.generated ?? 0)
      - Number(loss.dedupSkipped ?? 0)
      - Number(loss.validationSkipped ?? 0)
      - Number(loss.lowEvidenceSkipped ?? 0)
      - Number(summary.suppressedStaticCount ?? 0)
  ) || Math.max(0, probedCount, failedCount, queuedCount);
  const finished = Boolean(report?.finishedAt);
  const phaseKey = finished
    ? "completed"
    : (/prob/i.test(phaseLabel) ? "probing_candidates" : (String(summary.phaseKey || summary.phase || "").trim() || "scanning_sources"));
  return {
    active: !finished,
    phaseKey,
    phaseLabel: finished ? "Discovery completed" : (phaseLabel || "Initializing scan"),
    mode: (finished || (phaseKey === "probing_candidates" && probeTotal > 0)) ? "determinate" : "indeterminate",
    ratio: finished ? 1 : (phaseKey === "probing_candidates" && probeTotal > 0 ? Math.max(0, Math.min(1, probedCount / probeTotal)) : 0),
    counts: {
      foundEndpoints: foundCount,
      probedCandidates: probedCount,
      probeTotal,
      queuedCandidates: queuedCount,
      deferredCandidates: deferredCount,
      failedProbes: failedCount
    }
  };
}

export function deriveFetcherTaskProgress(report, { running = false } = {}) {
  const normalized = normalizeTaskProgressContract(report?.taskProgress);
  const finished = Boolean(String(report?.finishedAt || "").trim());
  if (finished) {
    return deriveLegacyFetcherTaskProgress(report, { running: false });
  }
  return normalized || deriveLegacyFetcherTaskProgress(report, { running });
}

export function deriveDiscoveryTaskProgress(report, { running = false, phaseHint = "" } = {}) {
  const rawTaskProgress = report?.taskProgress;
  let normalized = normalizeTaskProgressContract(rawTaskProgress);
  const finished = Boolean(String(report?.finishedAt || "").trim());
  if (normalized && !finished) {
    const rawObj = rawTaskProgress && typeof rawTaskProgress === "object" && !Array.isArray(rawTaskProgress)
      ? rawTaskProgress
      : null;
    if (rawObj && !Object.prototype.hasOwnProperty.call(rawObj, "active")) {
      normalized = { ...normalized, active: true };
    }
  }
  const nextPhaseHint = String(phaseHint || "").trim();
  if (normalized) {
    const phaseKey = String(normalized.phaseKey || "").trim();
    const phaseLabel = String(normalized.phaseLabel || "").trim();
    const shouldUsePhaseHint = !finished && normalized.active && nextPhaseHint && (
      !phaseLabel
      || /^initializing scan$/i.test(phaseLabel)
      || !phaseKey
      || phaseKey === "starting"
    );
    if (shouldUsePhaseHint) {
      return {
        ...normalized,
        phaseKey: inferDiscoveryPhaseKeyFromLabel(nextPhaseHint, phaseKey),
        phaseLabel: nextPhaseHint
      };
    }
    return normalized;
  }
  return deriveLegacyDiscoveryTaskProgress(report, { running, phaseHint });
}

function formatFetcherCountsLabel(counts, progress) {
  return formatTaskProgressCounts("fetch", counts, progress);
}

function formatDiscoveryCountsLabel(counts, progress) {
  return formatTaskProgressCounts("discovery", counts, progress);
}

export function applyOptimisticDiscoveryRun(model, optimisticRun, nowMs = Date.now()) {
  const baseModel = model && typeof model === "object"
    ? model
    : {
        currentRows: [],
        visibleCompletedRows: [],
        olderCompletedRows: [],
        hasLiveRuns: false,
        liveTypes: []
      };
  void optimisticRun;
  void nowMs;
  return baseModel;
}

export function deriveFetcherProgressModel(report, { running = false } = {}) {
  const shared = deriveSharedTaskRunProgressModel(report, {
    taskLabel: "Fetcher",
    taskType: "fetch",
    running
  });
  if (shared) return shared;
  return deriveTaskProgressView(
    deriveFetcherTaskProgress(report, { running }),
    {
      taskLabel: "Fetcher",
      fallbackPhaseLabel: "Executing sources",
      formatCountsLabel: formatFetcherCountsLabel
    }
  );
}

export function deriveDiscoveryProgressModel(report, { running = false, phaseHint = "" } = {}) {
  return deriveDiscoveryHybridProgressView(
    deriveDiscoveryTaskProgress(report, { running, phaseHint }),
    {
      taskLabel: "Discovery",
      fallbackPhaseLabel: "Initializing scan",
      formatCountsLabel: formatDiscoveryCountsLabel
    }
  );
}

export function applyOptimisticFetchRun(model, optimisticRun, nowMs = Date.now()) {
  const baseModel = model && typeof model === "object"
    ? model
    : {
        currentRows: [],
        visibleCompletedRows: [],
        olderCompletedRows: [],
        hasLiveRuns: false,
        liveTypes: []
      };
  void optimisticRun;
  void nowMs;
  return baseModel;
}

function hasReportFailureSignal(row) {
  if (!row || typeof row !== "object") return false;
  const status = normalizeSourceStatusToken(row?.status);
  if (status === "error") return true;
  return String(row?.error || "").trim().length > 0;
}

function classifyFetcherFailureBucket(row) {
  const adapter = String(row?.adapter || row?._groupAdapter || "").trim().toLowerCase();
  const error = String(row?.error || "").trim().toLowerCase();
  const low = `${adapter} ${error}`.trim();
  if (
    /fetch_ok_extract_zero|extract[_ -]?zero|no jobs extracted|no jobs parsed|empty extract|parsed 0 jobs|keptcount.?0/.test(low)
  ) {
    return "extract_zero";
  }
  if (
    /blocked|challenge|captcha|cloudflare|access denied|forbidden|http 403|status 403|linkedin 999|http 999/.test(low)
  ) {
    return "blocked_or_challenge";
  }
  if (/timed out|timeout|time out|read timed out/.test(low)) {
    return "timeout";
  }
  if (
    ["ashby", "personio", "greenhouse", "lever", "smartrecruiters", "workable", "recruitee", "pinpoint", "breezy", "jazzhr", "teamtailor"].includes(adapter)
    && /http 429|rate limit|too many requests/.test(low)
  ) {
    return "provider_rate_limited";
  }
  if (
    ["ashby", "personio", "greenhouse", "lever", "smartrecruiters", "workable", "recruitee", "pinpoint", "breezy", "jazzhr", "teamtailor"].includes(adapter)
    && /http 404|not found|bad config|missing board|missing feed|missing slug|missing board_url|missing feed_url|invalid .*host/.test(low)
  ) {
    return "provider_not_found_or_bad_config";
  }
  return "uncategorized";
}

export function deriveFetcherFailureSummary(report) {
  const groups = Array.isArray(report?.sources) ? report.sources : [];
  const buckets = new Map();
  let topLevelFailedSources = 0;
  let detailFailureCount = 0;

  function pushBucketRow(bucketKey, row) {
    const existing = buckets.get(bucketKey) || {
      key: bucketKey,
      count: 0,
      examples: []
    };
    existing.count += 1;
    const name = String(row?.name || row?.studio || row?._groupName || "unknown").trim();
    if (name && !existing.examples.includes(name) && existing.examples.length < 4) {
      existing.examples.push(name);
    }
    buckets.set(bucketKey, existing);
  }

  groups.forEach(group => {
    if (!group || typeof group !== "object") return;
    if (normalizeSourceStatusToken(group?.status) === "error") {
      topLevelFailedSources += 1;
    }
    const parsedDetails = (Array.isArray(group?.details) ? group.details : [])
      .map(coerceReportDetailRow)
      .filter(detail => detail && typeof detail === "object");
    const failingDetails = parsedDetails.filter(detail => hasReportFailureSignal(detail));
    if (failingDetails.length) {
      detailFailureCount += failingDetails.length;
      failingDetails.forEach(detail => {
        pushBucketRow(classifyFetcherFailureBucket({
          ...detail,
          _groupName: String(group?.name || ""),
          _groupAdapter: String(group?.adapter || "")
        }), detail);
      });
      return;
    }
    if (hasReportFailureSignal(group)) {
      pushBucketRow(classifyFetcherFailureBucket(group), group);
    }
  });

  const bucketOrder = [
    "extract_zero",
    "blocked_or_challenge",
    "timeout",
    "provider_rate_limited",
    "provider_not_found_or_bad_config",
    "uncategorized"
  ];

  return {
    topLevelFailedSources,
    detailFailureCount,
    buckets: bucketOrder
      .map(key => buckets.get(key))
      .filter(Boolean)
  };
}

export function deriveDiscoveryQueuedCount(report) {
  const summary = report?.summary || {};
  const summaryQueued = Math.max(0, Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0));
  const candidates = Array.isArray(report?.candidates) ? report.candidates : [];
  const derivedQueued = candidates.filter(row => row && typeof row === "object" && !row.deferred).length;
  return Math.max(summaryQueued, derivedQueued);
}

export function deriveDiscoveryLifecycleCounts(report) {
  const summary = report?.summary || {};
  return {
    validated: Math.max(0, Number(summary.validatedCandidateCount ?? 0)),
    approved: Math.max(0, Number(summary.approvedCandidateCount ?? 0)),
    live: Math.max(0, Number(summary.liveCandidateCount ?? 0)),
    quarantined: Math.max(0, Number(summary.quarantinedCandidateCount ?? 0))
  };
}
