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

export function buildOpsFetcherMetricSections(sectionContentByKey = {}) {
  const content = sectionContentByKey && typeof sectionContentByKey === "object"
    ? sectionContentByKey
    : {};
  return getOpsFetcherMetricSectionDefinitions()
    .map(section => ({
      ...section,
      html: String(content[section.key] || "")
    }))
    .filter(section => section.html.trim());
}

function normalizeRunType(row) {
  return String(row?.taskType || row?.type || "").trim().toLowerCase();
}

function normalizeRunStatus(row) {
  return String(row?.displayStatus || row?.status || "unknown").trim().toLowerCase() || "unknown";
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
      hasRun: Boolean(row),
      isLive: Boolean(liveRow),
      elapsedMs: toNumber(row?.elapsedMs ?? row?.durationMs),
      summary: formatTaskLaneSummary(type, row)
    };
  });
}
