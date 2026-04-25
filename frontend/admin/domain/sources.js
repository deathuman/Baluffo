export function getSourceJobsFoundCount(row) {
  const value = Number(
    row?.jobsFound
      ?? row?.sampleCount
      ?? row?._lastKeptCount
      ?? row?.keptCount
      ?? row?.lastKeptCount
      ?? row?._lastFetchedCount
      ?? row?.fetchedCount
      ?? row?.lastFetchedCount
      ?? NaN
  );
  return Number.isFinite(value) ? value : NaN;
}

export function getSourceDiscoveryJobsCount(row) {
  const value = Number(row?.jobsFound ?? row?.sampleCount ?? NaN);
  return Number.isFinite(value) ? value : NaN;
}

export function getSourceFetchJobsCount(row) {
  const value = Number(
    row?._lastKeptCount
      ?? row?.keptCount
      ?? row?.lastKeptCount
      ?? row?._lastFetchedCount
      ?? row?.fetchedCount
      ?? row?.lastFetchedCount
      ?? NaN
  );
  return Number.isFinite(value) ? value : NaN;
}

export function normalizeSourceStatusToken(value) {
  const token = String(value || "").trim().toLowerCase();
  if (!token) return "";
  if (token === "n/a" || token === "na" || token === "unknown" || token === "not_run" || token === "not run yet") {
    return "not_run";
  }
  if (token === "success" || token === "healthy") return "ok";
  if (token === "failed" || token === "failure") return "error";
  return token;
}

export function coerceReportDetailRow(detail) {
  if (detail && typeof detail === "object" && !Array.isArray(detail)) {
    return detail;
  }
  if (typeof detail !== "string") return null;
  const raw = detail.trim();
  if (!raw.startsWith("{") || !raw.endsWith("}")) return null;

  const candidates = [raw];
  const pyLike = raw
    .replace(/\bNone\b/g, "null")
    .replace(/\bTrue\b/g, "true")
    .replace(/\bFalse\b/g, "false");
  if (pyLike !== raw) candidates.push(pyLike);
  if (!raw.includes("\"")) candidates.push(pyLike.replace(/'/g, "\""));

  for (const attempt of candidates) {
    try {
      const parsed = JSON.parse(attempt);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return parsed;
      }
    } catch {
      // Continue trying fallbacks.
    }
  }
  return null;
}

function extractSourceIdFromLoaderName(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  if (raw.startsWith("static_source::")) {
    return raw.slice("static_source::".length).trim();
  }
  return "";
}

function toSourceMatchKeys(row) {
  const out = new Set();
  const studio = String(row?.studio || "").trim().toLowerCase();
  const name = String(row?.name || "").trim().toLowerCase();
  const id = String(row?.id || "").trim().toLowerCase();
  const loaderSourceId = extractSourceIdFromLoaderName(name);
  if (id) out.add(id);
  if (studio) out.add(studio);
  if (name) out.add(name);
  if (loaderSourceId) out.add(loaderSourceId);
  if (studio && name) out.add(`${studio}|${name}`);
  return Array.from(out);
}

function shouldTryGroupErrorMatch(group) {
  const status = normalizeSourceStatusToken(group?.status);
  return status === "error" && String(group?.error || "").trim().length > 0;
}

function rowMatchesGroupError(row, group) {
  if (!shouldTryGroupErrorMatch(group)) return false;
  const errorText = String(group?.error || "").toLowerCase();
  const tokens = toSourceMatchKeys(row).filter(token => token.length >= 4);
  return tokens.some(token => errorText.includes(token));
}

export function deriveSourceStatus(row) {
  const mergedStatus = normalizeSourceStatusToken(row?._lastStatus);
  if (mergedStatus) return mergedStatus;
  const rowStatus = normalizeSourceStatusToken(row?.status);
  if (rowStatus) return rowStatus;
  if (String(row?.lastProbeError || "").trim()) return "error";
  const jobsFound = getSourceFetchJobsCount(row);
  if (Number.isFinite(jobsFound) && jobsFound > 0) return "ok";
  if (String(row?.lastProbedAt || "").trim()) return "warning";
  return "not_run";
}

export function deriveSourceApprovalStatus(row, mode = "pending") {
  const registryState = String(row?.registryState || row?.candidateState || mode || "").trim().toLowerCase();
  const stateChangedBy = String(row?.stateChangedBy || "").trim();

  if (mode === "active" || registryState === "active" || registryState === "live") {
    return {
      label: stateChangedBy ? `Live: ${stateChangedBy}` : "Live",
      title: stateChangedBy
        ? `Active source. Last transition actor: ${stateChangedBy}.`
        : "Active source.",
      tone: "healthy"
    };
  }

  if (mode === "rejected" || registryState === "rejected" || registryState === "quarantined") {
    const reason = String(row?.quarantineReason || row?.pendingReason || row?.deferReason || "").trim();
    return {
      label: reason ? `Rejected: ${reason}` : "Rejected/quarantined",
      title: reason ? `Not eligible because it is rejected or quarantined: ${reason}.` : "Not eligible because it is rejected or quarantined.",
      tone: "critical"
    };
  }

  const lastProbeError = String(row?.lastProbeError || row?.error || "").trim();
  const status = normalizeSourceStatusToken(row?.status);
  const discoveryJobs = getSourceDiscoveryJobsCount(row);

  if (Boolean(row?.deferred)) {
    const reason = String(row?.deferReason || row?.pendingReason || "").trim();
    return {
      label: "Deferred",
      title: reason ? `Not auto-approved because this source is deferred: ${reason}.` : "Not auto-approved because this source is deferred.",
      tone: "warning"
    };
  }

  if (status === "error" || lastProbeError) {
    return {
      label: "Blocked: error",
      title: lastProbeError ? `Not auto-approved because the source has an error: ${lastProbeError}.` : "Not auto-approved because the source status is error.",
      tone: "critical"
    };
  }

  if (Number.isFinite(discoveryJobs) && discoveryJobs > 0) {
    return {
      label: "Auto-approvable",
      title: "Pending source has discovery job evidence and no blocking error.",
      tone: "healthy"
    };
  }

  return {
    label: "Blocked: 0 discovery jobs",
    title: "Not auto-approved because discovery found 0 jobs for this pending source.",
    tone: "warning"
  };
}

export function mergeSourceStatusFromReport(rows, report, mode) {
  const sourceRows = Array.isArray(rows) ? rows : [];
  const groups = Array.isArray(report?.sources) ? report.sources : [];
  const candidates = [];
  groups.forEach(group => {
    if (!group || typeof group !== "object") return;
    candidates.push(group);
    const details = Array.isArray(group?.details) ? group.details : [];
    details.forEach(detail => {
      const parsed = coerceReportDetailRow(detail);
      if (parsed) candidates.push(parsed);
    });
  });
  const byKey = new Map();
  candidates.forEach(candidate => {
    toSourceMatchKeys(candidate).forEach(key => {
      if (!byKey.has(key)) byKey.set(key, candidate);
    });
  });
  return sourceRows.map(row => {
    const keys = toSourceMatchKeys(row);
    const direct = keys.map(key => byKey.get(key)).find(Boolean) || null;
    const matched = direct || groups.find(group => rowMatchesGroupError(row, group)) || null;
    if (!matched) return row;
    return {
      ...row,
      _lastStatus: normalizeSourceStatusToken(matched?.status),
      _lastError: String(matched?.error || ""),
      _lastFetchedCount: Number(matched?.fetchedCount || 0),
      _lastKeptCount: Number(matched?.keptCount || 0),
      _mode: mode
    };
  });
}

export function applySourceFilter(rows, activeSourceFilter) {
  const filter = activeSourceFilter || "all";
  if (filter === "all") return rows;
  return (Array.isArray(rows) ? rows : []).filter(row => {
    const status = deriveSourceStatus(row);
    const jobsFound = getSourceJobsFoundCount(row);
    if (filter === "error") return status === "error";
    if (filter === "excluded") return status === "excluded";
    if (filter === "zero") return jobsFound === 0;
    if (filter === "healthy") return status === "ok" || (jobsFound > 0 && status !== "error");
    return true;
  });
}
