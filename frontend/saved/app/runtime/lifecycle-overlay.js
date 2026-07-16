export const SAVED_LIFECYCLE_JOBS_URLS = [
  "data/jobs-unified-light.json"
];

export const SAVED_LIFECYCLE_STATE_URLS = [
  "data/jobs-lifecycle-state.json",
  "jobs-lifecycle-state.json"
];

function toLifecycleOverlayRecord(row) {
  return {
    status: String(row?.status || "").trim().toLowerCase(),
    removedAt: String(row?.removedAt || "").trim(),
    lastSeenAt: String(row?.lastSeenAt || "").trim(),
    lifecycleEvent: String(row?.lifecycleEvent || "").trim().toLowerCase(),
    lifecycleReason: String(row?.lifecycleReason || "").trim().toLowerCase(),
    availabilityId: String(row?.availabilityId || "").trim(),
    availabilityStatus: String(row?.availabilityStatus || "").trim().toLowerCase(),
    availabilityCheckedAt: String(row?.availabilityCheckedAt || "").trim(),
    availabilityVerifiedAt: String(row?.availabilityVerifiedAt || "").trim(),
    availabilityUnavailableAt: String(row?.availabilityUnavailableAt || "").trim(),
    availabilityEvidence: row?.availabilityEvidence && typeof row.availabilityEvidence === "object"
      ? { ...row.availabilityEvidence }
      : {}
  };
}

function overlayKeys(row) {
  const keys = [];
  const availabilityId = String(row?.availabilityId || "").trim().toLowerCase();
  const jobKey = String(row?.jobKey || row?.dedupKey || "").trim().toLowerCase();
  if (availabilityId) keys.push(`availability:${availabilityId}`);
  if (jobKey) keys.push(`job:${jobKey}`);
  return keys;
}

export function parseLifecycleStatePayload(payload) {
  const rows = payload && typeof payload === "object" && payload.jobs && typeof payload.jobs === "object"
    ? Object.values(payload.jobs)
    : [];
  return rows.filter(row => row && typeof row === "object");
}

export function buildSavedLifecycleOverlayByJobKey(options = {}) {
  const {
    canonicalRows = [],
    lifecycleRows = [],
    runtimeRows = []
  } = options;
  const overlayByJobKey = new Map();

  for (const row of canonicalRows) {
    for (const key of overlayKeys(row)) {
      overlayByJobKey.set(key, toLifecycleOverlayRecord(row));
    }
  }
  for (const row of lifecycleRows) {
    for (const key of overlayKeys(row)) {
      if (!overlayByJobKey.has(key)) {
        overlayByJobKey.set(key, toLifecycleOverlayRecord(row));
      }
    }
  }
  for (const row of runtimeRows) {
    for (const key of overlayKeys(row)) {
      overlayByJobKey.set(key, toLifecycleOverlayRecord(row));
    }
  }
  return overlayByJobKey;
}

async function fetchJsonFromCandidates(urls, options = {}) {
  const timeoutMs = Number(options.timeoutMs) || 3000;
  for (const url of urls || []) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(url, { cache: "no-store", signal: controller.signal });
      if (response.ok) return await response.json();
    } catch {
      // Try the next supported projection.
    } finally {
      clearTimeout(timer);
    }
  }
  return null;
}

export async function loadSavedLifecycleOverlayByJobKey(options = {}) {
  const {
    fetchJsonFromCandidatesFn = fetchJsonFromCandidates,
    parseUnifiedJobsPayloadFn = payload => {
      if (Array.isArray(payload)) return payload;
      if (payload && typeof payload === "object" && Array.isArray(payload.jobs)) return payload.jobs;
      return [];
    },
    normalizeJobsFn = rows => (Array.isArray(rows) ? rows : []),
    runtimeRows = []
  } = options;
  const [canonicalPayload, lifecyclePayload] = await Promise.all([
    fetchJsonFromCandidatesFn(SAVED_LIFECYCLE_JOBS_URLS, { timeoutMs: 3000 }),
    fetchJsonFromCandidatesFn(SAVED_LIFECYCLE_STATE_URLS, { timeoutMs: 3000 })
  ]);
  const canonicalRows = normalizeJobsFn(parseUnifiedJobsPayloadFn(canonicalPayload));
  const lifecycleRows = normalizeJobsFn(parseLifecycleStatePayload(lifecyclePayload));
  return buildSavedLifecycleOverlayByJobKey({
    canonicalRows,
    lifecycleRows,
    runtimeRows
  });
}

export function lifecycleOverlayForSavedJob(overlay, job) {
  if (!(overlay instanceof Map)) return null;
  const availabilityId = String(job?.availabilityId || "").trim().toLowerCase();
  const jobKey = String(job?.jobKey || "").trim().toLowerCase();
  return (availabilityId && overlay.get(`availability:${availabilityId}`))
    || (jobKey && overlay.get(`job:${jobKey}`))
    || null;
}
