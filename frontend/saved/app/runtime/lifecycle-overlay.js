export const SAVED_LIFECYCLE_JOBS_URLS = [
  "data/jobs-unified-light.json",
  "data/jobs-unified.json",
  "jobs-unified-light.json",
  "jobs-unified.json"
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
    lifecycleReason: String(row?.lifecycleReason || "").trim().toLowerCase()
  };
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
    generateJobKeyForRow = () => ""
  } = options;
  const overlayByJobKey = new Map();

  for (const row of canonicalRows) {
    const jobKey = String(generateJobKeyForRow(row) || "").trim().toLowerCase();
    if (!jobKey) continue;
    overlayByJobKey.set(jobKey, toLifecycleOverlayRecord(row));
  }
  for (const row of lifecycleRows) {
    const jobKey = String(generateJobKeyForRow(row) || "").trim().toLowerCase();
    if (!jobKey || overlayByJobKey.has(jobKey)) continue;
    overlayByJobKey.set(jobKey, toLifecycleOverlayRecord(row));
  }
  return overlayByJobKey;
}

export async function loadSavedLifecycleOverlayByJobKey(options = {}) {
  const {
    fetchJsonFromCandidatesFn = async () => null,
    parseUnifiedJobsPayloadFn = payload => {
      if (Array.isArray(payload)) return payload;
      if (payload && typeof payload === "object" && Array.isArray(payload.jobs)) return payload.jobs;
      return [];
    },
    normalizeJobsFn = rows => (Array.isArray(rows) ? rows : []),
    generateJobKeyForRow = () => ""
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
    generateJobKeyForRow
  });
}
