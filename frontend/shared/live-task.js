/**
 * @param {import("./types.js").LiveTaskPayload|null|undefined} payload
 * @returns {Array<import("./types.js").LiveTaskWorkItem>}
 */
export function getLiveTaskWorkItems(payload) {
  if (Array.isArray(payload?.workItems)) return payload.workItems;
  return [];
}

/**
 * @param {import("./types.js").TaskStatePayload|null|undefined} payload
 * @returns {Array<import("./types.js").TaskStateRow>}
 */
export function getTaskStateRows(payload) {
  return Array.isArray(payload?.tasks) ? payload.tasks : [];
}

/**
 * @param {import("./types.js").TaskStatePayload|null|undefined} payload
 * @returns {boolean}
 */
export function hasActiveTaskStateRows(payload) {
  const activeStatuses = new Set(["running", "starting", "pending", "queued", "aborting"]);
  return getTaskStateRows(payload).some(task => {
    const status = String(task?.lifecycleStatus || task?.status || "").trim().toLowerCase();
    return Boolean(task?.active || task?.taskProgress?.active) || activeStatuses.has(status);
  });
}
