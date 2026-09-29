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

const ACTIVE_TASK_STATUSES = new Set(["running", "starting", "pending", "queued", "aborting"]);

/**
 * @param {import("./types.js").TaskStateRow|null|undefined} task
 * @returns {boolean}
 */
export function isActiveTaskStateRow(task) {
  const status = String(task?.lifecycleStatus || task?.status || "").trim().toLowerCase();
  return Boolean(task?.active || task?.taskProgress?.active || task?.progress?.active)
    || (!String(task?.finishedAt || "").trim() && ACTIVE_TASK_STATUSES.has(status));
}

/**
 * @param {import("./types.js").TaskStatePayload|null|undefined} payload
 * @returns {boolean}
 */
export function hasActiveTaskStateRows(payload) {
  return getTaskStateRows(payload).some(isActiveTaskStateRow);
}

/**
 * Split a raw bridge log chunk into complete log lines.
 *
 * Bridge log reads are bounded byte windows, so a chunk can end in the middle of
 * a line the writer is still flushing. `carry` holds that trailing fragment back
 * so it is never rendered as its own row (which would show no log timestamp);
 * it is prepended to the next chunk instead.
 *
 * @param {string} text
 * @param {{ carry?: string }} [options]
 * @returns {{ lines: string[], carry: string }}
 */
export function splitServerLogChunk(text, options = {}) {
  const chunk = String(options?.carry || "") + String(text || "");
  if (!chunk) {
    return { lines: [], carry: "" };
  }
  const parts = chunk.split(/\r?\n/);
  const carry = parts.pop() || "";
  return {
    lines: parts.filter(line => String(line || "").trim()),
    carry
  };
}
