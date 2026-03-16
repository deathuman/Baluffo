/**
 * Minimal state hub (observer pattern) for cross-module state.
 * Single source of truth for shared UI state so data flow is traceable.
 *
 * Keys (set/read locations):
 * - jobsFeedCount: number of jobs in the feed (set: jobs/app/feed.js after refresh)
 * - jobsLastUpdated: timestamp when feed was last updated (set: jobs/app/feed.js)
 * - savedCount: number of saved jobs (set: saved/app/runtime.js in subscribeToSavedJobs)
 * - savedLastUpdated: timestamp when saved list was last updated (set: saved/app/runtime.js)
 * - authStatus: optional, "locked" | "unlocked" (set: auth modules)
 */

const state = {};
const listeners = {};

/**
 * @param {string} key
 * @returns {unknown}
 */
export function get(key) {
  return state[key];
}

/**
 * @param {string} key
 * @param {unknown} value
 */
export function set(key, value) {
  if (state[key] === value) return;
  state[key] = value;
  const keyListeners = listeners[key];
  if (keyListeners) {
    keyListeners.forEach((cb) => {
      try {
        cb(value);
      } catch (_) {}
    });
  }
  const allListeners = listeners["*"];
  if (allListeners) {
    allListeners.forEach((cb) => {
      try {
        cb(key, value);
      } catch (_) {}
    });
  }
}

/**
 * @param {string} key - State key, or "*" for all keys
 * @param {(value: unknown) => void | ((key: string, value: unknown) => void)} callback
 * @returns {() => void} Unsubscribe function
 */
export function subscribe(key, callback) {
  if (!listeners[key]) listeners[key] = [];
  listeners[key].push(callback);
  const current = key === "*" ? undefined : get(key);
  if (current !== undefined) {
    try {
      if (key === "*") callback(key, current);
      else callback(current);
    } catch (_) {}
  }
  return () => {
    const list = listeners[key];
    if (!list) return;
    const i = list.indexOf(callback);
    if (i !== -1) list.splice(i, 1);
  };
}

/**
 * Subscribe to any state change. Callback receives (key, value).
 * @param {(key: string, value: unknown) => void} callback
 * @returns {() => void} Unsubscribe function
 */
export function subscribeAll(callback) {
  return subscribe("*", callback);
}
