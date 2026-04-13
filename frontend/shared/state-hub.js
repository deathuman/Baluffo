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
      } catch (_ignored) {
        // ignore listener errors
      }
    });
  }
  const allListeners = listeners["*"];
  if (allListeners) {
    allListeners.forEach((cb) => {
      try {
        cb(key, value);
      } catch (_ignored) {
        // ignore listener errors
      }
    });
  }
}
