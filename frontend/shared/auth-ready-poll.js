/**
 * @fileoverview Shared auth ready polling utility.
 * Provides a reusable polling mechanism for waiting on auth/API readiness.
 */

/**
 * Creates an auth ready poller.
 * @param {Object} options
 * @param {Function} options.isReady - Predicate returning true when auth/API is ready
 * @param {Function} options.onReady - Callback invoked when isReady() returns true
 * @param {number} [options.initialDelayMs] - Initial poll delay (default: 600)
 * @param {number} [options.minDelayMs] - Minimum poll delay (default: 250)
 * @returns {{ schedulePoll, stopPoll, isPolling }}
 */
export function createAuthReadyPoller({ isReady, onReady, initialDelayMs = 600, minDelayMs = 250 }) {
  let pollTimer = null;

  /**
   * Stops the polling timer.
   */
  function stopPoll() {
    if (!pollTimer) return;
    clearTimeout(pollTimer);
    pollTimer = null;
  }

  /**
   * Schedules a poll check.
   * @param {number} [delayMs] - Delay before check (default: initialDelayMs)
   */
  function schedulePoll(delayMs) {
    stopPoll();
    const delay = Math.max(minDelayMs, Number(delayMs) || initialDelayMs);
    pollTimer = setTimeout(() => {
      pollTimer = null;
      if (isReady()) {
        onReady();
        return;
      }
      schedulePoll(delayMs);
    }, delay);
  }

  /**
   * Returns whether polling is currently active.
   * @returns {boolean}
   */
  function isPolling() {
    return pollTimer !== null;
  }

  return {
    schedulePoll,
    stopPoll,
    isPolling
  };
}
