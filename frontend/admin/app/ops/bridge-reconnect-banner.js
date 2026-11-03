/**
 * Bridge reconnect presentation for the Admin page.
 *
 * When the local bridge refuses connections the page used to fail silently in
 * three ways at once: `loadAdminBootstrap` never rejects, so the badge was set
 * to "Bridge Online" right after a failed bootstrap; the badge then flipped
 * between Online and Offline because the badge watch and individual bridge
 * calls disagreed on failure counting; and `setOpsReadinessShell` blanked the
 * panels with no explanation. The page looked broken, gave no reason, then
 * appeared to recover on its own ~10s later when the next poll landed.
 *
 * This module owns the honest version of that state in two parts:
 *
 *  - `reconnecting` badge, driven by the bridge status watch.
 *  - A banner that names the wait, plus an explicit retry, shown while the
 *    desktop local-data bootstrap is pending and while the bridge is
 *    unreachable. It auto-dismisses on the first successful bridge call.
 *
 * The boot gate holds the Ops content hidden only for the bounded bootstrap
 * wait, so the fast path is untouched. It force-reveals at the deadline: a page
 * that has given up waiting must still be usable, just labelled degraded.
 *
 * It is deliberately display-only. It never changes when routes are attempted
 * or which degradation windows apply, so it cannot alter the startup or
 * hydration behaviour the packaged startup probes measure.
 */

const RECONNECTING_LABEL = "Bridge Reconnecting";
// The states setBridgeStatusBadge() renders. The waiting state takes the badge
// over, so it has to hand back exactly one of these when the wait ends.
const BADGE_STATES = ["online", "offline", "checking", "degraded"];
const DEFAULT_BADGE = { state: "checking", label: "Bridge Checking" };

// Long enough that a normal bridge start (~6s cold on this hardware) is never
// gated, short enough that a dead bridge does not hold the page hostage.
const BOOTSTRAP_GATE_MS = 8000;
const TICK_INTERVAL_MS = 1000;
const RETRY_BUSY_MS = 400;

/**
 * @param {object} params
 * @param {object} params.refs - Cached admin DOM refs.
 * @param {Function} params.getBootstrapStatus - Returns the local-data bootstrap status.
 * @param {Function} params.onRetryNow - Invoked when the user asks to retry now.
 * @param {Function} [params.nowFn] - Injectable clock for tests.
 * @returns {object} Bridge reconnect controller.
 */
export function createBridgeReconnectBanner({
  refs,
  getBootstrapStatus,
  onRetryNow,
  nowFn = () => Date.now()
}) {
  let bannerShownAt = null;
  let gateStartedAt = null;
  let tickTimer = 0;
  let retryTimer = 0;
  // Reachability is pushed in, not derived from the DOM. The badge label is
  // driven by every bridge call, but reading it back means depending on a CSS
  // class that setBridgeStatusBadge rewrites on each render, and on the bridge
  // status controller's own last-status, which only advances when its watch
  // runs (the Admin runtime does not start one). Owning the flag keeps the
  // banner and the badge reading the same signal.
  let bridgeReachable = true;
  // What the badge said before the wait took it over.
  let preWaitBadge = null;

  const isBootstrapPending = () => getBootstrapStatus() === "pending";
  const isWaiting = () => isBootstrapPending() || !bridgeReachable;

  function elapsedLabel() {
    if (bannerShownAt === null) return "";
    const seconds = Math.max(0, Math.round((nowFn() - bannerShownAt) / 1000));
    return seconds < 1 ? "less than a second" : `${seconds}s`;
  }

  function renderBanner() {
    const el = refs.bridgeReconnectBannerEl;
    if (!el) return;
    const visible = isWaiting();
    el.classList.toggle("hidden", !visible);
    el.setAttribute("aria-hidden", visible ? "false" : "true");
    const labelEl = refs.bridgeReconnectLabelEl;
    if (labelEl) {
      labelEl.textContent = isBootstrapPending()
        ? "Starting the Baluffo bridge..."
        : "Reconnecting to the Baluffo bridge...";
    }
    const elapsedEl = refs.bridgeReconnectElapsedEl;
    if (elapsedEl) {
      elapsedEl.textContent = visible ? `Retrying for ${elapsedLabel()}` : "";
    }
  }

  function snapshotBadge(badge) {
    const state = BADGE_STATES.find(name => badge.classList.contains(name)) || DEFAULT_BADGE.state;
    return {
      state,
      label: String(badge.textContent || "").trim() || DEFAULT_BADGE.label
    };
  }

  function renderBadge() {
    const badge = refs.adminBridgeStatusBadgeEl;
    if (!badge) return;
    if (isWaiting()) {
      if (preWaitBadge === null && !badge.classList.contains("reconnecting")) {
        preWaitBadge = snapshotBadge(badge);
      }
      badge.classList.remove(...BADGE_STATES);
      badge.classList.add("reconnecting");
      badge.textContent = isBootstrapPending() ? "Bridge Starting" : RECONNECTING_LABEL;
      return;
    }
    // Hand the badge back in the state it was in before the wait. Stripping only
    // `reconnecting` left the pending label with no state class: a grey pill
    // reading "Bridge Reconnecting" while every request was succeeding.
    const restore = preWaitBadge || (badge.classList.contains("reconnecting") ? DEFAULT_BADGE : null);
    if (restore === null) return;
    preWaitBadge = null;
    badge.classList.remove("reconnecting", ...BADGE_STATES);
    badge.classList.add(restore.state);
    badge.textContent = restore.label;
  }

  function renderGate() {
    const content = refs.adminContentEl;
    if (!content) return;
    if (!isBootstrapPending()) {
      content.classList.remove("hidden");
      gateStartedAt = null;
      return;
    }
    if (gateStartedAt === null) gateStartedAt = nowFn();
    // Bounded: past the deadline the page is shown regardless, degraded but
    // usable, rather than left blank.
    const expired = nowFn() - gateStartedAt >= BOOTSTRAP_GATE_MS;
    content.classList.toggle("hidden", !expired);
  }

  function render() {
    renderBanner();
    renderBadge();
    renderGate();
  }

  function stopTimers() {
    if (tickTimer) {
      clearInterval(tickTimer);
      tickTimer = 0;
    }
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = 0;
    }
  }

  function setBridgeReachable(nextReachable) {
    if (Boolean(nextReachable) === bridgeReachable) {
      return;
    }
    bridgeReachable = Boolean(nextReachable);
    sync();
  }

  function sync() {
    if (isWaiting()) {
      if (bannerShownAt === null) bannerShownAt = nowFn();
      if (!tickTimer) {
        tickTimer = setInterval(render, TICK_INTERVAL_MS);
        // A parked interval would keep a backgrounded Admin tab awake forever.
        if (typeof tickTimer?.unref === "function") tickTimer.unref();
      }
    } else {
      bannerShownAt = null;
      gateStartedAt = null;
      stopTimers();
    }
    render();
  }

  function bindRetry() {
    const btn = refs.bridgeReconnectRetryBtnEl;
    if (!btn || btn.dataset.bound === "1") return;
    btn.dataset.bound = "1";
    btn.addEventListener("click", () => {
      if (btn.disabled) return;
      btn.disabled = true;
      try {
        onRetryNow?.();
      } finally {
        // The button is re-enabled on the next sync; the timeout only stops it
        // staying stuck when the bridge never answers again.
        retryTimer = setTimeout(() => {
          btn.disabled = false;
          retryTimer = 0;
        }, RETRY_BUSY_MS);
      }
    });
  }

  bindRetry();

  return {
    sync,
    render,
    setBridgeReachable,
    get isWaiting() {
      return isWaiting();
    },
    get bootstrapGateMs() {
      return BOOTSTRAP_GATE_MS;
    }
  };
}
