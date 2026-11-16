/**
 * @fileoverview Rehearse / Run / Stop for the Runtime Database Storage panel.
 *
 * Every action is a request against the shipped reclaim routes, and every render
 * comes from their `storageReclaim` payload, so the panel never owns the run: the
 * bridge does, keyed by data directory and journaled. There is deliberately no
 * polling timer — a run reports its phase on demand, and Refresh reads the route
 * with `?refresh=1` so the verdict, the status and the measured gate's warnings
 * arrive together.
 *
 * The admin shell has a hard byte budget, so these controls live behind the
 * dynamically imported storage panel and resolve their own elements by id; the
 * shared ref map only carries the warning block it already had.
 */

import {
  reclaimButtonState,
  reclaimRunning,
  reclaimStatusText,
  renderStorageReclaimDialogHtml,
  renderStorageReclaimModalHtml,
  STORAGE_RECLAIM_ROUTE
} from "../render/storage-reclaim-dialog.js";
import { renderStorageReclaimHealthHtml } from "../render/storage-reclaim.js";

const DEFAULT_TIMEOUT_MS = 5000;

const ELEMENT_IDS = {
  adminStorageReclaimHealthEl: "admin-storage-reclaim-health",
  adminStorageReclaimStatusEl: "admin-storage-reclaim-status",
  adminStorageReclaimDialogEl: "admin-storage-reclaim-dialog",
  adminStorageReclaimRehearseBtnEl: "admin-storage-reclaim-rehearse-btn",
  adminStorageReclaimRunBtnEl: "admin-storage-reclaim-run-btn",
  adminStorageReclaimStopBtnEl: "admin-storage-reclaim-stop-btn",
  adminStorageReclaimModalEl: "admin-storage-reclaim-modal",
  adminStorageReclaimModalBodyEl: "admin-storage-reclaim-modal-body",
  adminStorageReclaimModalStopBtnEl: "admin-storage-reclaim-modal-stop-btn",
  adminStorageReclaimModalHideBtnEl: "admin-storage-reclaim-modal-hide-btn"
};

const REFUSAL_TEXT = {
  "not-bloated": "The database is not bloated enough to shrink. Rehearse still measures the copy.",
  "already-running": "A reclaim or rehearsal is already running.",
  "rebuild-running": "A current-feed rebuild is running; wait for it to finish.",
  "no-run": "There is no run to stop.",
  "run-id-required": "Stop needs the current run id; refresh and try again.",
  "run-id-mismatch": "That run id is stale; refresh and try again.",
  "phase-not-cancellable": "This phase cannot be stopped; refresh to follow it."
};

function refusalText(reason) {
  const key = String(reason || "").trim();
  if (key in REFUSAL_TEXT) return REFUSAL_TEXT[key];
  if (key) return `Did not start: ${key.replace(/-/g, " ")}.`;
  return "";
}

export function createStorageReclaimController({
  refs,
  getBridge,
  postBridge,
  formatBytes,
  timeoutMs = DEFAULT_TIMEOUT_MS
}) {
  let reclaim = null;
  let verdict = null;
  let message = "";
  let busy = "";
  let inFlight = false;
  // The container modal can be dismissed without cancelling: the run continues.
  let dismissedRunId = "";

  function el(name) {
    const injected = refs?.[name];
    if (injected) return injected;
    const id = ELEMENT_IDS[name];
    return id ? globalThis.document?.getElementById?.(id) || null : null;
  }

  function setText(element, value, tone = "") {
    if (!element) return;
    element.textContent = value;
    element.setAttribute?.("data-tone", tone || "");
  }

  function renderButtons() {
    const buttons = reclaimButtonState(reclaim, { busy, verdict });
    const rehearse = el("adminStorageReclaimRehearseBtnEl");
    if (rehearse) {
      rehearse.disabled = buttons.rehearse.disabled;
      rehearse.textContent = buttons.rehearse.label;
      rehearse.setAttribute?.("data-tooltip", buttons.rehearse.tooltip);
    }
    const run = el("adminStorageReclaimRunBtnEl");
    if (run) {
      run.disabled = buttons.run.disabled;
      run.textContent = buttons.run.label;
      run.setAttribute?.("data-tooltip", buttons.run.tooltip);
    }
    const stop = el("adminStorageReclaimStopBtnEl");
    if (stop) {
      stop.hidden = buttons.stop.hidden;
      stop.disabled = buttons.stop.disabled;
      stop.textContent = buttons.stop.label;
      stop.setAttribute?.("data-tooltip", buttons.stop.tooltip);
    }
    // The container modal blocks the panel, so it carries its own Stop.
    const modalStop = el("adminStorageReclaimModalStopBtnEl");
    if (modalStop) {
      modalStop.disabled = buttons.stop.disabled;
      modalStop.textContent = buttons.stop.label;
      modalStop.setAttribute?.("data-tooltip", buttons.stop.tooltip);
    }
  }

  function renderModal() {
    const modal = el("adminStorageReclaimModalEl");
    if (!modal) return;
    const runId = String(reclaim?.runId || "");
    const html = runId && runId === dismissedRunId ? "" : renderStorageReclaimModalHtml(reclaim);
    const body = el("adminStorageReclaimModalBodyEl");
    if (body) body.innerHTML = html;
    modal.hidden = !html;
  }

  function render() {
    const status = reclaimStatusText(reclaim);
    setText(el("adminStorageReclaimStatusEl"), status.text, status.tone);
    const dialog = el("adminStorageReclaimDialogEl");
    if (dialog) {
      dialog.innerHTML = message
        ? `<p class="admin-storage-reclaim-blocked">${message}</p>${renderStorageReclaimDialogHtml(reclaim, formatBytes)}`
        : renderStorageReclaimDialogHtml(reclaim, formatBytes);
    }
    const health = el("adminStorageReclaimHealthEl");
    if (health) {
      const html = renderStorageReclaimHealthHtml(verdict);
      health.innerHTML = html;
      health.hidden = !html;
    }
    renderButtons();
    renderModal();
  }

  /** Accept any payload that carries a `storageReclaim` object. */
  function accept(payload) {
    if (payload?.storageReclaim && typeof payload.storageReclaim === "object") {
      reclaim = payload.storageReclaim;
      if (reclaim.verdict) verdict = reclaim.verdict;
      if (String(reclaim.runId || "") !== dismissedRunId) dismissedRunId = "";
    }
    if (payload?.verdict && typeof payload.verdict === "object") verdict = payload.verdict;
    render();
    return reclaim;
  }

  /**
   * Read the route once. `refreshVerdict` adds `?refresh=1` so the cached verdict
   * is recomputed, which is the panel's only way to see new measurements.
   */
  async function refresh({ refreshVerdict = false } = {}) {
    if (inFlight || typeof getBridge !== "function") return null;
    inFlight = true;
    try {
      const query = refreshVerdict ? "?refresh=1" : "";
      const payload = await getBridge(`${STORAGE_RECLAIM_ROUTE}${query}`, {
        timeoutMs,
        affectsReachability: false
      });
      message = "";
      return accept(payload);
    } catch (err) {
      message = `Reclaim status unavailable: ${err?.message || err || "unknown error"}`;
      render();
      return null;
    } finally {
      inFlight = false;
    }
  }

  async function start(kind) {
    const action = kind === "rehearse" ? "rehearse" : "run";
    if (busy || reclaimRunning(reclaim) || typeof postBridge !== "function") return null;
    busy = action;
    message = "";
    render();
    try {
      const body = action === "rehearse" ? { dryRun: true } : {};
      const payload = await postBridge(STORAGE_RECLAIM_ROUTE, body, {
        timeoutMs,
        affectsReachability: false,
        allowStatuses: [409]
      });
      accept(payload);
      if (!payload?.started) message = refusalText(payload?.reason);
      return payload;
    } catch (err) {
      message = `Could not start the ${action === "rehearse" ? "rehearsal" : "reclaim"}: ${
        err?.message || err || "unknown error"
      }`;
      return null;
    } finally {
      busy = "";
      render();
      // One read, not a poll: the verdict and the status progress together.
      void refresh({ refreshVerdict: true });
    }
  }

  async function stop() {
    if (busy || typeof postBridge !== "function" || !reclaimRunning(reclaim)) return null;
    const runId = String(reclaim?.runId || "").trim();
    if (!runId) {
      message = REFUSAL_TEXT["run-id-required"];
      render();
      return null;
    }
    busy = "stop";
    message = "";
    // The button says what it is doing before the bridge answers.
    reclaim = { ...(reclaim || {}), status: "cancelling" };
    render();
    try {
      const payload = await postBridge(`${STORAGE_RECLAIM_ROUTE}/cancel`, { runId }, {
        timeoutMs,
        affectsReachability: false,
        allowStatuses: [409]
      });
      accept(payload);
      if (payload?.stale) message = REFUSAL_TEXT["run-id-mismatch"];
      else if (payload?.ok === false) message = refusalText(payload?.reason);
      return payload;
    } catch (err) {
      message = `Could not stop the run: ${err?.message || err || "unknown error"}`;
      return null;
    } finally {
      busy = "";
      render();
    }
  }

  function dismissModal() {
    dismissedRunId = String(reclaim?.runId || "");
    renderModal();
  }

  function init() {
    el("adminStorageReclaimRehearseBtnEl")?.addEventListener?.("click", () => start("rehearse"));
    el("adminStorageReclaimRunBtnEl")?.addEventListener?.("click", () => start("run"));
    el("adminStorageReclaimStopBtnEl")?.addEventListener?.("click", () => stop());
    el("adminStorageReclaimModalStopBtnEl")?.addEventListener?.("click", () => stop());
    el("adminStorageReclaimModalHideBtnEl")?.addEventListener?.("click", () => dismissModal());
  }

  return {
    init,
    accept,
    refresh,
    rehearse: () => start("rehearse"),
    run: () => start("run"),
    stop,
    dismissModal
  };
}
