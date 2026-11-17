/**
 * @fileoverview Rehearse / Run / Stop for the Runtime Database Storage panel.
 *
 * Every action is a request against the shipped reclaim routes, and every render
 * comes from their `storageReclaim` payload, so the panel never owns the run: the
 * bridge does, keyed by data directory and journaled. A live run is the one thing
 * that changes without a click, so while one is live the panel reads the status
 * again on its own — bounded in time and cancelled the moment the run settles, so
 * a finished run closes its modal instead of waiting for a manual Refresh. Refresh
 * reads the route with `?refresh=1` so the verdict, the status and the measured
 * gate's warnings arrive together.
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
import { maybeUnrefTimer } from "../../shared/ui-helpers.js";

const DEFAULT_TIMEOUT_MS = 5000;
// A copy can take minutes, so the follow-up read outlasts a click-driven panel.
// It has no window of its own: the run's terminal status is what ends it, and the
// bridge bounds every run, so a finished copy is never left showing a stale
// phase because the operator walked away.
const LIVE_READ_INTERVAL_MS = 3000;
// A bridge that stopped answering must not be polled for the life of the tab.
// One failed read is a blip — a restart, a busy route; five in a row is ~15 s
// without an answer, so the follow-up stops and leaves the last status on screen.
// Any read that answers again resumes it.
const LIVE_READ_MAX_FAILURES = 5;

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
  timeoutMs = DEFAULT_TIMEOUT_MS,
  setTimer = callback => maybeUnrefTimer(globalThis.setTimeout(callback, LIVE_READ_INTERVAL_MS)),
  clearTimer = timer => globalThis.clearTimeout(timer)
}) {
  let reclaim = null;
  let verdict = null;
  let message = "";
  // A refusal is the bridge's answer to a click, so a later read must not erase
  // it before the operator has seen it; a transport error is ours to replace.
  let messageIsRefusal = false;
  let busy = "";
  let inFlight = false;
  // The container modal can be dismissed without cancelling: the run continues.
  let dismissedRunId = "";
  let liveReadTimer = null;
  let liveReadFailures = 0;

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
    const html =
      runId && runId === dismissedRunId
        ? ""
        : renderStorageReclaimModalHtml(reclaim);
    const body = el("adminStorageReclaimModalBodyEl");
    if (body) body.innerHTML = html;
    modal.hidden = !html;
  }

  function render() {
    const status = reclaimStatusText(reclaim);
    setText(el("adminStorageReclaimStatusEl"), status.text, status.tone);
    const dialog = el("adminStorageReclaimDialogEl");
    if (dialog) {
      const body = renderStorageReclaimDialogHtml(reclaim, formatBytes);
      dialog.innerHTML = message
        ? `<p class="admin-storage-reclaim-blocked">${message}</p>${body}`
        : body;
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

  function stopLiveRead() {
    if (liveReadTimer !== null) clearTimer(liveReadTimer);
    liveReadTimer = null;
  }

  /**
   * Read the status again while a run is live, one read at a time. The read is
   * what settles the panel: a terminal status renders, and `accept` cancels the
   * next one, so a finished run closes its own modal.
   */
  function scheduleLiveRead() {
    if (!reclaimRunning(reclaim) || typeof getBridge !== "function") {
      stopLiveRead();
      return;
    }
    if (liveReadFailures >= LIVE_READ_MAX_FAILURES) return;
    // One timer, however many payloads arrive: a pending read already covers them.
    if (liveReadTimer !== null) return;
    liveReadTimer = setTimer(() => {
      liveReadTimer = null;
      // `refresh` is async, so it always answers with a promise; it still accepts
      // (and reschedules from) payloads that carry only the run's status.
      void refresh().finally(scheduleLiveRead);
    });
  }

  /** Accept any payload that carries a `storageReclaim` object. */
  function accept(payload) {
    if (payload?.storageReclaim && typeof payload.storageReclaim === "object") {
      const incoming = payload.storageReclaim;
      // A start or cancel answers with the run's status only, so it carries no scope.
      // The panel keeps the presentation it already read: the container modal opens
      // from the click's own payload instead of waiting for the follow-up read.
      reclaim = incoming.scope ? incoming : { ...incoming, scope: reclaim?.scope || "" };
      if (reclaim.verdict) verdict = reclaim.verdict;
      if (String(reclaim.runId || "") !== dismissedRunId) dismissedRunId = "";
    }
    if (payload?.verdict && typeof payload.verdict === "object") verdict = payload.verdict;
    // The bridge answered, so the follow-up has its footing back.
    liveReadFailures = 0;
    render();
    scheduleLiveRead();
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
      if (!messageIsRefusal) message = "";
      return accept(payload);
    } catch (err) {
      // The last status stays on screen: it is the last thing the bridge said.
      liveReadFailures += 1;
      message = `Reclaim status unavailable: ${err?.message || err || "unknown error"}`;
      messageIsRefusal = false;
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
    messageIsRefusal = false;
    render();
    try {
      const body = action === "rehearse" ? { dryRun: true } : {};
      const payload = await postBridge(STORAGE_RECLAIM_ROUTE, body, {
        timeoutMs,
        affectsReachability: false,
        allowStatuses: [409]
      });
      accept(payload);
      if (!payload?.started) {
        message = refusalText(payload?.reason);
        messageIsRefusal = true;
      }
      return payload;
    } catch (err) {
      message = `Could not start the ${action === "rehearse" ? "rehearsal" : "reclaim"}: ${
        err?.message || err || "unknown error"
      }`;
      messageIsRefusal = false;
      return null;
    } finally {
      busy = "";
      render();
      // One read after the click; a live run then follows it on its own.
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
    messageIsRefusal = false;
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
      if (payload?.stale) {
        message = REFUSAL_TEXT["run-id-mismatch"];
        messageIsRefusal = true;
      } else if (payload?.ok === false) {
        message = refusalText(payload?.reason);
        messageIsRefusal = true;
      }
      return payload;
    } catch (err) {
      message = `Could not stop the run: ${err?.message || err || "unknown error"}`;
      messageIsRefusal = false;
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

  /**
   * The modal is the container's only view of a live run, so ESC and a scrim click
   * are not ways out of it: hiding it there would look like a cancel that never
   * happened. **Hide** and **Stop** stay the exits.
   */
  function liveModalVisible() {
    const modal = el("adminStorageReclaimModalEl");
    return Boolean(modal && !modal.hidden && reclaimRunning(reclaim));
  }

  function blockEscapeWhileLive(event) {
    if (event?.key !== "Escape") return;
    if (!liveModalVisible()) return;
    event.preventDefault?.();
    event.stopPropagation?.();
  }

  /** Only a click whose target is the scrim itself counts as a backdrop click. */
  function blockBackdropWhileLive(event) {
    if (!liveModalVisible()) return;
    if (event?.target !== el("adminStorageReclaimModalEl")) return;
    event.preventDefault?.();
    event.stopPropagation?.();
  }

  /** A native `<dialog>` reports ESC as `cancel`; the same rule holds if this becomes one. */
  function blockCancelWhileLive(event) {
    if (!liveModalVisible()) return;
    event.preventDefault?.();
  }

  function init() {
    el("adminStorageReclaimRehearseBtnEl")?.addEventListener?.("click", () => start("rehearse"));
    el("adminStorageReclaimRunBtnEl")?.addEventListener?.("click", () => start("run"));
    el("adminStorageReclaimStopBtnEl")?.addEventListener?.("click", () => stop());
    el("adminStorageReclaimModalStopBtnEl")?.addEventListener?.("click", () => stop());
    el("adminStorageReclaimModalHideBtnEl")?.addEventListener?.("click", () => dismissModal());
    globalThis.document?.addEventListener?.("keydown", blockEscapeWhileLive);
    const modal = el("adminStorageReclaimModalEl");
    modal?.addEventListener?.("click", blockBackdropWhileLive);
    modal?.addEventListener?.("cancel", blockCancelWhileLive);
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
