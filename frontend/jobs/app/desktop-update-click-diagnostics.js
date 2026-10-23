// Why this exists: the packaged smoke occasionally saw the desktop-update
// toggle accept a click without the panel opening, and the only evidence was
// the panel's stale `data-update-state`. A lost click leaves no trace at all,
// so this module records the click *before* the controller can miss it: a
// capture-phase listener on the document sees every click that lands on the
// toggle, and a click that is still unclaimed one task later is reported as
// dropped together with its event path, the controller's bind state, and the
// toggle/panel DOM state at that moment. A click the controller did take is
// reported as handled, so "0 dropped" is a measured result rather than an
// absence of data.

const TOGGLE_ID = "desktop-update-toggle-btn";
const MAX_PATH_DEPTH = 8;

export function describeDiagnosticNode(node) {
  if (!node || typeof node !== "object") return "";
  const tag = String(node.tagName || node.nodeName || "").toLowerCase();
  const id = String(node.id || "");
  const ui = String(node.dataset?.ui || "");
  const classes = Array.from(node.classList || []).slice(0, 3).join(".");
  return `${tag}${id ? `#${id}` : ""}${ui ? `[data-ui=${ui}]` : ""}${classes ? `.${classes}` : ""}`;
}

export function describeEventPath(event, limit = MAX_PATH_DEPTH) {
  const path = typeof event?.composedPath === "function" ? event.composedPath() : [];
  const nodes = path.length ? path : [event?.target, event?.currentTarget];
  const depth = Math.max(1, Number(limit) || MAX_PATH_DEPTH);
  return nodes.filter(Boolean).slice(0, depth).map(describeDiagnosticNode);
}

function createBootId() {
  try {
    return `b${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
  } catch {
    return "boot";
  }
}

function targetsToggle(target) {
  if (!target) return false;
  if (String(target.id || "") === TOGGLE_ID) return true;
  if (typeof target.closest !== "function") return false;
  return Boolean(target.closest(`#${TOGGLE_ID}`));
}

export function createDesktopUpdateClickDiagnostics({
  documentObject = null,
  refs = {},
  readControllerState = () => ({}),
  emit = () => {},
  bootId = createBootId(),
  schedule = callback => setTimeout(callback, 0),
  now = () => (typeof performance !== "undefined" && typeof performance.now === "function" ? performance.now() : Date.now())
} = {}) {
  const pending = [];
  let installed = false;
  let sequence = 0;
  let divergenceReported = false;

  function elapsedMs() {
    const value = Number(now());
    return Number.isFinite(value) ? Math.max(0, Math.round(value)) : 0;
  }

  function emitDiagnostic(event, payload) {
    try {
      emit(event, { bootId, elapsedMs: elapsedMs(), ...payload });
    } catch {
      // Diagnostics must never break the update UI.
    }
  }

  function toggleSnapshot() {
    const toggle = refs.desktopUpdateToggleBtn || null;
    const panel = refs.desktopUpdatePanel || null;
    return {
      togglePresent: Boolean(toggle),
      toggleConnected: toggle ? toggle.isConnected !== false : false,
      toggleAriaExpanded: String(toggle?.getAttribute?.("aria-expanded") || ""),
      toggleStateToken: String(toggle?.dataset?.updateState || ""),
      panelPresent: Boolean(panel),
      panelHidden: panel ? Boolean(panel.classList?.contains?.("hidden")) : null,
      panelStateToken: String(panel?.dataset?.updateState || "")
    };
  }

  function controllerSnapshot() {
    try {
      const state = readControllerState();
      return state && typeof state === "object" ? { ...state } : {};
    } catch {
      return {};
    }
  }

  function handleClick(event) {
    if (!targetsToggle(event?.target)) return;
    sequence += 1;
    const record = {
      clickId: `${bootId}-${sequence}`,
      elapsedMs: elapsedMs(),
      eventTimestampMs: Math.round(Number(event?.timeStamp) || 0),
      detail: Number(event?.detail || 0),
      isTrusted: Boolean(event?.isTrusted),
      defaultPrevented: Boolean(event?.defaultPrevented),
      targetMatchesBoundToggle: event?.target === refs.desktopUpdateToggleBtn,
      path: describeEventPath(event),
      controller: controllerSnapshot(),
      ...toggleSnapshot()
    };
    emitDiagnostic("jobs_desktop_update_click_dispatched", { clickId: record.clickId, ...record });
    pending.push(record);
    schedule(() => {
      const index = pending.findIndex(entry => entry.clickId === record.clickId);
      if (index < 0) return;
      pending.splice(index, 1);
      emitDiagnostic("jobs_desktop_update_click_dropped", {
        ...record,
        // Both snapshots are kept: the dispatch-time bind state is what proves a
        // click arrived before `bind()`, while the drop-time state shows whether
        // the controller caught up in the meantime.
        controllerAtDispatch: record.controller,
        panelOpenInController: Boolean(controllerSnapshot().panelOpen),
        controller: controllerSnapshot(),
        ...toggleSnapshot()
      });
    });
  }

  function install() {
    if (installed || typeof documentObject?.addEventListener !== "function") return false;
    installed = true;
    // Capture phase, so the record does not depend on anything the controller
    // (or a stopped-propagation handler) does later in the dispatch.
    documentObject.addEventListener("click", handleClick, true);
    emitDiagnostic("jobs_desktop_update_click_diagnostics_installed", {
      controller: controllerSnapshot(),
      ...toggleSnapshot()
    });
    return true;
  }

  function claimClick() {
    const record = pending.shift() || null;
    return record
      ? {
        clickId: record.clickId,
        hadDispatchedRecord: true,
        dispatchedElapsedMs: record.elapsedMs,
        controllerAtDispatch: record.controller
      }
      : { clickId: "", hadDispatchedRecord: false, dispatchedElapsedMs: null, controllerAtDispatch: {} };
  }

  function recordHandled(claim = {}, { action = "", panelVisible = null } = {}) {
    emitDiagnostic("jobs_desktop_update_click_handled", {
      clickId: String(claim.clickId || ""),
      hadDispatchedRecord: Boolean(claim.hadDispatchedRecord),
      controllerAtDispatch: claim.controllerAtDispatch || {},
      action: String(action || ""),
      panelVisible: panelVisible === null ? null : Boolean(panelVisible),
      controller: controllerSnapshot(),
      ...toggleSnapshot()
    });
  }

  // `state.panelOpen` true while the panel is hidden is the "the click did
  // nothing" signature: the handler ran and the next render refused to expose
  // the panel. Reported once per divergence so a re-render cannot flood.
  function reportPanelDiverged(reason) {
    if (divergenceReported) return;
    divergenceReported = true;
    emitDiagnostic("jobs_desktop_update_panel_state_diverged", {
      reason: String(reason || ""),
      controller: controllerSnapshot(),
      ...toggleSnapshot()
    });
  }

  function clearPanelDiverged() {
    divergenceReported = false;
  }

  return {
    install,
    claimClick,
    recordHandled,
    reportPanelDiverged,
    clearPanelDiverged,
    isInstalled: () => installed
  };
}
