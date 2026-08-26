import { createVisibilityPausedInterval } from "../../shared/visibility-poll.js";

const STALE_FETCH_HOURS = 12;
const DISMISS_TTL_HOURS = 4;
const POLL_INTERVAL_MS = 30000;
const INITIAL_FULL_POLL_DELAY_MS = POLL_INTERVAL_MS;
const DISMISS_KEY_PREFIX = "baluffo_action_dismissed_";
const MAX_ITEMS = 3;
const CHECKING_SUMMARY = "Checking operational signals...";
const PARTIAL_SUMMARY = "No immediate action from core signals. Storage check pending.";
const ACTIVE_WORK_SUMMARY = "Operational checks delayed while job update is running.";
const UNAVAILABLE_SUMMARY = "Operational signals unavailable. Retry or copy diagnostics for details.";

const SIGNAL_ORDER = [
  "storage_health",
  "stale_fetch",
  "sync_status",
  "failed_sources"
];

const SIGNAL_LABELS = {
  storage_health: "Storage health issue detected",
  stale_fetch: "Jobs fetch is stale",
  sync_status: "Sync needs attention",
  failed_sources: "Some sources failed in last fetch"
};

const SIGNAL_ICONS = {
  storage_health: "\u26A0",
  stale_fetch: "\u23F0",
  sync_status: "\u2194\uFE0F",
  failed_sources: "\u274C"
};

function formatAge(hours) {
  if (hours < 1) return `${Math.round(hours * 60)}m ago`;
  if (hours < 24) return `${Math.round(hours)}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function nowIso() {
  return new Date().toISOString();
}

function hasPayload(value) {
  return Boolean(value && typeof value === "object");
}

function isoMs(value) {
  const ts = Date.parse(String(value || ""));
  return Number.isFinite(ts) ? ts : 0;
}

function isDismissed(signalId, nowMs) {
  try {
    const raw = localStorage.getItem(DISMISS_KEY_PREFIX + signalId);
    if (!raw) return false;
    const dismissedAt = isoMs(raw);
    if (!dismissedAt) return false;
    const ttlMs = DISMISS_TTL_HOURS * 60 * 60 * 1000;
    if (nowMs - dismissedAt > ttlMs) {
      localStorage.removeItem(DISMISS_KEY_PREFIX + signalId);
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

function dismissSignal(signalId) {
  try {
    localStorage.setItem(DISMISS_KEY_PREFIX + signalId, nowIso());
  } catch {
    // localStorage unavailable — silently degrade
  }
}

function clearAllDismissed(nowMs) {
  try {
    const keys = [];
    for (let i = 0; i < localStorage.length; i++) {
      const key = localStorage.key(i);
      if (key && key.startsWith(DISMISS_KEY_PREFIX)) keys.push(key);
    }
    keys.forEach(key => {
      const dismissedAt = isoMs(localStorage.getItem(key));
      const ttlMs = DISMISS_TTL_HOURS * 60 * 60 * 1000;
      if (!dismissedAt || nowMs - dismissedAt > ttlMs) {
        localStorage.removeItem(key);
      }
    });
  } catch {
    // localStorage unavailable
  }
}

function parseAge(value) {
  if (value === null || value === undefined) return Infinity;
  const text = String(value).trim();
  if (!text) return Infinity;
  const num = parseFloat(text);
  if (Number.isNaN(num)) return Infinity;
  if (/d/i.test(text)) return num * 24;
  if (/h/i.test(text)) return num;
  if (/m/i.test(text)) return num / 60;
  return num;
}

function evaluateStorageHealth(storageData) {
  if (!storageData) return null;
  const storage = storageData.storage;
  if (!storage) return null;

  const healthy = storage.healthy !== false;
  if (healthy) {
    const diagnostics = Array.isArray(storage.diagnostics) ? storage.diagnostics : [];
    if (!diagnostics.some(d => d && d.ok === false)) return null;
  }

  const diagFailures = (Array.isArray(storage.diagnostics) ? storage.diagnostics : [])
    .filter(d => d && d.ok === false).length;

  return {
    id: "storage_health",
    severity: "critical",
    summary: healthy
      ? `${diagFailures} storage diagnostic${diagFailures !== 1 ? "s" : ""} reported errors`
      : "Storage is unhealthy",
    actions: ["review", "copy_diagnostics", "dismiss"]
  };
}

function evaluateStaleFetch(healthData) {
  if (!healthData) return null;
  const alerts = Array.isArray(healthData.alerts) ? healthData.alerts : [];
  const fetchNeverRun = alerts.some(a => a && a.id === "fetch_never_run");
  const staleFetchAlert = alerts.find(a => a && a.id === "stale_fetch");

  const kpis = healthData.kpis || {};
  const ageHours = parseAge(kpis.lastSuccessfulFetchAge);
  const isStale = ageHours > STALE_FETCH_HOURS;

  if (fetchNeverRun) {
    return {
      id: "stale_fetch",
      severity: "critical",
      summary: "Jobs fetch has never run",
      actions: ["review", "retry_fetch", "dismiss"]
    };
  }
  if (isStale && staleFetchAlert) {
    return {
      id: "stale_fetch",
      severity: "warning",
      summary: `Last successful fetch was ${formatAge(ageHours)}`,
      actions: ["review", "retry_fetch", "dismiss"]
    };
  }
  return null;
}

function evaluateSyncStatus(syncData) {
  if (!syncData) return null;
  const runtime = syncData.runtime || {};
  const config = syncData.config || {};

  const lastError = String(runtime.lastError || "").trim();
  const lastResult = String(runtime.lastResult || "").trim();
  const lastAction = String(runtime.lastAction || "").trim();
  const enabled = config.enabled === true;
  const ready = config.ready !== false;

  if (!enabled) return null;

  if (!ready) {
    return {
      id: "sync_status",
      severity: "warning",
      summary: "Sync is enabled but not configured",
      actions: ["review", "dismiss"]
    };
  }

  if (lastResult === "error" && lastError) {
    if (String(config?.state || "").trim() === "remote_conflict" || /is at .* but expected|sha does not match|remote write conflict|not a fast-forward/i.test(lastError)) {
      return {
        id: "sync_status",
        severity: "warning",
        summary: "Sync conflict needs review; data refresh can continue",
        actions: ["review", "retry_sync", "dismiss"]
      };
    }
    return {
      id: "sync_status",
      severity: "warning",
      summary: `Sync ${lastAction || "operation"} failed`,
      actions: ["review", "retry_sync", "dismiss"]
    };
  }

  if (lastError && lastResult !== "error") {
    return {
      id: "sync_status",
      severity: "warning",
      summary: `Sync has a recorded error`,
      actions: ["review", "dismiss"]
    };
  }

  return null;
}

function evaluateFailedSources(healthData) {
  if (!healthData) return null;
  const kpis = healthData.kpis || {};
  const ageHours = parseAge(kpis.lastSuccessfulFetchAge);
  if (ageHours > STALE_FETCH_HOURS) return null;

  const failedRatio = Number(kpis.failedSourceRatioLatest || 0);
  if (failedRatio <= 0) return null;

  const pct = Math.round(failedRatio * 100);
  return {
    id: "failed_sources",
    severity: "warning",
    summary: `${pct}% of sources failed in the last fetch`,
    actions: ["review", "retry_failed", "dismiss"]
  };
}

export function createActionCenterController({
  refs,
  getBridge,
  postBridge,
  showToast,
  logAdminError,
  onSyncStatus,
  shouldDeferCoreSignals = () => false,
  shouldDeferStorageHealth = () => false
}) {
  let pollTimer = null;
  let fullPollTimer = null;
  const pollCache = { health: null, sync: null, storage: null };

  function orderedSignals(signalsMap) {
    const critical = [];
    const warning = [];
    for (const id of SIGNAL_ORDER) {
      const signal = signalsMap[id];
      if (!signal) continue;
      if (signal.severity === "critical") critical.push(signal);
      else warning.push(signal);
    }
    return [...critical, ...warning].slice(0, MAX_ITEMS);
  }

  function evaluateAll(healthData, syncData, storageData) {
    const map = {};
    const storage = evaluateStorageHealth(storageData);
    if (storage) map.storage_health = storage;
    const stale = evaluateStaleFetch(healthData);
    if (stale) map.stale_fetch = stale;
    const sync = evaluateSyncStatus(syncData);
    if (sync) map.sync_status = sync;
    const failed = evaluateFailedSources(healthData);
    if (failed) map.failed_sources = failed;
    return map;
  }

  function renderItemHtml(signal) {
    const icon = SIGNAL_ICONS[signal.id] || "\u26A0";
    const label = SIGNAL_LABELS[signal.id] || signal.id;
    let actionsHtml = "";
    const actions = Array.isArray(signal.actions) ? signal.actions : [];

    for (const action of actions) {
      if (action === "review") {
        actionsHtml += `<button class="btn clear-filters-btn" data-action="review" data-signal="${signal.id}">\u25B6 Review</button>`;
      } else if (action === "retry_fetch") {
        actionsHtml += `<button class="btn clear-filters-btn" data-action="retry" data-signal="${signal.id}" data-preset="default">\uD83D\uDD04 Run Jobs Fetcher</button>`;
      } else if (action === "retry_failed") {
        actionsHtml += `<button class="btn clear-filters-btn" data-action="retry" data-signal="${signal.id}" data-preset="retry_failed">\uD83D\uDD04 Retry failed</button>`;
      } else if (action === "retry_sync") {
        actionsHtml += `<button class="btn clear-filters-btn" data-action="retry" data-signal="${signal.id}" data-preset="sync_pull">\uD83D\uDD04 Retry sync</button>`;
      } else if (action === "copy_diagnostics") {
        actionsHtml += `<button class="btn clear-filters-btn" data-action="copy-diagnostics" data-signal="${signal.id}">\uD83D\uDCCB Copy diagnostics</button>`;
      } else if (action === "dismiss") {
        actionsHtml += `<button class="btn clear-filters-btn" data-action="dismiss" data-signal="${signal.id}">\u2715 Dismiss</button>`;
      }
    }

    const severityClass = signal.severity === "critical" ? "action-center-item-critical" : "action-center-item-warning";
    return `<div class="action-center-item ${severityClass}" data-signal="${signal.id}">
      <div class="action-center-item-body">
        <span class="action-center-item-icon">${icon}</span>
        <div class="action-center-item-content">
          <span class="action-center-item-summary">${label} &mdash; ${signal.summary}</span>
        </div>
      </div>
      <div class="action-center-item-actions">${actionsHtml}</div>
    </div>`;
  }

  function renderStatusState({
    className,
    state,
    icon,
    summary
  }) {
    return `<div class="action-center-item ${className}" data-state="${state}">
      <div class="action-center-item-body">
        <span class="action-center-item-icon">${icon}</span>
        <span class="action-center-item-summary">${summary}</span>
      </div>
    </div>`;
  }

  function renderCheckingState() {
    return renderStatusState({
      className: "action-center-item-neutral",
      state: "checking",
      icon: "i",
      summary: CHECKING_SUMMARY
    });
  }

  function renderPartialState() {
    return renderStatusState({
      className: "action-center-item-neutral",
      state: "partial",
      icon: "i",
      summary: PARTIAL_SUMMARY
    });
  }

  function renderActiveWorkState() {
    return renderStatusState({
      className: "action-center-item-neutral",
      state: "active-work-delayed",
      icon: "i",
      summary: ACTIVE_WORK_SUMMARY
    });
  }

  function renderUnavailableState() {
    return renderStatusState({
      className: "action-center-item-warning",
      state: "unavailable",
      icon: "\u26A0",
      summary: UNAVAILABLE_SUMMARY
    });
  }

  function renderHealthyState() {
    return renderStatusState({
      className: "action-center-item-ok",
      state: "healthy",
      icon: "\u2713",
      summary: "All systems operational"
    });
  }

  function renderSignals(signals, pollMeta = {}) {
    const itemsContainer = refs.actionCenterItemsEl;
    if (!itemsContainer) return;

    const signalList = orderedSignals(signals);
    const nowMs = Date.now();
    clearAllDismissed(nowMs);

    const visible = signalList.filter(s => !isDismissed(s.id, nowMs));
    const hasMore = signalList.length > MAX_ITEMS;

    let html = "";
    if (visible.length === 0) {
      if (pollMeta.activeWorkDeferred) {
        html = renderActiveWorkState();
      } else if (pollMeta.allRequiredChecked) {
        html = renderHealthyState();
      } else if (pollMeta.coreChecked && pollMeta.storagePending) {
        html = renderPartialState();
      } else if (!pollMeta.anyChecked) {
        html = renderUnavailableState();
      } else {
        html = renderUnavailableState();
      }
    } else {
      for (const signal of visible) {
        html += renderItemHtml(signal);
      }
      if (hasMore) {
        html += `<div class="action-center-view-all">
          <button class="btn action-center-view-all-btn" data-action="view-all">View all \u2192 Ops Health</button>
        </div>`;
      }
    }
    itemsContainer.innerHTML = html;
  }

  async function handleAction(action, signalId, preset) {
    if (action === "retry") {
      try {
        let result;
        if (preset === "sync_pull") {
          result = await postBridge("/tasks/run-sync-pull", {});
        } else {
          result = await postBridge("/tasks/run-fetcher", { preset });
        }
        if (result?.alreadyRunning) {
          showToast("Fetch is already running", "info");
        } else if (result?.alreadyCompleted) {
          showToast("Task already completed", "info");
          await pollActionCenter();
        } else if (result?.started) {
          showToast("Task started", "success");
        } else {
          showToast("Task could not be started", "warn");
        }
      } catch (err) {
        showToast("Retry failed: " + (err?.message || "unknown error"), "error");
      }
    } else if (action === "dismiss") {
      dismissSignal(signalId);
      await pollActionCenter();
    } else if (action === "copy-diagnostics") {
      copySignalDiagnostics(signalId);
    } else if (action === "review" || action === "view-all") {
      const contentEl = document.querySelector("[data-ui=\"admin-content\"]");
      if (contentEl) {
        contentEl.scrollIntoView({ behavior: "smooth" });
      }
      const overviewBtn = document.querySelector("#admin-ops-tab-overview-btn");
      if (overviewBtn) {
        overviewBtn.click();
      }
    }
  }

  // ponytail: execCommand fallback for non-secure contexts (Umbrel LAN http),
  // where navigator.clipboard is unavailable; drop when the UI is https-only.
  async function copyTextToClipboard(text) {
    const clipboardApi = typeof navigator === "undefined" ? null : navigator.clipboard;
    const secureContext = typeof window !== "undefined" && Boolean(window.isSecureContext);
    if (clipboardApi && secureContext) {
      try {
        await clipboardApi.writeText(text);
        return true;
      } catch {
        /* fall through to legacy path */
      }
    }
    if (typeof document === "undefined") return false;
    try {
      const ta = document.createElement("textarea");
      ta.value = text;
      ta.setAttribute("readonly", "");
      ta.style.position = "fixed";
      ta.style.top = "0";
      ta.style.left = "0";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(ta);
      return ok;
    } catch {
      return false;
    }
  }

  async function copySignalDiagnostics(signalId) {
    let json;
    if (signalId === "storage_health") json = pollCache.storage;
    else if (signalId === "sync_status") json = pollCache.sync;
    else json = pollCache.health;
    const ok = await copyTextToClipboard(JSON.stringify(json || {}, null, 2));
    showToast(ok ? "Diagnostics copied" : "Could not copy diagnostics", ok ? "success" : "warn");
  }

  async function copyAllDiagnostics() {
    const payload = {
      _meta: {
        generatedAt: nowIso(),
        partial: !pollCache.health || !pollCache.sync || !pollCache.storage
      },
      health: pollCache.health || { error: "endpoint not available" },
      sync: pollCache.sync || { error: "endpoint not available" },
      storage: pollCache.storage || { error: "endpoint not available" }
    };
    const ok = await copyTextToClipboard(JSON.stringify(payload, null, 2));
    showToast(ok ? "All diagnostics copied" : "Could not copy diagnostics", ok ? "success" : "warn");
  }

  async function pollActionCenter(options = {}) {
    try {
      const deferCore = Boolean(shouldDeferCoreSignals());
      const includeStorage = options?.includeStorage !== false
        && !deferCore
        && !shouldDeferStorageHealth();
      const [health, sync, storage] = deferCore
        ? [pollCache.health || null, pollCache.sync || null, pollCache.storage || null]
        : await Promise.all([
          getBridge("/ops/health?view=ready", { timeoutMs: 5000 }).catch(() => null),
          getBridge("/sync/status?view=summary", { timeoutMs: 5000 }).catch(() => null),
          includeStorage
            ? getBridge("/ops/storage-health", { timeoutMs: 5000 }).catch(() => null)
            : Promise.resolve(pollCache.storage || null)
        ]);
      const storagePayload = includeStorage ? storage : pollCache.storage || null;
      if (!deferCore || hasPayload(health)) pollCache.health = health;
      if (!deferCore || hasPayload(sync)) pollCache.sync = sync;
      pollCache.storage = storagePayload;
      if (!deferCore && hasPayload(sync) && typeof onSyncStatus === "function") {
        onSyncStatus(sync);
      }
      const healthChecked = hasPayload(health);
      const syncChecked = hasPayload(sync);
      const storageChecked = hasPayload(storagePayload);
      const pollMeta = {
        anyChecked: healthChecked || syncChecked || storageChecked,
        coreChecked: healthChecked && syncChecked,
        storagePending: !storageChecked && includeStorage === false,
        activeWorkDeferred: deferCore,
        allRequiredChecked: healthChecked && syncChecked && storageChecked
      };
      const signals = evaluateAll(health, sync, storagePayload);
      renderSignals(signals, pollMeta);
    } catch (err) {
      if (logAdminError) logAdminError("action_center_poll", err);
      renderSignals({}, { anyChecked: false });
    }
  }

  function bindEvents(itemsEl) {
    if (!itemsEl) return;
    itemsEl.addEventListener("click", event => {
      const btn = event.target.closest("[data-action]");
      if (!btn) return;
      event.preventDefault();
      const action = btn.dataset.action;
      const signalId = btn.dataset.signal || "";
      const preset = btn.dataset.preset || "";
      handleAction(action, signalId, preset);
    });

    const copyAllBtn = refs.actionCenterCopyBtnEl;
    if (copyAllBtn) {
      copyAllBtn.addEventListener("click", event => {
        event.preventDefault();
        copyAllDiagnostics();
      });
    }
  }

  function startPolling(options = {}) {
    stopPolling();
    const itemsEl = refs.actionCenterItemsEl;
    if (itemsEl) itemsEl.innerHTML = renderCheckingState();
    const runInitialPoll = () => {
      pollActionCenter({ includeStorage: false }).then(() => {
        bindEvents(itemsEl);
      });
    };
    runInitialPoll();
    const fullPollDelayMs = Math.max(0, Number(options?.fullPollDelayMs) || INITIAL_FULL_POLL_DELAY_MS);
    fullPollTimer = setTimeout(() => {
      pollActionCenter({ includeStorage: true }).catch(() => {});
      fullPollTimer = null;
    }, fullPollDelayMs);
    pollTimer = createVisibilityPausedInterval(() => {
      pollActionCenter({ includeStorage: true }).catch(() => {});
    }, POLL_INTERVAL_MS);
  }

  function stopPolling() {
    if (fullPollTimer) {
      clearTimeout(fullPollTimer);
      fullPollTimer = null;
    }
    if (pollTimer) {
      pollTimer.stop();
      pollTimer = null;
    }
  }

  function dispose() {
    stopPolling();
  }

  return {
    startPolling,
    stopPolling,
    pollActionCenter,
    copyAllDiagnostics,
    dispose
  };
}
