import { formatBytes } from "../../shared/format-utils.js";
import { createVisibilityPausedInterval } from "../../shared/visibility-poll.js";
import { createFetchKpisSummaryFeed } from "./fetch-kpis-summary-feed.js";
import {
  renderActionCenterBody,
  renderCheckedAt,
  renderStatusChip
} from "../render/action-center.js";

const STALE_FETCH_HOURS = 12;
const DISMISS_TTL_HOURS = 4;
const POLL_INTERVAL_MS = 30000;
const DISMISS_KEY_PREFIX = "baluffo_action_dismissed_";
const MAX_ITEMS = 4;
// The Action Center reads three things off the health payload:
// `alerts` (for `fetch_never_run` / `stale_fetch`), `kpis`
// (`lastSuccessfulFetchAge`, `failedSourceRatioLatest`), and the run-level
// browser probe at `kpis.browserFallbackHealth`. It used to poll
// `/ops/health?view=ready`, which is built by `compute_ops_health_ready()`
// (src/bridge/ops_api_health.py) and contains *neither* key — that payload is a
// deliberate lightweight-startup shape, pinned by
// tests/bridge/test_lightweight_startup_routes.py. So `evaluateStaleFetch` and
// `evaluateFailedSources` could never fire: two of the four signals were dead.
//
// `/ops/fetch-kpis?view=summary` is the one cached route that carries both keys.
// The alternatives do not:
//   * `/ops/health?view=ready` — neither key (the original bug).
//   * `/ops/dashboard-health?view=summary` — `alerts` yes, but its three KPI keys
//     omit `lastSuccessfulFetchAge`, so the age reads as "unknown".
//   * `/ops/dashboard-health` (full) — carries both, but is *uncached*, and the
//     route module records 0.5-1.6s per uncached chain on idle low-end hardware.
//     A 30s poll of it would reintroduce the load the summary caches exist to
//     remove.
// fetch-kpis also stays consistent with the Ops tab, whose fetch-KPI hydration
// already polls this same path, and it is in the frontend in-flight dedupe set.
const HEALTH_ROUTE = "/ops/fetch-kpis?view=summary";
const CHECKING_SUMMARY = "Checking operational signals...";
const PARTIAL_SUMMARY = "No immediate action from core signals. Storage check pending.";
const ACTIVE_WORK_SUMMARY = "Operational checks delayed while job update is running.";
const UNAVAILABLE_SUMMARY = "Operational signals unavailable. Retry or copy diagnostics for details.";

const SIGNAL_ORDER = [
  "storage_health",
  "stale_fetch",
  "sync_status",
  "failed_sources",
  "browser_fallback_health"
];

const SIGNAL_LABELS = {
  storage_health: "Storage health issue detected",
  stale_fetch: "Jobs fetch is stale",
  sync_status: "Sync needs attention",
  failed_sources: "Source outcomes need review",
  browser_fallback_health: "Browser fallback health needs attention"
};

function formatAge(hours) {
  // `parseAge` returns Infinity for a missing or unparseable age. Without this
  // guard the summary renders the literal string "Last successful fetch was
  // Infinityd ago" — reachable whenever the polled health payload omits
  // `kpis.lastSuccessfulFetchAge` (e.g. the `?view=summary` variant, which
  // carries alerts but only three KPI keys).
  if (!Number.isFinite(hours)) return "at an unknown time";
  if (hours < 1) return `${Math.round(hours * 60)}m ago`;
  if (hours < 24) return `${Math.round(hours)}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

function formatFailureTimestamp(ms) {
  return `${new Date(ms).toISOString().slice(0, 19).replace("T", " ")} UTC`;
}

function formatElapsedAge(ms, nowMs = Date.now()) {
  const elapsedMs = Math.max(0, nowMs - ms);
  if (elapsedMs < 60_000) return `${Math.floor(elapsedMs / 1000)}s ago`;
  return formatAge(elapsedMs / 3_600_000);
}

function nowIso() {
  return new Date().toISOString();
}

function hasPayload(value) {
  return Boolean(value && typeof value === "object");
}

function normalizePollIntervalMs(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0
    ? Math.max(1, Math.round(parsed))
    : POLL_INTERVAL_MS;
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

// The store latches unhealthy with a code now, so the panel can name what happened
// instead of asserting an integrity failure for every unhealthy verdict. A deferred
// quick check (large database) is not a failure, and it used to render as one.
const STORAGE_REASON_LABELS = {
  write_lock_timeout: "Storage write lock timed out",
  busy_exhausted_write: "SQLite stayed busy during a write",
  busy_exhausted_read: "SQLite stayed busy during a read",
  busy_exhausted_vacuum: "SQLite stayed busy during a database rebuild",
  write_error: "SQLite write failed",
  vacuum_error: "Database rebuild failed",
  wal_checkpoint_error: "WAL checkpoint failed",
  wal_checkpoint_busy: "WAL checkpoint could not finish",
  integrity_check: "SQLite integrity check failed",
  startup_probe: "SQLite startup probe failed",
  pragma: "SQLite could not enable a required setting",
  reopen_failed: "The store could not reopen after a database swap"
};

function storageReason(storage) {
  const reason = storage?.unhealthyReason;
  return reason && typeof reason === "object" && !Array.isArray(reason) ? reason : {};
}

function storageReasonAt(storage) {
  const at = String(storageReason(storage).at || "").trim();
  return at || String(storage?.walMaintenance?.attemptedAt || "").trim();
}

function storageReasonMessage(storage) {
  const text = String(storageReason(storage).message || "").trim();
  return text.replace(/\s+/g, " ").slice(0, 240);
}

// Mirrors the store's own rule: a check that was skipped for size is not a result.
function quickCheckFailed(storage) {
  if (storage?.quickCheckDeferred === true || storage?.quickCheckDeferred === 1) return false;
  const check = String(storage?.quickCheck || "").trim().toLowerCase();
  if (!check || check === "ok" || check === "deferred-large-database") return false;
  return true;
}

function storageSizesCopy(walBytes, databaseBytes) {
  const sizes = [];
  if (Number.isFinite(walBytes) && walBytes > 0) sizes.push(`${formatBytes(walBytes)} WAL`);
  if (Number.isFinite(databaseBytes) && databaseBytes > 0) {
    sizes.push(`${formatBytes(databaseBytes)} database`);
  }
  return sizes.join(" on ");
}

// One reading of the live files and the checkpoint record, shared by the signal
// gate and its copy. W1: the record is a condition and only a live measurement ends
// it. A null, blank or unreadable size is not evidence the WAL shrank, so it keeps
// the warning: an alert fails toward speaking, never toward silence.
function walGateState(storage) {
  const wal = storage?.walMaintenance || {};
  const readSize = value => {
    if (value === null || value === undefined) return null;
    if (typeof value === "string" && value.trim() === "") return null;
    const size = Number(value);
    return Number.isFinite(size) && size >= 0 ? size : null;
  };
  const liveWal = readSize(storage?.walBytes);
  const liveDatabase = readSize(storage?.databaseBytes);
  const threshold = readSize(wal.checkpointThresholdBytes);
  const status = String(wal.status || "").trim();
  return {
    status,
    liveWal,
    liveDatabase,
    known: liveWal !== null || liveDatabase !== null,
    outstanding: (status === "busy" || status === "failed")
      && (liveWal === null || threshold === null || threshold <= 0 || liveWal >= threshold)
  };
}

// W3: the record keeps the sizes observed before its attempt, so quoting them as
// current made a resolved checkpoint read as a 1 GB WAL beside a 520 MB database.
// The live files are the actionable numbers; the attempt's are history worth naming
// only when they disagree.
function walCheckpointCopy(storage) {
  const wal = storage?.walMaintenance || {};
  const { known: liveKnown, liveWal, liveDatabase } = walGateState(storage);
  const attemptWal = Number(wal.walBytes ?? 0);
  const attemptDatabase = Number(wal.databaseBytes ?? 0);
  const current = storageSizesCopy(liveWal, liveDatabase);
  const attempt = storageSizesCopy(attemptWal, attemptDatabase);
  let sizes = attempt;
  if (liveKnown) {
    sizes = current;
    if (attempt && (attemptWal !== liveWal || attemptDatabase !== liveDatabase)) {
      sizes = current ? `${current}; the attempt saw ${attempt}` : `the attempt saw ${attempt}`;
    }
  }
  const failed = String(wal.status || "").trim() === "failed";
  const headline = failed ? "WAL checkpoint failed" : "WAL checkpoint could not finish";
  return sizes ? `${headline} — ${sizes}` : headline;
}

function attemptAgeNote(storage) {
  const seconds = Number(storage?.walMaintenance?.attemptAgeS);
  if (!Number.isFinite(seconds) || seconds <= 0) return "";
  // `formatAge` rounds to whole minutes, which would render a 20 s old attempt as
  // "0m ago". A fresh record must never read as no time at all.
  if (seconds < 60) return " (just now)";
  return ` (${formatAge(seconds / 3600)})`;
}

function evaluateStorageHealth(storageData) {
  if (!storageData) return null;
  const storage = storageData.storage;
  if (!storage) return null;

  const diagnostics = Array.isArray(storage.diagnostics) ? storage.diagnostics : [];
  const diagFailures = diagnostics.filter(d => d && d.ok === false).length;
  const unhealthy = storage.healthy === false;
  // S1.2 makes a busy checkpoint a maintenance state, so the store reports
  // healthy: true while its WAL keeps growing. The warning must follow the
  // maintenance record, or the original 1 GB-WAL symptom goes silent.
  // W1: the record is a condition, not a verdict. It is rewritten only by a later
  // attempt, so a WAL emptied by a VACUUM, a rebuild or a bulk-delete checkpoint
  // left it reading `busy` forever. The live file decides whether it is still true;
  // payloads without a live size or threshold cannot prove it ended, so they keep
  // the record-based warning.
  const { status: walStatus, outstanding: walOutstanding } = walGateState(storage);
  if (!unhealthy && diagFailures === 0 && !walOutstanding) return null;

  const actions = ["review", "copy_diagnostics", "dismiss"];
  const at = storageReasonAt(storage);
  const failureMs = isoMs(at);
  const ageNote = attemptAgeNote(storage);
  const when = failureMs ? ` on ${formatFailureTimestamp(failureMs)}${ageNote}` : "";
  const deferredNote = storage.quickCheckDeferred === true
    ? "; integrity check deferred at this database size"
    : "";
  if (!unhealthy) {
    if (diagFailures === 0 && walOutstanding) {
      const dateNote = when || "; attempt date not recorded";
      return {
        id: "storage_health",
        severity: "warning",
        summary: `${walCheckpointCopy(storage)}${dateNote}${deferredNote}`,
        actions
      };
    }
    return {
      id: "storage_health",
      severity: "critical",
      // Specific rather than "Storage is unhealthy", which only restated the label.
      summary: `${diagFailures} storage diagnostic${diagFailures !== 1 ? "s" : ""} reported errors`,
      actions
    };
  }

  const code = String(storageReason(storage).code || "").trim();
  const reasonMessage = storageReasonMessage(storage);

  if (quickCheckFailed(storage)) {
    const detail = String(storage.quickCheck || "").trim().slice(0, 160);
    return {
      id: "storage_health",
      severity: "critical",
      summary: `SQLite integrity check failed${detail ? `: ${detail}` : ""}${when}`,
      actions
    };
  }

  // A busy checkpoint leaves the WAL intact and retryable. Payloads from before the
  // reason code existed (0.3.062) carry only `walMaintenance.status: "failed"`, so
  // the failed checkpoint record is what names their cause.
  if (code === "wal_checkpoint_busy" || code === "wal_checkpoint_error"
    || (!code && walStatus === "failed")) {
    const dateNote = when || "; attempt date not recorded";
    return {
      id: "storage_health",
      severity: "warning",
      summary: `${walCheckpointCopy(storage)}${reasonMessage ? `: ${reasonMessage}` : ""}${dateNote}${deferredNote}`,
      actions
    };
  }

  const label = STORAGE_REASON_LABELS[code] || "Storage reported unhealthy";
  return {
    id: "storage_health",
    severity: "critical",
    summary: `${label}${reasonMessage ? `: ${reasonMessage}` : ""}${when}${deferredNote}`,
    actions
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

function formatCount(value) {
  const count = Number(value || 0);
  return Number.isFinite(count) ? count.toLocaleString("en-US") : "0";
}

// S2.4: refused demand (breaker in cooldown) and a crashed browser both count as an
// attempted-but-failed fallback, so the row must name the split, not just the total.
function browserFallbackDemandCopy(demand) {
  if (!demand || typeof demand !== "object" || Array.isArray(demand)) return "";
  const attempts = Number(demand.attempts || 0);
  if (!(attempts > 0)) return "";
  const refused = Number(demand.refused || 0);
  // Older sidecars lack `pages`; derive the calls that reached the browser.
  const pages = demand.pages == null ? Math.max(0, attempts - refused) : Number(demand.pages || 0);
  const emptyEnvironment = Number(demand.servedEmptyEnvironment || 0);
  const emptyPage = Number(demand.servedEmptyPage || 0);
  return (
    `${formatCount(attempts)} browser fallback call${attempts === 1 ? "" : "s"} ` +
    `(${formatCount(pages)} page attempt${pages === 1 ? "" : "s"}): ` +
    `${formatCount(refused)} refused in cooldown / ` +
    `${formatCount(emptyEnvironment)} empty-environment / ` +
    `${formatCount(emptyPage)} empty-page`
  );
}

function evaluateFailedSources(healthData) {
  if (!healthData) return null;
  const kpis = healthData.kpis || {};
  const ageHours = parseAge(kpis.lastSuccessfulFetchAge);
  if (ageHours > STALE_FETCH_HOURS) return null;

  const failedRatio = Number(kpis.failedSourceRatioLatest || 0);

  // The ratio alone conflates a transport failure with a board that fetched fine
  // and kept nothing. The bounded sourceOutcomeSummary comes from the report
  // sidecar, so this distinction costs no full-report read on the poll path.
  const outcome = kpis.sourceOutcomeSummary || {};
  const failedSources = Number(outcome.failedSources || 0);
  const zeroReviewFallback = Math.max(
    0,
    Number(outcome.zeroKeptNeedsReviewSources || 0) - failedSources
  );
  const zeroReview = Number.isFinite(Number(outcome.zeroKeptNeedsReviewNonFailedSources))
    ? Number(outcome.zeroKeptNeedsReviewNonFailedSources)
    : zeroReviewFallback;
  if (failedRatio <= 0 && failedSources <= 0 && zeroReview <= 0) return null;

  const browserFallback = Number(outcome.browserFallbackRecommendedSources || 0);
  const browserAttempted = Number(outcome.browserFallbackAttemptedSources || 0);
  const details = [];
  if (failedSources > 0 && zeroReview > 0) {
    details.push(`${formatCount(zeroReview)} other zero-kept sources need review`);
  }
  if (browserFallback > 0) {
    const attempted = browserAttempted > 0 ? ` (${formatCount(browserAttempted)} attempted)` : "";
    details.push(`${formatCount(browserFallback)} need browser fallback${attempted}`);
  }
  const demandCopy = browserFallbackDemandCopy(kpis.browserFallbackDemand);
  if (demandCopy) details.push(demandCopy);
  const breakdown = details.length ? `; ${details.join("; ")}` : "";

  const pct = Math.round(failedRatio * 100);
  const failedCopy = failedSources > 0
    ? `${formatCount(failedSources)} source${failedSources === 1 ? "" : "s"} failed in the last fetch`
    : zeroReview > 0
      ? `${formatCount(zeroReview)} zero-kept source${zeroReview === 1 ? "" : "s"} need review`
      : `${pct}% of sources failed in the last fetch`;
  return {
    id: "failed_sources",
    severity: "warning",
    summary: `${failedCopy}${breakdown}`,
    actions: failedSources > 0
      ? ["review", "retry_failed", "dismiss"]
      : ["review", "dismiss"]
  };
}

function evaluateBrowserFallbackHealth(healthData) {
  if (!healthData) return null;
  const kpis = healthData.kpis || {};
  const rawHealth = kpis.browserFallbackHealth;
  const health = rawHealth && typeof rawHealth === "object" && !Array.isArray(rawHealth)
    ? rawHealth
    : {};
  const failed = rawHealth === false || health.ok === false;
  const rawBlocked = kpis.browserFallbackEnvironmentBlockedSources
    ?? kpis.sourceOutcomeSummary?.browserFallbackEnvironmentBlockedSources;
  const blocked = Math.max(0, Number(rawBlocked));
  const environmentBlocked = kpis.browserFallbackEnvironmentBlocked === true
    || (Number.isFinite(blocked) && blocked > 0);
  if (!failed && !environmentBlocked) return null;

  const stage = String(health.stage || "").trim() || (environmentBlocked ? "preflight" : "unknown");
  const errorType = String(health.errorType || "").trim();
  const error = [health.error, health.lastLaunchError, health.lastNavigationError]
    .map(value => String(value || "").trim().replace(/\s+/g, " "))
    .find(Boolean) || "";
  const clipped = error.length > 240 ? `${error.slice(0, 237)}...` : error;
  const failureMs = isoMs(health.lastFailureAt);
  const failureCopy = failureMs
    ? `; last failure ${formatFailureTimestamp(failureMs)} (${formatElapsedAge(failureMs)})`
    : "";
  const rawNeeded = kpis.sourceOutcomeSummary?.browserFallbackRecommendedSources;
  const needed = Math.max(0, Number(rawNeeded));
  const neededCopy = rawNeeded != null && Number.isFinite(needed)
    ? `; ${formatCount(needed)} source${needed === 1 ? " needs" : "s need"} browser fallback`
    : "";
  const blockedCopy = Number.isFinite(blocked) && blocked > 0
    ? `; ${formatCount(blocked)} source${blocked === 1 ? "" : "s"} environment-blocked`
    : environmentBlocked
      ? "; browser environment blocked"
      : "";
  const demandCopy = browserFallbackDemandCopy(kpis.browserFallbackDemand);
  const headline = environmentBlocked
    ? `Browser environment blocked at ${stage}`
    : `Probe failed at ${stage}`;
  return {
    id: "browser_fallback_health",
    severity: "warning",
    summary: `${headline}${errorType ? ` (${errorType})` : ""}${clipped ? `: ${clipped}` : ""}${neededCopy}${blockedCopy}${failureCopy}${demandCopy ? `; ${demandCopy}` : ""}`,
    actions: ["review", "copy_diagnostics", "dismiss"]
  };
}

export function createActionCenterController({
  refs,
  getBridge,
  postBridge,
  showToast,
  logAdminError,
  onSyncStatus,
  onStorageHealth,
  shouldDeferCoreSignals = () => false,
  shouldDeferStorageHealth = () => false,
  // HEALTH_ROUTE is a startup-heavy route (see STARTUP_HEAVY_ROUTES in
  // tests/frontend/unit/admin-schedule-partial-hydration-smoke.test.mjs): the bridge has a
  // single gateway, so heavy GETs must not overlap at boot. The composition root injects the
  // serial startup lane here; without it (unit fixtures) the first poll stays immediate.
  enqueueStartupTask = null,
  fetchKpisSummaryFeed = null,
  pollIntervalMs = POLL_INTERVAL_MS
}) {
  let pollTimer = null;
  let fullPollTimer = null;
  let lastCheckedAtMs = 0;
  const pollCache = { health: null, sync: null, storage: null };
  const healthFeed = fetchKpisSummaryFeed || createFetchKpisSummaryFeed();
  let hasCompletedPoll = false;
  let lastPollMeta = null;
  let pollInFlight = false;
  let pollHealthPayload = null;
  let pollSequence = 0;

  // Critical first, then warnings, in SIGNAL_ORDER. Deliberately *not* capped here:
  // the cap is a display concern, and applying it before the `hasMore` comparison
  // made the "View all" row unreachable.
  function orderedSignalsAll(signalsMap) {
    const critical = [];
    const warning = [];
    for (const id of SIGNAL_ORDER) {
      const signal = signalsMap[id];
      if (!signal) continue;
      if (signal.severity === "critical") critical.push(signal);
      else warning.push(signal);
    }
    return [...critical, ...warning];
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
    const browserHealth = evaluateBrowserFallbackHealth(healthData);
    if (browserHealth) map.browser_fallback_health = browserHealth;
    return map;
  }

  // The label is domain copy, so it is attached here rather than in the renderer.
  function withLabel(signal) {
    return { ...signal, label: SIGNAL_LABELS[signal.id] || signal.id };
  }

  // The five states used to render through one function, so a passing check was
  // pixel-identical to a failed one. The renderer now owns state presentation and
  // gives each its own tone, icon and chip.
  function resolveStatusState(pollMeta = {}, signals = []) {
    if (pollMeta.activeWorkDeferred) {
      return { state: "active-work-delayed", summary: ACTIVE_WORK_SUMMARY };
    }
    // A poll can complete every check and still have a visible warning. The signal
    // rows replace the status line, but the state must not claim "healthy" here:
    // the browser probe is one of those warnings.
    if (signals.length > 0) {
      return { state: "warning", summary: "" };
    }
    if (pollMeta.allRequiredChecked) {
      return { state: "healthy", summary: "" };
    }
    if (pollMeta.coreChecked && pollMeta.storagePending) {
      return { state: "partial", summary: PARTIAL_SUMMARY };
    }
    return { state: "unavailable", summary: UNAVAILABLE_SUMMARY };
  }

  function renderSignals(signals, pollMeta = {}) {
    const itemsContainer = refs.actionCenterItemsEl;
    if (!itemsContainer) return;

    const allSignals = orderedSignalsAll(signals);
    const nowMs = Date.now();
    clearAllDismissed(nowMs);

    // The cap never binds today: `stale_fetch` (age > 12h) and `failed_sources`
    // (age <= 12h) are mutually exclusive, so at most four signals can be active:
    // storage_health, one of the fetch signals, sync_status, and
    // browser_fallback_health. MAX_ITEMS is 4. It is kept as a display contract so
    // the panel cannot grow unbounded if the signal set ever expands. The "View all"
    // overflow row that used to sit behind it was unreachable by construction and
    // has been removed — if a sixth signal is added, that row should come back.
    const visible = allSignals.filter(s => !isDismissed(s.id, nowMs)).slice(0, MAX_ITEMS);

    const status = resolveStatusState(pollMeta, visible);
    itemsContainer.innerHTML = renderActionCenterBody({
      state: status.state,
      summary: status.summary,
      signals: visible.map(withLabel)
    });

    if (refs.actionCenterStatusChipEl) {
      refs.actionCenterStatusChipEl.innerHTML = renderStatusChip({
        state: status.state,
        signals: visible
      });
    }
    if (refs.actionCenterCheckedAtEl) {
      // Stamped only when a poll actually returned data, so a deferred poll leaves
      // the previous timestamp in place and the stamp ages honestly rather than
      // resetting to "just now" while showing stale signals.
      if (pollMeta.anyChecked) lastCheckedAtMs = nowMs;
      refs.actionCenterCheckedAtEl.innerHTML = lastCheckedAtMs
        ? renderCheckedAt(lastCheckedAtMs, nowMs)
        : "";
    }
  }

  const unsubscribeHealthFeed = healthFeed.subscribe(payload => {
    if (!hasPayload(payload)) return;
    pollCache.health = payload;
    if (pollInFlight) {
      // The current poll is still waiting on sync/storage; let it render this
      // newer health payload instead of painting twice.
      pollHealthPayload = payload;
      return;
    }
    if (!hasCompletedPoll || shouldDeferCoreSignals()) return;
    const pollMeta = {
      ...(lastPollMeta || {}),
      anyChecked: true,
      activeWorkDeferred: false
    };
    lastPollMeta = pollMeta;
    renderSignals(evaluateAll(payload, pollCache.sync, pollCache.storage), pollMeta);
  });

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
    } else if (action === "review") {
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

  // ponytail: execCommand fallback for non-secure contexts (plain-http LAN),
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
    pollInFlight = true;
    pollHealthPayload = null;
    try {
      const deferCore = Boolean(shouldDeferCoreSignals());
      const includeStorage = options?.includeStorage !== false
        && !deferCore
        && !shouldDeferStorageHealth();
      const [health, sync, storage] = deferCore
        ? [pollCache.health || null, pollCache.sync || null, pollCache.storage || null]
        : await Promise.all([
          healthFeed.load(
            // The summary is a passive data read. A slow summary computation must
            // not claim the bridge is offline; `/app/ready` owns that state.
            () => getBridge(HEALTH_ROUTE, {
              timeoutMs: 5000,
              affectsReachability: false
            }).catch(() => null),
            { maxAgeMs: 5000 }
          ),
          getBridge("/sync/status?view=summary", { timeoutMs: 5000 }).catch(() => null),
          includeStorage
            // Diagnostic-only: a slow storage read must not report the bridge as
            // offline, because every data route in the same batch answered.
            ? getBridge("/ops/storage-health", { timeoutMs: 5000, affectsReachability: false })
              .catch(() => null)
            : Promise.resolve(pollCache.storage || null)
        ]);
      const storagePayload = includeStorage ? storage : pollCache.storage || null;
      if (pollHealthPayload) pollCache.health = pollHealthPayload;
      else if (!deferCore || hasPayload(health)) pollCache.health = health;
      if (!deferCore || hasPayload(sync)) pollCache.sync = sync;
      pollCache.storage = storagePayload;
      if (includeStorage && storagePayload && typeof onStorageHealth === "function") {
        onStorageHealth(storagePayload);
      }
      if (!deferCore && hasPayload(sync) && typeof onSyncStatus === "function") {
        onSyncStatus(sync);
      }
      const resolvedHealth = pollHealthPayload || health;
      const healthChecked = hasPayload(resolvedHealth);
      const syncChecked = hasPayload(sync);
      const storageChecked = hasPayload(storagePayload);
      const pollMeta = {
        anyChecked: healthChecked || syncChecked || storageChecked,
        coreChecked: healthChecked && syncChecked,
        storagePending: !storageChecked && includeStorage === false,
        activeWorkDeferred: deferCore,
        allRequiredChecked: healthChecked && syncChecked && storageChecked
      };
      const signals = evaluateAll(resolvedHealth, sync, storagePayload);
      lastPollMeta = pollMeta;
      hasCompletedPoll = true;
      renderSignals(signals, pollMeta);
      // Shared-feed renders do not increment this: it proves the Action Center
      // completed a poll of its own, independent of the 30 s wall clock.
      pollSequence += 1;
      if (refs.actionCenterItemsEl) {
        const sequence = String(pollSequence);
        if (refs.actionCenterItemsEl.dataset) refs.actionCenterItemsEl.dataset.pollSequence = sequence;
        else refs.actionCenterItemsEl.setAttribute?.("data-poll-sequence", sequence);
      }
    } catch (err) {
      if (logAdminError) logAdminError("action_center_poll", err);
      renderSignals({}, { anyChecked: false });
    } finally {
      pollInFlight = false;
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
    if (itemsEl) {
      itemsEl.innerHTML = renderActionCenterBody({ state: "checking", summary: CHECKING_SUMMARY });
    }
    if (refs.actionCenterStatusChipEl) {
      refs.actionCenterStatusChipEl.innerHTML = renderStatusChip({ state: "checking" });
    }
    const runInitialPoll = () => pollActionCenter({ includeStorage: false }).then(() => {
      bindEvents(itemsEl);
    });
    if (typeof enqueueStartupTask === "function") {
      Promise.resolve(enqueueStartupTask(runInitialPoll)).catch(err => {
        if (logAdminError) logAdminError("action_center_startup_poll", err);
      });
    } else {
      runInitialPoll();
    }
    const intervalMs = normalizePollIntervalMs(options?.pollIntervalMs ?? pollIntervalMs);
    const fullPollDelayMs = Math.max(0, Number(options?.fullPollDelayMs ?? intervalMs) || intervalMs);
    fullPollTimer = setTimeout(() => {
      pollActionCenter({ includeStorage: true }).catch(() => {});
      fullPollTimer = null;
    }, fullPollDelayMs);
    pollTimer = createVisibilityPausedInterval(() => {
      pollActionCenter({ includeStorage: true }).catch(() => {});
    }, intervalMs);
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
    unsubscribeHealthFeed();
  }

  return {
    startPolling,
    stopPolling,
    pollActionCenter,
    copyAllDiagnostics,
    dispose
  };
}
