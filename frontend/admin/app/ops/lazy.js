/**
 * Lazy seam for the Admin Ops surface.
 *
 * The Admin page boots as a shell: the page chrome, bridge badge, stored-profile
 * overview and the action center. The Ops surface -- the health controller, its
 * five tab panels and every renderer they own -- is roughly 40% of the page's
 * module graph and nothing on the first paint needs it. `composition.js` builds
 * this facade synchronously and the real controller arrives in its own esbuild
 * chunk after the bootstrap settles, so the shell's parse and boot no longer
 * wait for ops code the user cannot see yet.
 *
 * Only calls that shape the initial paint are buffered (`QUEUE_WHILE_LOADING`):
 * the readiness shell, placeholders, the bootstrap payload and the bridge badge.
 * They replay in call order (last call per method wins) as soon as the chunk
 * lands, which is what keeps a slow chunk from painting a stale badge or
 * dropping the bootstrap's Ops data. Everything else awaits the chunk, so the
 * existing call sites keep their promise-shaped contract unchanged.
 *
 * When the chunk loads matters as much as what it leaves out: fetching it on
 * the first idle slot put its parse back inside the shell's own boot, which
 * measured slower than the single-asset page it replaced. So the chunk waits
 * for `start()`, which the composition calls once the bootstrap has settled
 * ("the shell is up; now do the Ops work"), with a fallback timer so a bootstrap
 * that never settles cannot strand the panel.
 *
 * A failed chunk load must not wedge the shell: the facade degrades to an inert
 * controller that still answers every call with a promise, and logs once.
 */

const LOAD_FALLBACK_TIMEOUT_MS = 1500;

// Last call wins per method; these run on the buffered payloads when the chunk
// lands instead of touching a controller that does not exist yet.
const QUEUE_WHILE_LOADING = [
  "applyBootstrapPayload",
  "setBridgeStatusBadge",
  "setOpsPlaceholders",
  "setOpsReadinessShell"
];

// Promise-shaped in the real controller, so the facade can simply await the
// chunk first. `getBridgeStatus` is intentionally absent: it returns a value,
// not a promise, and only the Ops controller itself reads it.
const DELEGATED = [
  "loadActiveOpsSummaryData",
  "loadIdleOpsHeavyHydration",
  "loadOpsHealthData",
  "loadOpsHistoryData",
  "loadOpsOverviewDetailData",
  "loadPipelineScheduleData",
  "loadPipelineStatusFallbackData",
  "loadRegistrySyncDiagnosticsData",
  "pollBridgeStatus",
  "scheduleOpsHealthPolling",
  "selectOpsTab",
  "startBridgeStatusWatch",
  "stopBridgeStatusWatch",
  "stopOpsHealthPolling"
];

function scheduleFallbackLoad(callback, delayMs) {
  const globalScope = globalThis;
  if (typeof globalScope.setTimeout !== "function") return;
  const timer = globalScope.setTimeout(callback, delayMs);
  timer?.unref?.();
}

export function createLazyAdminOpsController({
  createController,
  fallbackTimeoutMs = LOAD_FALLBACK_TIMEOUT_MS
}) {
  let controller = null;
  let loading = null;
  let started = false;
  let resolveStarted;
  const startGate = new Promise(resolve => {
    resolveStarted = resolve;
  });
  const queued = [];
  let fallbackArmed = false;

  function enqueue(name, args) {
    const index = queued.findIndex(entry => entry.name === name);
    if (index === -1) queued.push({ name, args });
    else queued[index] = { name, args };
  }

  function replayQueuedCalls() {
    const pending = queued.splice(0, queued.length);
    pending.forEach(({ name, args }) => {
      try {
        controller[name]?.(...args);
      } catch (error) {
        console.error(`[admin] Ops surface ${name} failed during replay:`, error);
      }
    });
  }

  function start() {
    if (started) return;
    started = true;
    resolveStarted();
  }

  function ensure() {
    if (loading) return loading;
    if (controller) return Promise.resolve(controller);
    loading = startGate
      .then(createController)
      .catch(error => {
        console.error("[admin] Could not load the Ops surface:", error);
        return null;
      })
      .then(impl => {
        controller = impl || {};
        replayQueuedCalls();
        return controller;
      });
    return loading;
  }

  function startAndLoad() {
    start();
    return ensure();
  }

  // The fallback only arms once the page asks for Ops behaviour, so a page that
  // never boots the panel does not fetch it either.
  function armFallbackLoad() {
    if (started || fallbackArmed) return;
    fallbackArmed = true;
    scheduleFallbackLoad(() => {
      void startAndLoad();
    }, fallbackTimeoutMs);
  }

  const facade = {};

  QUEUE_WHILE_LOADING.forEach(name => {
    facade[name] = (...args) => {
      enqueue(name, args);
      armFallbackLoad();
    };
  });

  DELEGATED.forEach(name => {
    facade[name] = (...args) => {
      armFallbackLoad();
      return ensure().then(impl => impl[name]?.(...args));
    };
  });

  facade.start = () => startAndLoad();
  // Test and diagnostic seam: starts the load immediately and resolves once the
  // chunk has been applied, without waiting for the bootstrap's settle signal.
  facade.ready = () => facade.start();
  facade.isLoaded = () => Boolean(controller);

  return facade;
}
