const SECTION_DEFINITIONS = Object.freeze({
  ops: {
    hash: "#admin-ops-section",
    elementId: "admin-ops-section"
  },
  fetcher: {
    hash: "#admin-fetcher-section",
    elementId: "admin-fetcher-section"
  },
  discovery: {
    hash: "#admin-discovery-section",
    elementId: "admin-discovery-section"
  },
  sync: {
    hash: "#admin-sync-section",
    elementId: "admin-sync-section"
  }
});

const ADMIN_SECTION_LOG_TAIL_LIMIT_CHARS = 8192;

function sectionKeyFromHash(hashValue) {
  const normalized = String(hashValue || "").trim();
  return Object.entries(SECTION_DEFINITIONS)
    .find(([_key, definition]) => definition.hash === normalized)?.[0] || "";
}

function setLoadingHtml(el, message) {
  if (!el) return;
  el.innerHTML = `<div class="admin-section-loading">${message}</div>`;
}

export function createAdminSectionLoadCoordinator({
  state,
  refs,
  documentObject = globalThis.document,
  windowObject = globalThis.window,
  opsController,
  fetcherController,
  discoveryController,
  registryController,
  syncController
} = {}) {
  const sectionStates = state?.adminSectionLoadState && typeof state.adminSectionLoadState === "object"
    ? state.adminSectionLoadState
    : {};
  if (state && !state.adminSectionLoadState) state.adminSectionLoadState = sectionStates;
  const queue = [];
  let activeKey = "";
  let started = false;

  function getSectionState(key) {
    if (!sectionStates[key]) {
      sectionStates[key] = {
        status: "idle",
        promise: null,
        error: ""
      };
    }
    return sectionStates[key];
  }

  async function loadOpsSection() {
    if (!state?.opsHistoryLoaded && refs?.adminOpsHistoryEl) {
      setLoadingHtml(refs.adminOpsHistoryEl, "Loading recent run history...");
    }
    await opsController?.loadOpsHistoryData?.({ limit: 2, silent: true });
  }

  async function loadFetcherSection() {
    fetcherController?.setFetcherLogPlaceholder?.("Loading latest fetcher output...");
    await fetcherController?.loadFetcherLogChunk?.({
      reset: true,
      showEmptyState: true,
      view: "tail",
      limitChars: ADMIN_SECTION_LOG_TAIL_LIMIT_CHARS
    });
    await fetcherController?.loadLatestFetcherSummary?.({ silent: false });
  }

  async function loadDiscoverySection() {
    discoveryController?.setDiscoveryLogPlaceholder?.("Loading discovery output...");
    await registryController?.loadDiscoveryData?.({
      sourceTablesOnly: true,
      skipIfFreshMs: 10000
    });
    await discoveryController?.loadDiscoveryLogChunk?.({
      reset: true,
      guarded: false,
      view: "tail",
      limitChars: ADMIN_SECTION_LOG_TAIL_LIMIT_CHARS
    });
  }

  async function loadSyncSection() {
    if (refs?.adminSyncStatusEl && !state?.latestSyncStatusCache) {
      setLoadingHtml(refs.adminSyncStatusEl, "Loading sync status...");
    }
    await syncController?.loadSyncStatus?.({
      silent: true,
      forceForm: false,
      includeLive: true,
      summary: false
    });
  }

  const loaders = {
    ops: loadOpsSection,
    fetcher: loadFetcherSection,
    discovery: loadDiscoverySection,
    sync: loadSyncSection
  };

  async function runNext() {
    if (activeKey) return;
    const nextKey = queue.shift();
    if (!nextKey) return;
    activeKey = nextKey;
    const loadState = getSectionState(nextKey);
    loadState.status = "loading";
    try {
      const promise = Promise.resolve(loaders[nextKey]?.());
      loadState.promise = promise;
      await promise;
      loadState.status = "loaded";
      loadState.error = "";
    } catch (err) {
      loadState.status = "failed";
      loadState.error = String(err?.message || err || "unknown error");
    } finally {
      loadState.promise = null;
      activeKey = "";
      runNext();
    }
  }

  function enqueueSection(key, { force = false } = {}) {
    const normalizedKey = String(key || "").trim();
    if (!loaders[normalizedKey]) return null;
    const loadState = getSectionState(normalizedKey);
    if (!force && (loadState.status === "loaded" || loadState.status === "loading")) {
      return loadState.promise || null;
    }
    if (!force && queue.includes(normalizedKey)) return loadState.promise || null;
    if (force) {
      const existingIndex = queue.indexOf(normalizedKey);
      if (existingIndex >= 0) queue.splice(existingIndex, 1);
    }
    queue.push(normalizedKey);
    runNext();
    return loadState.promise || null;
  }

  function handleHashChange() {
    const key = sectionKeyFromHash(windowObject?.location?.hash || "");
    if (key) enqueueSection(key);
  }

  function handleOlderHistoryToggle(event) {
    const target = event?.target;
    if (!target?.matches?.("[data-ops-load-older-history]")) return;
    if (!target.open || state?.opsHistoryFullLoaded) return;
    const body = target.querySelector?.(".jobs-table-body");
    setLoadingHtml(body, "Loading older run history...");
    opsController?.loadOpsHistoryData?.({ limit: 80, silent: true }).catch(() => {
      setLoadingHtml(body, "Could not load older run history.");
    });
  }

  function start() {
    if (started) return;
    started = true;
    documentObject?.querySelectorAll?.('a[href^="#admin-"]')?.forEach?.(link => {
      link.addEventListener?.("click", () => {
        const key = sectionKeyFromHash(link.getAttribute?.("href") || "");
        if (key) {
          windowObject?.setTimeout?.(() => enqueueSection(key), 0);
        }
      });
    });
    windowObject?.addEventListener?.("hashchange", handleHashChange);
    refs?.adminOpsHistoryEl?.addEventListener?.("toggle", handleOlderHistoryToggle, true);
    handleHashChange();
  }

  return {
    start,
    enqueueSection,
    getSectionState,
    handleOlderHistoryToggle
  };
}
