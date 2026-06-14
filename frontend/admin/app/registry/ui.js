import { renderSourcesTableHtml } from "../../render.js?v=20";

const SOURCE_TABLE_ROW_HEIGHT_PX = 52;
const SOURCE_TABLE_VISIBLE_ROWS = 15;
const SOURCE_TABLE_OVERSCAN_ROWS = 12;
const SOURCE_TABLE_WINDOW_CHUNK_ROWS = 4;
const SOURCE_TABLE_VIRTUAL_THRESHOLD = 60;

function isCheckedSourceInput(el) {
  if (typeof HTMLInputElement === "undefined") {
    return Boolean(el?.checked);
  }
  return el instanceof HTMLInputElement && el.checked;
}

function getBucketContainer(refs, type) {
  if (type === "pending") return refs.adminPendingSourcesEl;
  if (type === "active") return refs.adminActiveSourcesEl;
  if (type === "rejected") return refs.adminRejectedSourcesEl;
  return null;
}

function queryScopedSelector(container, selector) {
  if (container && typeof container.querySelectorAll === "function") {
    return Array.from(container.querySelectorAll(selector));
  }
  return [];
}

function selectedIdsFromDom(container, selector) {
  return queryScopedSelector(container, selector)
    .filter(isCheckedSourceInput)
    .map(el => String(el?.dataset?.sourceId || ""))
    .filter(Boolean);
}

function selectedSourcesAcrossDiscoveryBucketsFromDom(refs) {
  const out = [];
  const seen = new Set();

  [
    ["pending", ".pending-source-checkbox"],
    ["active", ".active-source-checkbox"],
    ["rejected", ".rejected-source-checkbox"]
  ].forEach(([type, selector]) => {
    const container = getBucketContainer(refs, type);
    queryScopedSelector(container, selector)
      .filter(isCheckedSourceInput)
      .map(el => ({
        id: String(el?.dataset?.sourceId || "").trim(),
        url: String(el?.dataset?.sourceUrl || "").trim()
      }))
      .filter(item => item.id || item.url)
      .forEach(item => {
        const key = `${item.id}|${item.url}`;
        if (!key || seen.has(key)) return;
        seen.add(key);
        out.push(item);
      });
  });

  return out;
}

function toggleSelectAllSources(refs, type, checkAll) {
  const classMap = {
    pending: ".pending-source-checkbox",
    active: ".active-source-checkbox",
    rejected: ".rejected-source-checkbox"
  };
  const selector = classMap[type];
  if (!selector) return;
  queryScopedSelector(getBucketContainer(refs, type), selector).forEach(cb => {
    cb.checked = Boolean(checkAll);
  });
}

function getSelectAllRef(refs, type) {
  if (type === "pending") return refs.adminPendingSourcesSelectAllEl;
  if (type === "active") return refs.adminActiveSourcesSelectAllEl;
  if (type === "rejected") return refs.adminRejectedSourcesSelectAllEl;
  return null;
}

function sourceUrlFromRow(row) {
  return String(
    row?.listing_url
    || row?.api_url
    || row?.feed_url
    || row?.board_url
    || (Array.isArray(row?.pages) ? (row.pages[0] || "") : "")
    || ""
  ).trim();
}

function sourceSelectionItemFromRow(row) {
  const id = String(row?.id || "").trim();
  const url = sourceUrlFromRow(row);
  return {
    id,
    url,
    key: id || `|${url}`
  };
}

function sourceSelectionItemFromInput(el) {
  const id = String(el?.dataset?.sourceId || "").trim();
  const url = String(el?.dataset?.sourceUrl || "").trim();
  return {
    id,
    url,
    key: id || `|${url}`
  };
}

function createBucketState() {
  return {
    rows: [],
    selected: new Map(),
    scrollTop: 0,
    renderStart: 0,
    virtual: false,
    renderFrame: 0
  };
}

function getVirtualWindowStart(scrollTop) {
  const firstVisibleRow = Math.max(0, Math.floor(Number(scrollTop || 0) / SOURCE_TABLE_ROW_HEIGHT_PX));
  const overscannedStart = Math.max(0, firstVisibleRow - SOURCE_TABLE_OVERSCAN_ROWS);
  return Math.floor(overscannedStart / SOURCE_TABLE_WINDOW_CHUNK_ROWS) * SOURCE_TABLE_WINDOW_CHUNK_ROWS;
}

function setBodyDataset(bodyEl, mode, rows, start, end, virtual) {
  if (!bodyEl || !bodyEl.dataset) return;
  bodyEl.dataset.sourceMode = mode;
  bodyEl.dataset.totalRows = String(rows.length);
  if (virtual) {
    bodyEl.dataset.virtualized = "true";
    bodyEl.dataset.windowStart = String(start);
    bodyEl.dataset.windowEnd = String(end);
    return;
  }
  delete bodyEl.dataset.virtualized;
  delete bodyEl.dataset.windowStart;
  delete bodyEl.dataset.windowEnd;
}

function renderTableHtmlIntoContainer(container, html, { mode, rows, start, end, virtual }) {
  const existingBody = virtual && container?.querySelector?.(".admin-source-table-body");
  if (!existingBody || typeof document === "undefined" || typeof document.createElement !== "function") {
    container.innerHTML = html;
    return {
      bodyEl: container.querySelector?.(".admin-source-table-body") || null,
      reusedBody: false
    };
  }
  const template = document.createElement("template");
  template.innerHTML = String(html || "");
  const nextHeader = template.content?.querySelector?.(".jobs-table-header");
  const nextBody = template.content?.querySelector?.(".admin-source-table-body");
  if (!nextBody) {
    container.innerHTML = html;
    return {
      bodyEl: container.querySelector?.(".admin-source-table-body") || null,
      reusedBody: false
    };
  }
  const existingHeader = container.querySelector?.(".jobs-table-header");
  if (existingHeader && nextHeader) {
    existingHeader.innerHTML = nextHeader.innerHTML;
  }
  setBodyDataset(existingBody, mode, rows, start, end, virtual);
  existingBody.innerHTML = nextBody.innerHTML;
  return {
    bodyEl: existingBody,
    reusedBody: true
  };
}

function getBucketFromSelector(selector) {
  const value = String(selector || "");
  if (value.includes("pending")) return "pending";
  if (value.includes("active")) return "active";
  if (value.includes("rejected")) return "rejected";
  return "";
}

export function createRegistryUi({
  refs,
  getSourceJobsFoundCount,
  getSourceDiscoveryJobsCount = getSourceJobsFoundCount,
  deriveSourceStatus,
  deriveSourceApprovalStatus,
  renderSourcesTableHtml: renderSourcesTableHtmlImpl = renderSourcesTableHtml
}) {
  const bucketStates = {
    pending: createBucketState(),
    active: createBucketState(),
    rejected: createBucketState()
  };

  function setManualSourceFeedback(message, level = "muted") {
    if (!refs.adminManualSourceFeedbackEl) return;
    const normalized = String(level || "muted").toLowerCase();
    refs.adminManualSourceFeedbackEl.textContent = String(message || "");
    refs.adminManualSourceFeedbackEl.classList.remove("success", "warn", "error", "muted");
    refs.adminManualSourceFeedbackEl.classList.add(
      normalized === "success" ? "success" : normalized === "warn" ? "warn" : normalized === "error" ? "error" : "muted"
    );
  }

  function syncSelectAllCheckbox(mode) {
    const state = bucketStates[mode];
    const checkboxEl = getSelectAllRef(refs, mode);
    if (!state || !checkboxEl) return;
    const selectableKeys = new Set(state.rows.map(row => sourceSelectionItemFromRow(row).key));
    const selectedCount = Array.from(state.selected.keys()).filter(key => selectableKeys.has(key)).length;
    checkboxEl.checked = selectableKeys.size > 0 && selectedCount === selectableKeys.size;
    checkboxEl.indeterminate = selectedCount > 0 && selectedCount < selectableKeys.size;
  }

  function selectedIds(container, selector) {
    const mode = getBucketFromSelector(selector);
    const state = bucketStates[mode];
    if (!state) return selectedIdsFromDom(container, selector);
    if (state.selected.size === 0) {
      const domIds = selectedIdsFromDom(container, selector);
      if (domIds.length) return domIds;
    }
    return Array.from(state.selected.values())
      .map(item => item.id)
      .filter(Boolean);
  }

  function selectedSourcesAcrossDiscoveryBuckets() {
    const out = [];
    const seen = new Set();
    Object.values(bucketStates).forEach(state => {
      state.selected.forEach(item => {
        if (!item.id && !item.url) return;
        if (seen.has(item.key)) return;
        seen.add(item.key);
        out.push({ id: item.id, url: item.url });
      });
    });
    if (out.length) return out;
    return selectedSourcesAcrossDiscoveryBucketsFromDom(refs);
  }

  function renderBucketWindow(container, mode, { preserveScrollTop = null } = {}) {
    const state = bucketStates[mode];
    if (!container || !state) return;
    const rows = state.rows;
    const virtual = rows.length > SOURCE_TABLE_VIRTUAL_THRESHOLD;
    state.virtual = virtual;
    const scrollTop = preserveScrollTop == null ? state.scrollTop : Math.max(0, Number(preserveScrollTop || 0));
    state.scrollTop = scrollTop;
    const start = virtual ? getVirtualWindowStart(scrollTop) : 0;
    const visibleWindow = SOURCE_TABLE_VISIBLE_ROWS + (SOURCE_TABLE_OVERSCAN_ROWS * 2);
    const end = virtual ? Math.min(rows.length, start + visibleWindow) : rows.length;
    state.renderStart = start;
    const selectedSourceKeys = new Set(state.selected.keys());
    const selectedSourceIds = new Set(Array.from(state.selected.values()).map(item => item.id).filter(Boolean));
    const html = renderSourcesTableHtmlImpl(rows, mode, row => {
      const value = mode === "pending"
        ? getSourceDiscoveryJobsCount(row)
        : getSourceJobsFoundCount(row);
      return Number.isFinite(value) && value >= 0 ? value.toLocaleString() : "N/A";
    }, deriveSourceStatus, deriveSourceApprovalStatus, {
      virtual,
      startIndex: start,
      endIndex: end,
      rowHeightPx: SOURCE_TABLE_ROW_HEIGHT_PX,
      selectedSourceKeys,
      selectedSourceIds
    });
    const { bodyEl, reusedBody } = renderTableHtmlIntoContainer(container, html, {
      mode,
      rows,
      start,
      end,
      virtual
    });
    if (!bodyEl) {
      syncSelectAllCheckbox(mode);
      return;
    }
    if (virtual) {
      if (!reusedBody) {
        bodyEl.scrollTop = scrollTop;
      }
      if (!bodyEl.__baluffoSourceVirtualScrollBound) bodyEl.addEventListener("scroll", () => {
        const nextScrollTop = Number(bodyEl.scrollTop || 0);
        const nextStart = getVirtualWindowStart(nextScrollTop);
        if (nextStart === state.renderStart) {
          state.scrollTop = nextScrollTop;
          return;
        }
        state.scrollTop = nextScrollTop;
        if (state.renderFrame && typeof cancelAnimationFrame === "function") cancelAnimationFrame(state.renderFrame);
        const render = () => {
          state.renderFrame = 0;
          renderBucketWindow(container, mode, { preserveScrollTop: state.scrollTop });
        };
        state.renderFrame = typeof requestAnimationFrame === "function"
          ? requestAnimationFrame(render)
          : 0;
        if (!state.renderFrame) render();
      }, { passive: true });
      bodyEl.__baluffoSourceVirtualScrollBound = true;
    }
    if (!bodyEl.__baluffoSourceSelectionBound) bodyEl.addEventListener("change", event => {
      const inputEl = event?.target;
      if (!inputEl || String(inputEl?.dataset?.ui || "") !== "source-checkbox") return;
      const item = sourceSelectionItemFromInput(inputEl);
      if (!item.id && !item.url) return;
      if (inputEl.checked) {
        state.selected.set(item.key, item);
      } else {
        state.selected.delete(item.key);
      }
      syncSelectAllCheckbox(mode);
    });
    bodyEl.__baluffoSourceSelectionBound = true;
    syncSelectAllCheckbox(mode);
  }

  function renderSourcesTable(container, rows, mode = "pending") {
    if (!container) return;
    const state = bucketStates[mode];
    if (!state) return;
    state.rows = Array.isArray(rows) ? rows : [];
    state.selected.clear();
    state.scrollTop = 0;
    state.renderStart = 0;
    renderBucketWindow(container, mode);
  }

  function toggleSelectAllSourcesForBucket(type, checkAll) {
    const state = bucketStates[type];
    if (!state) {
      toggleSelectAllSources(refs, type, checkAll);
      return;
    }
    state.selected.clear();
    if (checkAll) {
      state.rows.forEach(row => {
        const item = sourceSelectionItemFromRow(row);
        if (item.id || item.url) state.selected.set(item.key, item);
      });
    }
    renderBucketWindow(getBucketContainer(refs, type), type, { preserveScrollTop: state.scrollTop });
  }

  return {
    setManualSourceFeedback,
    renderSourcesTable,
    getBucketContainer: type => getBucketContainer(refs, type),
    selectedIds,
    selectedSourcesAcrossDiscoveryBuckets,
    toggleSelectAllSources: toggleSelectAllSourcesForBucket
  };
}
