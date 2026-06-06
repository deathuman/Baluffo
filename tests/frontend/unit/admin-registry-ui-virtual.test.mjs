import test from "node:test";
import assert from "node:assert/strict";
import { createRegistryUi } from "../../../frontend/admin/app/registry/ui.js";

function createClassList() {
  return {
    add() {},
    remove() {},
    toggle() {},
    contains() {
      return false;
    }
  };
}

function createBodyElement() {
  const listeners = new Map();
  return {
    dataset: {},
    innerHTML: "",
    scrollTop: 0,
    addEventListener(type, callback) {
      listeners.set(type, callback);
    },
    dispatch(type, event = {}) {
      const callback = listeners.get(type);
      if (callback) callback({ target: this, ...event });
    }
  };
}

function createHeaderElement() {
  return {
    innerHTML: ""
  };
}

function createContainer() {
  return {
    _innerHTML: "",
    _body: null,
    _header: null,
    classList: createClassList(),
    set innerHTML(value) {
      this._innerHTML = String(value || "");
      this._body = this._innerHTML.includes("admin-source-table-body") ? createBodyElement() : null;
      this._header = this._innerHTML.includes("jobs-table-header") ? createHeaderElement() : null;
      if (this._body) {
        this._body.innerHTML = this._innerHTML;
      }
    },
    get innerHTML() {
      return this._innerHTML;
    },
    querySelector(selector) {
      if (selector === ".admin-source-table-body") return this._body;
      if (selector === ".jobs-table-header") return this._header;
      return null;
    },
    querySelectorAll() {
      return [];
    }
  };
}

function withTemplateDocument(callback) {
  const originalDocument = globalThis.document;
  globalThis.document = {
    createElement(tagName) {
      assert.equal(tagName, "template");
      return {
        _innerHTML: "",
        set innerHTML(value) {
          this._innerHTML = String(value || "");
        },
        get innerHTML() {
          return this._innerHTML;
        },
        content: {
          querySelector: selector => {
            const html = globalThis.document.__lastTemplateHtml || "";
            if (selector === ".jobs-table-header") return { innerHTML: "header" };
            if (selector !== ".admin-source-table-body") return null;
            const bodyStart = html.indexOf("admin-source-table-body");
            const bodyHtml = bodyStart >= 0 ? html.slice(bodyStart) : "";
            return { innerHTML: bodyHtml };
          }
        }
      };
    },
    __lastTemplateHtml: ""
  };
  const createElement = globalThis.document.createElement;
  globalThis.document.createElement = tagName => {
    const template = createElement(tagName);
    Object.defineProperty(template, "innerHTML", {
      set(value) {
        this._innerHTML = String(value || "");
        globalThis.document.__lastTemplateHtml = this._innerHTML;
      },
      get() {
        return this._innerHTML;
      }
    });
    return template;
  };
  try {
    return callback();
  } finally {
    if (originalDocument === undefined) {
      delete globalThis.document;
    } else {
      globalThis.document = originalDocument;
    }
  }
}

function sourceRows(count) {
  return Array.from({ length: count }, (_, index) => ({
    id: `source-${index}`,
    name: `Source ${index}`,
    adapter: "static",
    studio: `Studio ${index}`,
    jobsFound: index + 1,
    status: "ok"
  }));
}

function createUiFixture() {
  const refs = {
    adminManualSourceFeedbackEl: { textContent: "", classList: createClassList() },
    adminPendingSourcesEl: createContainer(),
    adminActiveSourcesEl: createContainer(),
    adminRejectedSourcesEl: createContainer(),
    adminPendingSourcesSelectAllEl: { checked: false, indeterminate: false },
    adminActiveSourcesSelectAllEl: { checked: false, indeterminate: false },
    adminRejectedSourcesSelectAllEl: { checked: false, indeterminate: false }
  };
  const ui = createRegistryUi({
    refs,
    getSourceJobsFoundCount: row => Number(row.jobsFound || 0),
    getSourceDiscoveryJobsCount: row => Number(row.jobsFound || 0),
    deriveSourceStatus: row => String(row.status || "not_run"),
    deriveSourceApprovalStatus: () => ({ label: "Live", tone: "healthy" })
  });
  return { refs, ui };
}

test("admin registry UI virtualizes large source buckets", () => {
  const { refs, ui } = createUiFixture();
  ui.renderSourcesTable(refs.adminPendingSourcesEl, sourceRows(120), "pending");

  const renderedRows = refs.adminPendingSourcesEl.innerHTML.match(/admin-user-row admin-source-row/g) || [];
  assert.equal(renderedRows.length, 39);
  assert.match(refs.adminPendingSourcesEl.innerHTML, /data-virtualized="true"/);
  assert.match(refs.adminPendingSourcesEl.innerHTML, /Source 0/);
  assert.doesNotMatch(refs.adminPendingSourcesEl.innerHTML, /Source 80/);
});

test("admin registry UI scrolls virtual source buckets without losing selected offscreen rows", () => {
  const { refs, ui } = createUiFixture();
  ui.renderSourcesTable(refs.adminPendingSourcesEl, sourceRows(120), "pending");

  refs.adminPendingSourcesEl._body.scrollTop = 52 * 80;
  refs.adminPendingSourcesEl._body.dispatch("scroll");

  assert.match(refs.adminPendingSourcesEl.innerHTML, /Source 80/);
  assert.doesNotMatch(refs.adminPendingSourcesEl.innerHTML, /Source 0/);
});

test("admin registry UI keeps the virtual scroll body stable while changing windows", () => {
  withTemplateDocument(() => {
    const { refs, ui } = createUiFixture();
    ui.renderSourcesTable(refs.adminPendingSourcesEl, sourceRows(120), "pending");
    const bodyEl = refs.adminPendingSourcesEl._body;

    bodyEl.scrollTop = 52 * 80;
    bodyEl.dispatch("scroll");

    assert.equal(refs.adminPendingSourcesEl._body, bodyEl);
    assert.match(refs.adminPendingSourcesEl._body.innerHTML, /Source 80/);
  });
});

test("admin registry UI select-all applies to the full filtered bucket, not only visible rows", () => {
  const { refs, ui } = createUiFixture();
  ui.renderSourcesTable(refs.adminActiveSourcesEl, sourceRows(120), "active");

  ui.toggleSelectAllSources("active", true);

  assert.equal(refs.adminActiveSourcesSelectAllEl.checked, true);
  assert.equal(refs.adminActiveSourcesSelectAllEl.indeterminate, false);
  assert.equal(ui.selectedIds(refs.adminActiveSourcesEl, ".active-source-checkbox").length, 120);
  assert.equal(ui.selectedSourcesAcrossDiscoveryBuckets().length, 120);
});

test("admin registry UI keeps individual selection across virtual scroll windows", () => {
  const { refs, ui } = createUiFixture();
  ui.renderSourcesTable(refs.adminPendingSourcesEl, sourceRows(120), "pending");
  refs.adminPendingSourcesEl._body.dispatch("change", {
    target: {
      checked: true,
      dataset: {
        ui: "source-checkbox",
        sourceId: "source-2",
        sourceUrl: "https://example.test/source-2"
      }
    }
  });

  refs.adminPendingSourcesEl._body.scrollTop = 52 * 90;
  refs.adminPendingSourcesEl._body.dispatch("scroll");

  assert.deepEqual(ui.selectedIds(refs.adminPendingSourcesEl, ".pending-source-checkbox"), ["source-2"]);
  assert.equal(refs.adminPendingSourcesSelectAllEl.indeterminate, true);
});
