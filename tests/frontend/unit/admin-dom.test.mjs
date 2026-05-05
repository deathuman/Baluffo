import test from "node:test";
import assert from "node:assert/strict";
import { cacheAdminDom } from "../../../frontend/admin/app/dom.js";

test("admin DOM refs resolve lazily and cache query results", () => {
  const queryCalls = [];
  const queryAllCalls = [];
  const elements = new Map();
  const doc = {
    querySelector(selector) {
      queryCalls.push(selector);
      if (!elements.has(selector)) {
        elements.set(selector, { selector });
      }
      return elements.get(selector);
    },
    querySelectorAll(selector) {
      queryAllCalls.push(selector);
      return [{ selector, index: 0 }, { selector, index: 1 }];
    }
  };

  const refs = cacheAdminDom(doc);

  assert.equal(queryCalls.length, 6);
  assert.equal(queryAllCalls.length, 0);
  assert.equal(queryCalls.some(selector => selector.includes("admin-refresh-btn")), false);

  const refreshButton = refs.adminRefreshBtnEl;
  assert.equal(refreshButton.selector, '[data-ui="admin-refresh-btn"]');
  assert.equal(refs.adminRefreshBtnEl, refreshButton);
  assert.equal(queryCalls.filter(selector => selector.includes("admin-refresh-btn")).length, 1);

  assert.equal(refs.adminOpsTabBtnEls.length, 2);
  assert.equal(refs.adminOpsTabBtnEls.length, 2);
  assert.equal(queryAllCalls.filter(selector => selector.includes("admin-ops-tab-btn")).length, 1);
});
