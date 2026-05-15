import test from "node:test";
import assert from "node:assert/strict";

import {
  applyAdminAdvancedBulkLayout
} from "../../../frontend/admin/app/bulk-actions.js";

function createClassList(initial = []) {
  const values = new Set(initial);
  return {
    add(...tokens) {
      tokens.forEach(token => values.add(token));
    },
    remove(...tokens) {
      tokens.forEach(token => values.delete(token));
    },
    toggle(token, force) {
      if (force === true) {
        values.add(token);
        return true;
      }
      if (force === false) {
        values.delete(token);
        return false;
      }
      if (values.has(token)) {
        values.delete(token);
        return false;
      }
      values.add(token);
      return true;
    },
    contains(token) {
      return values.has(token);
    }
  };
}

class FakeElement {
  constructor(tagName) {
    this.tagName = String(tagName || "div").toUpperCase();
    this.nodeType = 1;
    this.children = [];
    this.parentNode = null;
    this.attributes = {};
    this.dataset = {};
    this.classList = createClassList();
    this.textContent = "";
    this.id = "";
  }

  setAttribute(name, value) {
    const safeName = String(name);
    const safeValue = String(value);
    this.attributes[safeName] = safeValue;
    if (safeName === "id") this.id = safeValue;
    if (safeName === "class") {
      safeValue.split(/\s+/).filter(Boolean).forEach(token => this.classList.add(token));
    }
    if (safeName.startsWith("data-")) {
      const key = safeName
        .slice(5)
        .replace(/-([a-z])/g, (_match, char) => char.toUpperCase());
      this.dataset[key] = safeValue;
    }
  }

  appendChild(child) {
    if (!child) return child;
    child.parentNode?.removeChild(child);
    child.parentNode = this;
    this.children.push(child);
    return child;
  }

  insertBefore(child, beforeNode) {
    if (!child) return child;
    child.parentNode?.removeChild(child);
    child.parentNode = this;
    const index = beforeNode ? this.children.indexOf(beforeNode) : -1;
    if (index >= 0) {
      this.children.splice(index, 0, child);
    } else {
      this.children.push(child);
    }
    return child;
  }

  removeChild(child) {
    const index = this.children.indexOf(child);
    if (index >= 0) {
      this.children.splice(index, 1);
      child.parentNode = null;
    }
    return child;
  }

  get nextSibling() {
    if (!this.parentNode) return null;
    const siblings = this.parentNode.children;
    const index = siblings.indexOf(this);
    return index >= 0 ? siblings[index + 1] || null : null;
  }

  closest(selector) {
    let node = this;
    while (node) {
      if (matchesSelector(node, selector)) return node;
      node = node.parentNode;
    }
    return null;
  }

  querySelector(selector) {
    return findAll(this, selector)[0] || null;
  }
}

function matchesSelector(el, selector) {
  if (!el || !selector) return false;
  if (selector.startsWith(".")) return el.classList.contains(selector.slice(1));
  if (selector.startsWith("#")) return el.id === selector.slice(1);
  const dataUiMatch = selector.match(/^\[data-ui="([^"]+)"\]$/);
  if (dataUiMatch) return el.dataset.ui === dataUiMatch[1];
  return false;
}

function findAll(root, selector, results = []) {
  for (const child of root.children || []) {
    if (matchesSelector(child, selector)) results.push(child);
    findAll(child, selector, results);
  }
  return results;
}

function makeButton(doc, id, ui, label) {
  const button = doc.createElement("button");
  button.setAttribute("id", id);
  button.setAttribute("data-ui", ui);
  button.textContent = label;
  return button;
}

function buildAdminBulkDoc() {
  const doc = {
    body: new FakeElement("body"),
    createElement: tagName => new FakeElement(tagName),
    querySelector(selector) {
      return this.body.querySelector(selector);
    }
  };
  const card = doc.createElement("div");
  card.setAttribute("class", "admin-discovery-card admin-discovery-bulk-card");
  const title = doc.createElement("h4");
  title.textContent = "Bulk Actions";
  const primaryRow = doc.createElement("div");
  primaryRow.setAttribute("class", "admin-fetcher-actions");
  const secondaryRow = doc.createElement("div");
  secondaryRow.setAttribute("class", "admin-fetcher-actions");

  const approve = makeButton(doc, "admin-approve-sources-btn", "admin-approve-sources-btn", "Approve Selected");
  const reject = makeButton(doc, "admin-reject-sources-btn", "admin-reject-sources-btn", "Reject Selected");
  const restore = makeButton(doc, "admin-restore-rejected-btn", "admin-restore-rejected-btn", "Restore Selected");
  const demote = makeButton(doc, "admin-demote-active-btn", "admin-demote-active-btn", "Demote zero-jobs to Pending");
  const deleteButton = makeButton(doc, "admin-delete-sources-btn", "admin-delete-sources-btn", "Delete Selected");

  primaryRow.appendChild(approve);
  primaryRow.appendChild(reject);
  primaryRow.appendChild(restore);
  secondaryRow.appendChild(demote);
  secondaryRow.appendChild(deleteButton);
  card.appendChild(title);
  card.appendChild(primaryRow);
  card.appendChild(secondaryRow);
  doc.body.appendChild(card);

  return {
    doc,
    refs: {
      adminApproveSourcesBtnEl: approve,
      adminRejectSourcesBtnEl: reject,
      adminRestoreRejectedBtnEl: restore,
      adminDemoteActiveBtnEl: demote,
      adminDeleteSourcesBtnEl: deleteButton
    }
  };
}

test("admin default creates a closed advanced bulk disclosure and preserves button hooks", () => {
  const { doc, refs } = buildAdminBulkDoc();

  applyAdminAdvancedBulkLayout({ doc, refs });

  const details = doc.querySelector('[data-ui="admin-advanced-bulk-actions"]');
  assert.ok(details);
  assert.equal(Boolean(details.open), false);
  assert.match(details.children[0].textContent, /Advanced bulk actions/);
  assert.match(details.children[1].textContent, /These actions change source registry state/);

  assert.equal(refs.adminApproveSourcesBtnEl.closest('[data-ui="admin-advanced-bulk-actions"]'), null);
  assert.equal(refs.adminRejectSourcesBtnEl.closest('[data-ui="admin-advanced-bulk-actions"]'), null);
  assert.equal(refs.adminRestoreRejectedBtnEl.closest('[data-ui="admin-advanced-bulk-actions"]'), details);
  assert.equal(refs.adminDemoteActiveBtnEl.closest('[data-ui="admin-advanced-bulk-actions"]'), details);
  assert.equal(refs.adminDeleteSourcesBtnEl.closest('[data-ui="admin-advanced-bulk-actions"]'), details);

  [
    "admin-approve-sources-btn",
    "admin-reject-sources-btn",
    "admin-restore-rejected-btn",
    "admin-demote-active-btn",
    "admin-delete-sources-btn"
  ].forEach(id => {
    assert.equal(findAll(doc.body, `#${id}`).length, 1);
  });
  assert.equal(refs.adminBulkBusyMessageEl.dataset.ui, "admin-bulk-busy-message");
});

test("admin advanced bulk layout is idempotent", () => {
  const { doc, refs } = buildAdminBulkDoc();

  const first = applyAdminAdvancedBulkLayout({ doc, refs });
  const second = applyAdminAdvancedBulkLayout({ doc, refs });

  assert.equal(first, second);
  assert.equal(findAll(doc.body, '[data-ui="admin-advanced-bulk-actions"]').length, 1);
  assert.equal(findAll(doc.body, '[data-ui="admin-bulk-busy-message"]').length, 1);
});
