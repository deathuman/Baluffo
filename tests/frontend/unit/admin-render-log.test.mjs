import test from "node:test";
import assert from "node:assert/strict";
import { appendAdminLogRow } from "../../../frontend/admin/render.js";
import {
  createElement,
} from "./helpers/admin-controller-test-helpers.mjs";

test("appendAdminLogRow passes Date to custom toLocalTime and renders its result", () => {
  const created = [];
  const previousDocument = global.document;
  global.document = {
    createElement(tagName) {
      const el = createElement({
        tagName,
        dataset: {},
        children: [],
        append(...nodes) {
          this.children.push(...nodes);
        },
        appendChild(node) {
          this.children.push(node);
        },
        removeChild(node) {
          const index = this.children.indexOf(node);
          if (index >= 0) this.children.splice(index, 1);
        },
        scrollTop: 0,
        scrollHeight: 0
      });
      created.push(el);
      return el;
    }
  };

  try {
    const container = createElement({
      children: [],
      appendChild(node) {
        this.children.push(node);
      },
      removeChild(node) {
        const index = this.children.indexOf(node);
        if (index >= 0) this.children.splice(index, 1);
      },
      scrollTop: 0,
      scrollHeight: 0
    });

    const event = {
      timestamp: "2026-03-08T10:00:00.000Z",
      level: "info",
      scope: "fetcher",
      sourceId: "",
      message: "START source=test"
    };

    let receivedValue = null;
    function toLocalTimeSpy(value) {
      receivedValue = value;
      return "10:00:00";
    }

    appendAdminLogRow(container, event, {
      normalizeLogLevel: value => value,
      toLocalTime: toLocalTimeSpy,
      formatLogEventText: row => String(row?.message || "")
    });

    assert.ok(receivedValue instanceof Date);
    assert.equal(container.children.length, 1);
    const rowEl = container.children[0];
    assert.ok(Array.isArray(rowEl.children));
    assert.ok(rowEl.children.length >= 1);
    const stampEl = rowEl.children[0];
    assert.equal(String(stampEl.textContent), "10:00:00");
  } finally {
    global.document = previousDocument;
  }
});

test("appendAdminLogRow falls back safely when timestamp is invalid", () => {
  const previousDocument = global.document;
  global.document = {
    createElement(tagName) {
      const el = createElement({
        tagName,
        dataset: {},
        children: [],
        append(...items) {
          this.children.push(...items);
        }
      });
      return el;
    }
  };
  try {
    const container = createElement({
      children: [],
      firstChild: null,
      appendChild(child) {
        this.children.push(child);
        this.firstChild = this.children[0] || null;
      },
      removeChild(child) {
        const index = this.children.indexOf(child);
        if (index >= 0) {
          this.children.splice(index, 1);
        }
        this.firstChild = this.children[0] || null;
      },
      scrollTop: 0,
      scrollHeight: 0
    });
    appendAdminLogRow(
      container,
      {
        timestamp: "[[2026-03-08T10:01:00.000Z",
        level: "info",
        scope: "fetcher",
        sourceId: "",
        message: "Broken timestamp"
      },
      {
        normalizeLogLevel: value => value,
        toLocalTime: value => value.toString(),
        formatLogEventText: row => String(row?.message || "")
      }
    );

    const rowEl = container.children[0];
    const stampEl = rowEl.children[0];
    assert.notEqual(String(stampEl.textContent), "Invalid Date");
  } finally {
    global.document = previousDocument;
  }
});
