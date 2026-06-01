import test from "node:test";
import assert from "node:assert/strict";

import {
  installExplainStateHandler,
  showExplain
} from "../../../frontend/shared/ui/explain-state.js";

function classTokens(owner) {
  return String(owner.className || "").split(/\s+/).filter(Boolean);
}

function setClassTokens(owner, tokens) {
  owner.className = Array.from(new Set(tokens)).join(" ");
}

function createClassList(owner) {
  return {
    add: (...items) => setClassTokens(owner, [...classTokens(owner), ...items.map(String)]),
    remove: (...items) => {
      const removeSet = new Set(items.map(String));
      setClassTokens(owner, classTokens(owner).filter(item => !removeSet.has(item)));
    },
    contains: item => classTokens(owner).includes(String(item))
  };
}

function dataAttributeToKey(name) {
  return String(name || "")
    .replace(/^data-/, "")
    .replace(/-([a-z])/g, (_, letter) => letter.toUpperCase());
}

function createFakeElement(doc, tagName = "div") {
  const listeners = new Map();
  const el = {
    nodeType: 1,
    ownerDocument: doc,
    parentNode: null,
    parentElement: null,
    tagName: tagName.toUpperCase(),
    attributes: {},
    dataset: {},
    children: [],
    className: "",
    textContent: "",
    innerHTML: "",
    appendChild(child) {
      child.parentNode = this;
      child.parentElement = this;
      this.children.push(child);
      return child;
    },
    replaceChildren(...children) {
      this.children.forEach(child => {
        child.parentNode = null;
        child.parentElement = null;
      });
      this.children = [];
      children.forEach(child => this.appendChild(child));
    },
    setAttribute(name, value) {
      const text = String(value);
      this.attributes[name] = text;
      if (name.startsWith("data-")) this.dataset[dataAttributeToKey(name)] = text;
      if (name === "role") this.role = text;
    },
    getAttribute(name) {
      return Object.hasOwn(this.attributes, name) ? this.attributes[name] : null;
    },
    addEventListener(name, handler) {
      const handlers = listeners.get(name) || [];
      handlers.push(handler);
      listeners.set(name, handlers);
    },
    click() {
      const handlers = listeners.get("click") || [];
      handlers.forEach(handler => handler({
        target: this,
        stopPropagation() {}
      }));
    },
    querySelector(selector) {
      return this.querySelectorAll(selector)[0] || null;
    },
    querySelectorAll(selector) {
      const matches = [];
      const visit = node => {
        if (matchesSelector(node, selector)) matches.push(node);
        node.children.forEach(visit);
      };
      this.children.forEach(visit);
      return matches;
    },
    closest(selector) {
      let node = this;
      while (node) {
        if (matchesSelector(node, selector)) return node;
        node = node.parentNode;
      }
      return null;
    }
  };
  el.classList = createClassList(el);
  return el;
}

function matchesSelector(node, selector) {
  if (selector.startsWith(".")) {
    return classTokens(node).includes(selector.slice(1));
  }
  if (selector === "[data-explain-action]") {
    return Object.hasOwn(node.dataset, "explainAction");
  }
  if (selector === "[data-explain-state]") {
    return Object.hasOwn(node.dataset, "explainState");
  }
  if (selector === "[data-explain-target]") {
    return Object.hasOwn(node.dataset, "explainTarget");
  }
  return false;
}

function createExplainDom() {
  const listeners = new Map();
  const doc = {
    body: null,
    createElement: tagName => createFakeElement(doc, tagName),
    addEventListener(name, handler) {
      const handlers = listeners.get(name) || [];
      handlers.push(handler);
      listeners.set(name, handlers);
    },
    dispatch(name, event = {}) {
      const handlers = listeners.get(name) || [];
      handlers.forEach(handler => handler({
        target: event.target,
        key: event.key,
        preventDefault: event.preventDefault || (() => {})
      }));
    },
    querySelectorAll(selector) {
      return this.body.querySelectorAll(selector);
    },
    contains(target) {
      let node = target;
      while (node) {
        if (node === this.body) return true;
        node = node.parentNode;
      }
      return false;
    }
  };
  doc.body = createFakeElement(doc, "body");
  return doc;
}

test("showExplain renders quoted action ids without HTML attribute construction", () => {
  const previousDocument = global.document;
  const doc = createExplainDom();
  global.document = doc;

  try {
    let actionCount = 0;
    const actionId = 'retry" onclick="alert(1)';
    showExplain("<b>Unsafe title</b>", 'Body with "quotes" and <tags>', [
      {
        id: actionId,
        label: 'Retry "now" <script>',
        onAction: () => {
          actionCount += 1;
        }
      }
    ]);

    const content = doc.body.querySelector(".explain-state-content");
    assert.ok(content);
    assert.equal(content.querySelector(".explain-state-title").textContent, "<b>Unsafe title</b>");
    assert.equal(content.querySelector(".explain-state-body").textContent, 'Body with "quotes" and <tags>');

    const button = content.querySelector("[data-explain-action]");
    assert.ok(button);
    assert.equal(button.dataset.explainAction, actionId);
    assert.equal(button.textContent, 'Retry "now" <script>');

    button.click();
    assert.equal(actionCount, 1);
    assert.equal(doc.body.querySelector(".explain-state-overlay").classList.contains("hidden"), true);
  } finally {
    global.document = previousDocument;
  }
});

test("installExplainStateHandler resolves quoted action targets without selector interpolation", () => {
  const previousDocument = global.document;
  const doc = createExplainDom();
  global.document = doc;

  try {
    const actionId = 'retry" target';
    const source = doc.createElement("button");
    source.setAttribute("data-explain-state", "Retry state");
    source.setAttribute("data-explain-body", "Retry body");
    source.setAttribute("data-explain-actions", actionId);
    doc.body.appendChild(source);

    let targetClickCount = 0;
    const target = doc.createElement("button");
    target.setAttribute("data-explain-target", actionId);
    target.addEventListener("click", () => {
      targetClickCount += 1;
    });
    doc.body.appendChild(target);

    let prevented = false;
    installExplainStateHandler();
    doc.dispatch("click", {
      target: source,
      preventDefault: () => {
        prevented = true;
      }
    });

    const actionButton = doc.body.querySelector("[data-explain-action]");
    assert.ok(actionButton);
    assert.equal(actionButton.dataset.explainAction, actionId);
    actionButton.click();

    assert.equal(prevented, true);
    assert.equal(targetClickCount, 1);
  } finally {
    global.document = previousDocument;
  }
});
