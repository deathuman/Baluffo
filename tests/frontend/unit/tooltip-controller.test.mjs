import test from "node:test";
import assert from "node:assert/strict";

import {
  clearTooltip,
  clippedTooltipAttrs,
  setTooltip,
  tooltipAttrs
} from "../../../frontend/shared/ui/index.js";
import { installGlobalTooltipController } from "../../../frontend/shared/ui/tooltip-controller.js";

function createClassList(owner) {
  const tokens = new Set();
  return {
    add: (...items) => {
      items.forEach(item => tokens.add(String(item)));
      owner.className = Array.from(tokens).join(" ");
    },
    remove: (...items) => {
      items.forEach(item => tokens.delete(String(item)));
      owner.className = Array.from(tokens).join(" ");
    },
    toggle: (item, force) => {
      const token = String(item);
      const enabled = force === undefined ? !tokens.has(token) : Boolean(force);
      if (enabled) tokens.add(token);
      else tokens.delete(token);
      owner.className = Array.from(tokens).join(" ");
      return enabled;
    },
    contains: item => tokens.has(String(item))
  };
}

function createFakeElement(doc, tagName = "div") {
  const el = {
    nodeType: 1,
    ownerDocument: doc,
    parentNode: null,
    parentElement: null,
    tagName: tagName.toUpperCase(),
    attributes: {},
    dataset: {},
    children: [],
    style: {},
    className: "",
    textContent: "",
    title: "",
    clientWidth: 80,
    scrollWidth: 80,
    clientHeight: 24,
    scrollHeight: 24,
    _rect: { left: 100, top: 80, right: 180, bottom: 104, width: 80, height: 24 },
    appendChild(child) {
      child.parentNode = this;
      child.parentElement = this;
      this.children.push(child);
      return child;
    },
    removeChild(child) {
      this.children = this.children.filter(item => item !== child);
      child.parentNode = null;
      child.parentElement = null;
      return child;
    },
    remove() {
      this.parentNode?.removeChild?.(this);
    },
    setAttribute(name, value) {
      this.attributes[name] = String(value);
      if (name === "id") this.id = String(value);
      if (name === "data-tooltip") this.dataset.tooltip = String(value);
      if (name === "data-tooltip-if-clipped") this.dataset.tooltipIfClipped = String(value);
      if (name === "role") this.role = String(value);
    },
    getAttribute(name) {
      return Object.hasOwn(this.attributes, name) ? this.attributes[name] : null;
    },
    removeAttribute(name) {
      delete this.attributes[name];
      if (name === "data-tooltip") delete this.dataset.tooltip;
      if (name === "data-tooltip-if-clipped") delete this.dataset.tooltipIfClipped;
    },
    getBoundingClientRect() {
      return this._rect;
    }
  };
  el.classList = createClassList(el);
  return el;
}

function createTooltipDom() {
  const documentListeners = new Map();
  const windowListeners = new Map();
  const doc = {
    body: null,
    activeElement: null,
    createElement: tagName => createFakeElement(doc, tagName),
    addEventListener(name, handler) {
      const handlers = documentListeners.get(name) || [];
      handlers.push(handler);
      documentListeners.set(name, handlers);
    },
    removeEventListener(name, handler) {
      const handlers = documentListeners.get(name) || [];
      documentListeners.set(name, handlers.filter(item => item !== handler));
    },
    dispatch(name, event = {}) {
      const handlers = documentListeners.get(name) || [];
      handlers.forEach(handler => handler({ target: event.target, relatedTarget: null, ...event }));
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
  const windowTarget = {
    innerWidth: 320,
    innerHeight: 240,
    requestAnimationFrame(callback) {
      callback();
      return 1;
    },
    addEventListener(name, handler) {
      const handlers = windowListeners.get(name) || [];
      handlers.push(handler);
      windowListeners.set(name, handlers);
    },
    removeEventListener(name, handler) {
      const handlers = windowListeners.get(name) || [];
      windowListeners.set(name, handlers.filter(item => item !== handler));
    }
  };
  return { doc, windowTarget };
}

test("tooltip helpers set data-tooltip and remove legacy title", () => {
  assert.equal(
    tooltipAttrs('Use "quotes" & <tags>'),
    ' data-tooltip="Use &quot;quotes&quot; &amp; &lt;tags&gt;"'
  );
  assert.equal(tooltipAttrs(""), "");

  const { doc } = createTooltipDom();
  const el = createFakeElement(doc, "button");
  el.setAttribute("title", "Legacy");
  el.title = "Legacy";

  setTooltip(el, "Polished tooltip");
  assert.equal(el.getAttribute("data-tooltip"), "Polished tooltip");
  assert.equal(el.getAttribute("title"), null);
  assert.equal(el.title, "");

  clearTooltip(el);
  assert.equal(el.getAttribute("data-tooltip"), null);
});

test("tooltip controller only shows clipped conditional tooltips when text overflows", () => {
  assert.equal(
    clippedTooltipAttrs('Long "position" & title'),
    ' data-tooltip-if-clipped="Long &quot;position&quot; &amp; title"'
  );
  assert.equal(clippedTooltipAttrs(""), "");

  const { doc, windowTarget } = createTooltipDom();
  const target = createFakeElement(doc, "span");
  target.setAttribute("data-tooltip-if-clipped", "Complete position title");
  doc.body.appendChild(target);

  installGlobalTooltipController({ documentTarget: doc, windowTarget });
  doc.dispatch("pointerover", { target });
  assert.equal(doc.body.children.find(child => child.id === "baluffo-global-tooltip"), undefined);

  target.scrollWidth = 160;
  target.clientWidth = 80;
  doc.dispatch("pointerover", { target });
  const portal = doc.body.children.find(child => child.id === "baluffo-global-tooltip");
  assert.ok(portal);
  assert.equal(portal.textContent, "Complete position title");
  assert.equal(portal.getAttribute("aria-hidden"), "false");
});

test("tooltip controller shows via focus, restores aria-describedby, and flips near viewport edge", () => {
  const { doc, windowTarget } = createTooltipDom();
  const target = createFakeElement(doc, "button");
  target.setAttribute("data-tooltip", "Verify remote state");
  target.setAttribute("aria-describedby", "existing-description");
  doc.body.appendChild(target);

  const controller = installGlobalTooltipController({ documentTarget: doc, windowTarget });
  assert.equal(controller, installGlobalTooltipController({ documentTarget: doc, windowTarget }));

  doc.dispatch("focusin", { target });
  const portal = doc.body.children.find(child => child.id === "baluffo-global-tooltip");
  assert.ok(portal);
  assert.equal(portal.textContent, "Verify remote state");
  assert.equal(portal.getAttribute("aria-hidden"), "false");
  assert.equal(portal.dataset.placement, "top");
  assert.equal(target.getAttribute("aria-describedby"), "existing-description baluffo-global-tooltip");
  assert.match(portal.className, /\bvisible\b/);

  doc.dispatch("focusout", { target });
  assert.equal(target.getAttribute("aria-describedby"), "existing-description");
  assert.equal(portal.getAttribute("aria-hidden"), "true");

  target._rect = { left: 0, top: 2, right: 40, bottom: 26, width: 40, height: 24 };
  doc.dispatch("pointerover", { target });
  assert.equal(portal.dataset.placement, "bottom");
  assert.ok(Number.parseInt(portal.style.left, 10) >= 12);

  doc.dispatch("keydown", { key: "Escape", target });
  assert.equal(portal.getAttribute("aria-hidden"), "true");
});
