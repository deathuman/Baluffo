import test from "node:test";
import assert from "node:assert/strict";

import {
  openReleaseNotesDialog,
  renderReleaseNotesMarkdown
} from "../../../frontend/shared/ui/release-notes-dialog.js";

function createFakeDocument() {
  const documentListeners = new Map();

  function createTextNode(text) {
    return {
      nodeType: "text",
      ownerDocument: doc,
      parentNode: null,
      _textContent: String(text || ""),
      get textContent() {
        return this._textContent;
      },
      set textContent(value) {
        this._textContent = String(value || "");
      },
      remove() {
        this.parentNode?.removeChild?.(this);
      }
    };
  }

  function createElement(tagName) {
    const listeners = new Map();
    const element = {
      nodeType: "element",
      ownerDocument: doc,
      parentNode: null,
      tagName: String(tagName || "").toUpperCase(),
      className: "",
      dataset: {},
      attributes: {},
      children: [],
      _textContent: "",
      href: "",
      rel: "",
      target: "",
      id: "",
      type: "",
      get textContent() {
        if (this._textContent) return this._textContent;
        return this.children.map(child => child.textContent).join("");
      },
      set textContent(value) {
        this._textContent = String(value || "");
        this.children = [];
      },
      appendChild(child) {
        this._textContent = "";
        child.parentNode = this;
        this.children.push(child);
        return child;
      },
      append(...nodes) {
        nodes.forEach(node => {
          if (typeof node === "string") {
            this.appendChild(createTextNode(node));
            return;
          }
          this.appendChild(node);
        });
      },
      removeChild(child) {
        const index = this.children.indexOf(child);
        if (index >= 0) {
          this.children.splice(index, 1);
          child.parentNode = null;
        }
        return child;
      },
      remove() {
        this.parentNode?.removeChild?.(this);
      },
      addEventListener(name, handler) {
        const handlers = listeners.get(name) || [];
        handlers.push(handler);
        listeners.set(name, handlers);
      },
      removeEventListener(name, handler) {
        const handlers = listeners.get(name) || [];
        listeners.set(name, handlers.filter(item => item !== handler));
      },
      dispatch(name, event = {}) {
        const handlers = listeners.get(name) || [];
        handlers.forEach(handler => handler({
          target: this,
          preventDefault() {},
          ...event,
        }));
      },
      setAttribute(name, value) {
        this.attributes[name] = String(value);
        if (name === "id") this.id = String(value);
      },
      focus() {
        doc.activeElement = this;
      },
    };
    return element;
  }

  function walk(node, predicate) {
    if (!node) return null;
    if (predicate(node)) return node;
    const children = Array.isArray(node.children) ? node.children : [];
    for (const child of children) {
      const match = walk(child, predicate);
      if (match) return match;
    }
    return null;
  }

  const doc = {
    activeElement: null,
    defaultView: {
      requestAnimationFrame(callback) {
        callback();
        return 1;
      },
      open() {
        return null;
      }
    },
    body: null,
    createElement,
    createTextNode,
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
      handlers.forEach(handler => handler({
        preventDefault() {},
        ...event,
      }));
    },
    contains(target) {
      return Boolean(walk(this.body, node => node === target));
    }
  };
  doc.body = createElement("body");
  doc.body.ownerDocument = doc;
  doc.find = predicate => walk(doc.body, predicate);
  return doc;
}

test("renderReleaseNotesMarkdown renders headings, lists, inline code, and links", () => {
  const doc = createFakeDocument();
  const container = doc.createElement("div");
  const openedUrls = [];
  doc.body.appendChild(container);

  renderReleaseNotesMarkdown(
    container,
    "# Added\n\n- Read the [guide](https://example.com/guide)\n- Use `baluffo update`\n\nParagraph text.",
    {
      documentTarget: doc,
      openExternalUrl: url => openedUrls.push(url),
      windowTarget: doc.defaultView,
    }
  );

  assert.equal(container.children[0].tagName, "H3");
  assert.equal(container.children[0].textContent, "Added");

  const list = container.children[1];
  assert.equal(list.tagName, "UL");
  assert.equal(list.children.length, 2);

  const guideLink = doc.find(node => node.tagName === "A" && node.href === "https://example.com/guide");
  assert.ok(guideLink);
  guideLink.dispatch("click");
  assert.deepEqual(openedUrls, ["https://example.com/guide"]);

  const inlineCode = doc.find(node => node.tagName === "CODE");
  assert.ok(inlineCode);
  assert.equal(inlineCode.textContent, "baluffo update");
  assert.equal(container.children[2].tagName, "P");
  assert.equal(container.children[2].textContent, "Paragraph text.");
});

test("openReleaseNotesDialog renders fallback state, supports escape close, and restores focus", () => {
  const doc = createFakeDocument();
  const trigger = doc.createElement("button");
  const openedUrls = [];
  doc.body.appendChild(trigger);
  trigger.focus();

  const dialog = openReleaseNotesDialog({
    title: "Baluffo v0.1.2",
    markdown: "",
    publishedAt: "2026-04-15T10:00:00Z",
    releaseNotesUrl: "https://example.com/releases/v0.1.2",
    openExternalUrl: url => openedUrls.push(url),
    fallbackMessage: "Release notes are unavailable in-app for this build.",
    documentTarget: doc,
    windowTarget: doc.defaultView,
  });

  assert.ok(dialog);
  assert.equal(dialog.overlay.parentNode, doc.body);
  assert.match(dialog.overlay.className, /\bpopup-overlay-visible\b/);
  assert.match(dialog.panel.className, /\bpopup-visible\b/);
  assert.equal(
    doc.activeElement,
    doc.find(node => typeof node.className === "string" && node.className.includes("release-notes-dialog-close"))
  );
  assert.ok(doc.find(node => node.className === "release-notes-dialog-empty"));

  const openButton = doc.find(
    node => typeof node.className === "string" && node.className.includes("release-notes-dialog-open")
  );
  const closeButton = doc.find(
    node => typeof node.className === "string" && node.className.includes("release-notes-dialog-close")
  );
  assert.ok(openButton);
  assert.ok(closeButton);
  assert.match(openButton.className, /\bpopup-btn-primary\b/);
  assert.match(closeButton.className, /\bpopup-btn-secondary\b/);
  openButton.dispatch("click");
  assert.deepEqual(openedUrls, ["https://example.com/releases/v0.1.2"]);

  let prevented = false;
  doc.dispatch("keydown", {
    key: "Escape",
    preventDefault() {
      prevented = true;
    }
  });

  assert.equal(prevented, true);
  assert.equal(dialog.overlay.parentNode, null);
  assert.equal(doc.activeElement, trigger);
});

test("openReleaseNotesDialog closes when clicking the dimmed overlay", () => {
  const doc = createFakeDocument();
  const dialog = openReleaseNotesDialog({
    title: "Baluffo v0.1.2",
    markdown: "### Fixed\n- Modal works",
    documentTarget: doc,
    windowTarget: doc.defaultView,
  });

  assert.ok(dialog);
  dialog.overlay.dispatch("click", { target: dialog.overlay });
  assert.equal(dialog.overlay.parentNode, null);
});
