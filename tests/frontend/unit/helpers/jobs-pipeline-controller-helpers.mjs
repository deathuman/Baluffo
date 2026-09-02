import { createJobsPipelineController } from "../../../../frontend/jobs/app/runtime/pipeline-controller.js";
import { createJobsPipelineUiState } from "../../../../frontend/jobs/app/runtime/state.js";

function createClassList() {
  const values = new Set();
  return {
    toggle(name, enabled) {
      if (enabled) values.add(name);
      else values.delete(name);
    },
    contains(name) {
      return values.has(name);
    }
  };
}

function createStyle() {
  const values = new Map();
  return {
    setProperty(name, value) {
      values.set(name, value);
      this[name] = value;
    },
    removeProperty(name) {
      values.delete(name);
      delete this[name];
    },
    getPropertyValue(name) {
      return values.get(name) || "";
    }
  };
}

function createElementMock(tagName) {
  return {
    tagName: String(tagName || "").toUpperCase(),
    dataset: {},
    style: createStyle(),
    className: "",
    textContent: "",
    hidden: false,
    classList: createClassList(),
    setAttribute(name, value) {
      this[name] = value;
    },
    removeAttribute(name) {
      delete this[name];
    }
  };
}

export function createButtonMock(textContent = "Update jobs") {
  const listeners = new Map();
  const siblings = [];
  const button = {
    dataset: {},
    style: createStyle(),
    disabled: false,
    classList: createClassList(),
    _textContent: textContent,
    ownerDocument: { createElement: createElementMock },
    children: [],
    get textContent() {
      return this.children.length
        ? this.children.map(child => String(child?.textContent || "")).join("")
        : String(this._textContent || "");
    },
    set textContent(value) {
      this._textContent = String(value);
      this.children = [];
    },
    replaceChildren(...nodes) {
      this.children = nodes;
    },
    querySelector(selector) {
      const match = /^\[data-ui="([^"]+)"\]$/.exec(String(selector || ""));
      if (!match) return null;
      return this.children.find(child => String(child?.dataset?.ui || "") === match[1]) || null;
    },
    setAttribute(name, value) {
      this[name] = value;
    },
    removeAttribute(name) {
      delete this[name];
    },
    addEventListener(name, handler) {
      const key = String(name || "");
      listeners.set(key, [...(listeners.get(key) || []), handler]);
    },
    dispatch(name, event = {}) {
      for (const handler of listeners.get(String(name || "")) || []) {
        handler({ target: this, ...event });
      }
    }
  };
  // ponytail: sibling elements created by the pipeline UI (the abort affordance
  // and the sub-progress caption) land in parentElement.children, mirroring how
  // the real toolbar DOM collects them after the button.
  button.parentElement = {
    querySelector(selector) {
      const match = /^\[data-ui="([^"]+)"\]$/.exec(String(selector || ""));
      if (!match) return null;
      return siblings.find(child => String(child?.dataset?.ui || "") === match[1]) || null;
    }
  };
  button.insertAdjacentElement = (_position, element) => {
    siblings.push(element);
  };
  return button;
}

// Returns the sub-progress caption rendered as a sibling of the Update-jobs
// button, or null if none has been created. Tests assert the live counts/ETA
// live here (full-width, wrapping) rather than in the clamped in-button span.
export function getJobsPipelineProgressCaption(button) {
  return button?.parentElement?.querySelector?.('[data-ui="jobs-pipeline-progress-caption"]') || null;
}

export function installFakeTimers() {
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  globalThis.setTimeout = callback => ({ callback });
  globalThis.clearTimeout = () => {};
  return () => {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
  };
}

export { createJobsPipelineController, createJobsPipelineUiState };
