import { createClassList } from "./dom-test-helpers.mjs";

export function createElement(overrides = {}) {
  const listeners = new Map();
  return {
    listeners,
    innerHTML: "",
    textContent: "",
    disabled: false,
    hidden: false,
    title: "",
    value: "",
    dataset: {},
    attributes: {},
    classList: createClassList(),
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    getAttribute(name) {
      return this.attributes[name];
    },
    addEventListener(type, handler) {
      const handlers = listeners.get(type) || [];
      handlers.push(handler);
      listeners.set(type, handlers);
    },
    dispatch(type, event = {}) {
      for (const handler of listeners.get(type) || []) {
        handler(event);
      }
    },
    querySelectorAll() {
      return [];
    },
    contains() {
      return false;
    },
    closest() {
      return null;
    },
    ...overrides
  };
}

export function buildDesktopUpdateRefs(createElement) {
  return {
    desktopUpdateToggleBtn: createElement("Check updates"),
    desktopUpdatePanel: createElement(),
    desktopUpdateTitle: createElement(),
    desktopUpdateBody: createElement(),
    desktopUpdateMeta: createElement(),
    desktopUpdateProgress: createElement(),
    desktopUpdatePrimaryBtn: createElement(),
    desktopUpdateSecondaryBtn: createElement(),
    desktopUpdateReleaseNotes: createElement(),
  };
}
