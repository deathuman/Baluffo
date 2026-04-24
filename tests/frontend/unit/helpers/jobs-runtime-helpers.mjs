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
