export function createClassList(initial = []) {
  const values = new Set(initial);
  return {
    add(...tokens) {
      tokens.forEach(token => values.add(token));
    },
    remove(...tokens) {
      tokens.forEach(token => values.delete(token));
    },
    toggle(token, force) {
      if (force === undefined) {
        if (values.has(token)) {
          values.delete(token);
          return false;
        }
        values.add(token);
        return true;
      }
      if (force) {
        values.add(token);
        return true;
      }
      values.delete(token);
      return false;
    },
    contains(token) {
      return values.has(token);
    }
  };
}

export function createElement(overrides = {}) {
  return {
    textContent: "",
    title: "",
    disabled: false,
    value: "",
    dataset: {},
    classList: createClassList(),
    attributes: {},
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    getAttribute(name) {
      return this.attributes[name];
    },
    focus() {},
    querySelector() {
      return null;
    },
    querySelectorAll() {
      return [];
    },
    ...overrides
  };
}

export function createButton(overrides = {}) {
  return createElement(overrides);
}
