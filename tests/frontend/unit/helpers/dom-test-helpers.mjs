// Shared DOM/element stubs for frontend unit tests (W1b consolidation).

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

export function createStyleStub() {
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

export function createRenderEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

export function createToggleClassList() {
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

export function createClickableButton(dataset = {}) {
  let clickHandler = null;
  return {
    dataset,
    addEventListener(type, handler) {
      if (type === "click") clickHandler = handler;
    },
    click() {
      if (clickHandler) clickHandler();
    }
  };
}

export function ruleBodies(source, selector) {
  const bodies = [];
  for (const match of source.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    const selectors = match[1].split(",").map((part) => part.trim());
    if (selectors.includes(selector)) bodies.push(match[2]);
  }
  return bodies;
}

export function createButtonMapEl(buttonsBySelector = {}) {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: selector => buttonsBySelector[selector] || []
  };
}

export function createWindowStub({ innerHeight = 900, innerWidth = 1200 } = {}) {
  const listeners = new Map();
  return {
    innerHeight,
    innerWidth,
    listeners,
    addEventListener(type, handler) {
      const handlers = listeners.get(type) || [];
      handlers.push(handler);
      listeners.set(type, handlers);
    }
  };
}

export function createBeforeUnloadEvent() {
  let prevented = false;
  return {
    preventDefault() {
      prevented = true;
    },
    get defaultPrevented() {
      return prevented;
    },
    returnValue: undefined
  };
}

export function createAttrButton(attrs) {
  return {
    getAttribute(name) {
      return attrs[name] || "";
    },
    addEventListener(_event, handler) {
      this.click = handler;
    }
  };
}

export function createSelectorEl(buttonsBySelector = {}) {
  return {
    innerHTML: "",
    dataset: {},
    querySelectorAll(selector) {
      return buttonsBySelector[selector] || [];
    }
  };
}

export function createDatasetButton(dataset) {
  return {
    dataset,
    addEventListener(_event, handler) {
      this.click = handler;
    }
  };
}
