export function createStorageMock(seed = {}) {
  const map = new Map(Object.entries(seed).map(([key, value]) => [String(key), String(value)]));
  return {
    getItem(key) {
      return map.has(key) ? map.get(key) : null;
    },
    setItem(key, value) {
      map.set(String(key), String(value));
    },
    removeItem(key) {
      map.delete(String(key));
    }
  };
}

export async function importFresh(specifier, { relativeTo = import.meta.url } = {}) {
  const resolvedUrl = new URL(specifier, relativeTo);
  resolvedUrl.searchParams.set("t", `${Date.now()}_${Math.random()}`);
  return import(resolvedUrl.href);
}

export function createFakeDocument() {
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
    return {
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
      value: "",
      name: "",
      placeholder: "",
      required: false,
      autocomplete: "",
      maxLength: 0,
      selected: false,
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
      replaceChildren(...nodes) {
        this.children.forEach(child => {
          child.parentNode = null;
        });
        this.children = [];
        this._textContent = "";
        if (nodes.length) {
          this.append(...nodes);
        }
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
        if (name === "name") this.name = String(value);
        if (name === "type") this.type = String(value);
        if (name === "placeholder") this.placeholder = String(value);
        if (name === "autocomplete") this.autocomplete = String(value);
        if (name === "maxlength") this.maxLength = Number(value);
      },
      focus() {
        doc.activeElement = this;
      },
      select() {
        this.selected = true;
      }
    };
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
