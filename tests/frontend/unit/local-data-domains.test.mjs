import test from "node:test";
import assert from "node:assert/strict";
import { createAuthDomain } from "../../../frontend/local-data/auth.js";
import {
  requestConfirmationDialog,
  requestTextInputDialog
} from "../../../frontend/local-data/profile-name-dialog.js";
import { createSavedJobsDomain } from "../../../frontend/local-data/saved-jobs.js";

function createStorageMock() {
  const map = new Map();
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

test("auth domain signIn/signOut updates session and emits auth changes", async () => {
  const listeners = new Set();
  const profiles = [];
  let currentUser = null;
  const localStorage = createStorageMock();
  let promptCalls = 0;
  global.localStorage = localStorage;
  global.window = {
    prompt: () => {
      promptCalls += 1;
      return "Andrea";
    },
    addEventListener: () => {}
  };

  const authDomain = createAuthDomain({
    listeners,
    getCurrentUser: () => currentUser,
    setCurrentUser: value => {
      currentUser = value;
    },
    makeUser: profile => ({ uid: profile.id, displayName: profile.name, email: profile.email || "" }),
    readProfiles: () => profiles.slice(),
    writeProfiles: next => {
      profiles.length = 0;
      profiles.push(...next);
    },
    hashFNV1a: () => "abcd1234",
    sessionKey: "session_key"
  });

  const observed = [];
  const unsub = authDomain.onAuthStateChanged(user => {
    observed.push(user ? user.uid : "");
  });

  const result = await authDomain.signIn();
  assert.equal(promptCalls, 1);
  assert.equal(result.user.uid, "local_andrea");
  assert.equal(localStorage.getItem("session_key"), "local_andrea");
  assert.equal(profiles.length, 1);
  assert.equal(observed.at(-1), "local_andrea");

  await authDomain.signOut();
  assert.equal(localStorage.getItem("session_key"), null);
  assert.equal(observed.at(-1), "");
  unsub();
});

test("confirmation dialog falls back to window.confirm outside the browser DOM", async () => {
  let confirmCalls = 0;
  global.window = {
    confirm: message => {
      confirmCalls += 1;
      return /cannot be undone/i.test(String(message || ""));
    }
  };

  const result = await requestConfirmationDialog({
    title: "Delete selected sources?",
    description: "Delete 2 selected source(s) from registry? This cannot be undone."
  });

  assert.equal(result, true);
  assert.equal(confirmCalls, 1);
});

test("text input dialog becomes visible, focuses the field, and restores trigger focus on cancel", async () => {
  const previousDocument = global.document;
  const previousWindow = global.window;
  const doc = createFakeDocument();
  const trigger = doc.createElement("button");
  doc.body.appendChild(trigger);
  trigger.focus();
  global.document = doc;
  global.window = doc.defaultView;

  try {
    const pending = requestTextInputDialog({
      title: "Sign in",
      label: "Profile name",
      defaultValue: "Andrea"
    });

    const overlay = doc.find(node => typeof node.className === "string" && node.className.includes("popup-overlay"));
    const panel = doc.find(node => typeof node.className === "string" && node.className.includes("popup "));
    const input = doc.find(node => node.tagName === "INPUT");
    const cancelBtn = doc.find(
      node => typeof node.className === "string" && node.className.includes("local-auth-dialog-cancel")
    );

    assert.ok(overlay);
    assert.ok(panel);
    assert.match(overlay.className, /\bpopup-overlay-visible\b/);
    assert.match(panel.className, /\bpopup-visible\b/);
    assert.equal(doc.activeElement, input);
    assert.equal(input.selected, true);

    cancelBtn.dispatch("click");
    const result = await pending;
    assert.equal(result, null);
    assert.equal(doc.activeElement, trigger);
  } finally {
    global.document = previousDocument;
    global.window = previousWindow;
  }
});

test("confirmation dialog becomes visible, focuses confirm, and resolves on enter", async () => {
  const previousDocument = global.document;
  const previousWindow = global.window;
  const doc = createFakeDocument();
  const trigger = doc.createElement("button");
  doc.body.appendChild(trigger);
  trigger.focus();
  global.document = doc;
  global.window = doc.defaultView;

  try {
    const pending = requestConfirmationDialog({
      title: "Delete selected sources?",
      description: "Delete 2 selected source(s) from registry? This cannot be undone."
    });

    const overlay = doc.find(node => typeof node.className === "string" && node.className.includes("popup-overlay"));
    const panel = doc.find(node => typeof node.className === "string" && node.className.includes("popup "));
    const confirmBtn = doc.find(
      node => typeof node.className === "string" && node.className.includes("local-auth-dialog-submit")
    );

    assert.ok(overlay);
    assert.ok(panel);
    assert.match(overlay.className, /\bpopup-overlay-visible\b/);
    assert.match(panel.className, /\bpopup-visible\b/);
    assert.equal(doc.activeElement, confirmBtn);

    doc.dispatch("keydown", { key: "Enter" });
    const result = await pending;
    assert.equal(result, true);
    assert.equal(doc.activeElement, trigger);
  } finally {
    global.document = previousDocument;
    global.window = previousWindow;
  }
});

test("saved-jobs domain normalizes bookmark timestamp and merge keeps richer existing row", () => {
  const savedJobsDomain = createSavedJobsDomain({
    withStore: async () => {
      throw new Error("withStore not expected");
    },
    listSavedJobs: async () => [],
    ensureCurrentUser: () => ({ uid: "u1" }),
    notifySavedJobsChanged: async () => {},
    addActivityLog: async () => {},
    generateJobKey: input => String(input?.jobKey || "job_x"),
    normalizeApplicationStatus: status => String(status || "bookmark"),
    canTransitionPhase: () => true,
    normalizeSectorValue: value => String(value || "Tech"),
    normalizeCustomSourceLabel: value => String(value || "Personal"),
    sanitizeJobUrl: value => String(value || ""),
    nowIso: () => "2026-03-08T12:00:00.000Z",
    normalizeIsoOrNow: (value, fallback = "") => String(value || fallback),
    toPlainObject: value => (value && typeof value === "object" && !Array.isArray(value) ? value : {}),
    isClearlyLowerQualityImported: () => true
  });

  const normalized = savedJobsDomain.normalizeSavedJobRecord("u1", {
    jobKey: "job_1",
    title: "Role",
    company: "Studio"
  });
  assert.equal(normalized.phaseTimestamps.bookmark, "2026-03-08T12:00:00.000Z");
  assert.equal(normalized.jobKey, "job_1");
  assert.equal(normalized.profileId, "u1");

  const existing = savedJobsDomain.normalizeSavedJobRecord("u1", {
    jobKey: "job_1",
    title: "Senior Role",
    company: "Studio"
  });
  const merged = savedJobsDomain.mergeSavedJobRows("u1", existing, {
    jobKey: "job_1",
    title: "",
    company: ""
  });
  assert.equal(merged.title, existing.title);
  assert.equal(merged.company, existing.company);
});

test("saved-jobs domain sanitizes city and country on read and write", async () => {
  const writes = [];
  const savedJobsDomain = createSavedJobsDomain({
    withStore: async (_storeName, _mode, fn) => {
      const store = {
        get() {
          return {
            result: null,
            onerror: null,
            set onsuccess(handler) {
              setTimeout(() => handler(), 0);
            }
          };
        },
        put(row) {
          writes.push(row);
          return {
            onerror: null,
            set onsuccess(handler) {
              setTimeout(() => handler(), 0);
            }
          };
        }
      };
      await new Promise((resolve, reject) => fn(store, resolve, reject));
    },
    listSavedJobs: async () => [],
    ensureCurrentUser: () => ({ uid: "u1" }),
    notifySavedJobsChanged: async () => {},
    addActivityLog: async () => {},
    generateJobKey: input => String(input?.jobKey || "job_x"),
    normalizeApplicationStatus: status => String(status || "bookmark"),
    canTransitionPhase: () => true,
    normalizeSectorValue: value => String(value || "Tech"),
    normalizeCustomSourceLabel: value => String(value || "Personal"),
    sanitizeJobUrl: value => String(value || ""),
    nowIso: () => "2026-03-08T12:00:00.000Z",
    normalizeIsoOrNow: (value, fallback = "") => String(value || fallback),
    toPlainObject: value => (value && typeof value === "object" && !Array.isArray(value) ? value : {}),
    isClearlyLowerQualityImported: () => false
  });

  const normalized = savedJobsDomain.normalizeSavedJobRecord("u1", {
    jobKey: "job_1",
    city: "A bachelor's degree in digital communications",
    country: "Japan"
  });
  assert.equal(normalized.city, "");
  assert.equal(normalized.country, "Japan");

  await savedJobsDomain.saveJobForUser("u1", {
    title: "Role",
    company: "Studio",
    city: "2026",
    country: "Japan",
    jobLink: "https://example.com/job"
  });

  assert.equal(writes.length, 1);
  assert.equal(writes[0].city, "");
  assert.equal(writes[0].country, "Japan");
});

test("saved-jobs domain updateJobNotes does not mutate updatedAt ordering field", async () => {
  const existingRow = {
    pk: "u1::job_1",
    profileId: "u1",
    jobKey: "job_1",
    title: "Role",
    company: "Studio",
    notes: "old",
    updatedAt: "2026-03-08T12:00:00.000Z"
  };
  const writes = [];
  let notifyCount = 0;

  const savedJobsDomain = createSavedJobsDomain({
    withStore: async (_storeName, _mode, fn) => {
      const store = {
        get() {
          return {
            result: existingRow,
            onerror: null,
            set onsuccess(handler) {
              this._onsuccess = handler;
              setTimeout(() => handler(), 0);
            }
          };
        },
        put(row) {
          writes.push(row);
          return {
            onerror: null,
            set onsuccess(handler) {
              setTimeout(() => handler(), 0);
            }
          };
        }
      };
      await new Promise((resolve, reject) => fn(store, resolve, reject));
    },
    listSavedJobs: async () => [],
    ensureCurrentUser: () => ({ uid: "u1" }),
    notifySavedJobsChanged: async () => {
      notifyCount += 1;
    },
    addActivityLog: async () => {},
    generateJobKey: input => String(input?.jobKey || "job_x"),
    normalizeApplicationStatus: status => String(status || "bookmark"),
    canTransitionPhase: () => true,
    normalizeSectorValue: value => String(value || "Tech"),
    normalizeCustomSourceLabel: value => String(value || "Personal"),
    sanitizeJobUrl: value => String(value || ""),
    nowIso: () => "2030-01-01T00:00:00.000Z",
    normalizeIsoOrNow: (value, fallback = "") => String(value || fallback),
    toPlainObject: value => (value && typeof value === "object" && !Array.isArray(value) ? value : {}),
    isClearlyLowerQualityImported: () => false
  });

  await savedJobsDomain.updateJobNotes("u1", "job_1", "new note");

  assert.equal(writes.length, 1);
  assert.equal(writes[0].notes, "new note");
  assert.equal(writes[0].updatedAt, "2026-03-08T12:00:00.000Z");
  assert.equal(notifyCount, 1);
});
