// Stubs for the operations-history renderer tests.
//
// Run detail now lives in the inspector drawer, not in a panel inside the history
// container. The renderer cannot reach the drawer's buttons with
// `querySelectorAll` (the inspector controller paints it asynchronously), so the
// integration those tests exercise is: a row click stages its detail body on the
// row, and the drawer host gets one delegated click listener that resolves
// Copy/Abort.

export function makeRowStub(runKey, rowArea) {
  return {
    dataset: { runKey, rowArea },
    classList: { toggle() {}, add() {}, remove() {}, contains: () => false },
    getAttribute: (name) => (name === "data-run-key" ? runKey : ""),
    onclick: null,
    __runDetailHtml: ""
  };
}

// Row stubs are derived from the markup the renderer wrote, so a test exercises
// the real run keys instead of a hand-copied duplicate of them. They are cached
// because the renderer attaches its handlers to the objects this returns: deriving
// fresh stubs on every lookup would hand the test a different object than the one
// carrying `onclick`. Writing `innerHTML` invalidates the cache, which is what
// makes a second render produce fresh rows.
export function makeHistoryEl() {
  let markup = "";
  let cachedRows = null;
  return {
    textContent: "",
    dataset: {},
    get innerHTML() {
      return markup;
    },
    set innerHTML(value) {
      markup = String(value ?? "");
      cachedRows = null;
    },
    querySelectorAll(selector) {
      if (selector !== ".admin-ops-history-row[data-run-key]") return [];
      if (!cachedRows) {
        cachedRows = Array.from(
          markup.matchAll(/class="[^"]*admin-ops-history-row[^"]*"\s+data-row-area="([^"]*)"\s+data-run-key="([^"]*)"/g)
        ).map((match) => makeRowStub(match[2], match[1]));
      }
      return cachedRows;
    },
    querySelector() {
      return null;
    }
  };
}

export function rowsFor(historyEl) {
  return historyEl.querySelectorAll(".admin-ops-history-row[data-run-key]");
}

// Drives the row's own selection handler, which is what stages `__runDetailHtml`.
export function clickRow(rowEl) {
  rowEl.onclick({ target: { closest: () => null }, preventDefault() {} });
}

export function createDetailHostStub() {
  const listeners = [];
  return {
    innerHTML: "",
    addEventListener(type, handler) {
      if (type === "click") listeners.push(handler);
    },
    click(target) {
      listeners.forEach((handler) => handler({ target, stopPropagation() {}, preventDefault() {} }));
    }
  };
}

// Stands in for a Copy or Abort button inside the drawer: `closest` is what the
// delegated listener matches on, and `getAttribute` is where it reads the run key.
export function makeDetailButton(attribute, key) {
  const button = {
    getAttribute: (name) => (name === attribute ? key : ""),
    closest: (selector) => (selector.includes(attribute) ? button : null)
  };
  return button;
}
