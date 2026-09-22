// The inspector drawer is the single home for run detail. Two regressions are
// pinned here because both were silent: the overlay shipped with `hidden`, which
// is `display: none !important` and outranks the visible class, and nothing ever
// removed it — so the drawer rendered correctly and never appeared.

import test from "node:test";
import assert from "node:assert/strict";

import { createAdminInspectorController } from "../../../frontend/admin/app/inspector.js";
import { createClassList } from "./helpers/dom-test-helpers.mjs";

function createOverlayStub() {
  return {
    classList: createClassList(["inspector-overlay", "hidden"]),
    addEventListener() {},
    _boundClose: false
  };
}

function createPanelStub() {
  return { classList: createClassList(["inspector-panel"]) };
}

function createContentStub() {
  return { innerHTML: "", querySelectorAll: () => [] };
}

function createController() {
  const overlay = createOverlayStub();
  const panel = createPanelStub();
  const content = createContentStub();
  const title = { textContent: "" };
  const controller = createAdminInspectorController({
    refs: {
      inspectorOverlayEl: overlay,
      inspectorPanelEl: panel,
      inspectorContentEl: content,
      inspectorTitleEl: title,
      inspectorCloseBtnEl: null
    },
    _getBridge: () => null,
    postBridge: async () => ({}),
    showToast: () => {},
    _logAdminError: () => {}
  });
  return { controller, overlay, panel, content, title };
}

test("opening the drawer clears the hidden class that would suppress it", () => {
  const { controller, overlay, panel, content, title } = createController();
  assert.equal(overlay.classList.contains("hidden"), true, "precondition: admin.html ships hidden");

  controller.openInspector("task_run", { id: "fetch_1", html: "<p>detail</p>" });

  assert.equal(overlay.classList.contains("hidden"), false, "hidden must come off or the drawer stays invisible");
  assert.equal(overlay.classList.contains("inspector-overlay-visible"), true);
  assert.equal(panel.classList.contains("inspector-panel-visible"), true);
  assert.equal(controller.isOpen(), true);
  assert.equal(content.innerHTML, "<p>detail</p>");
  assert.equal(title.textContent, "Task Run");
});

test("closing the drawer restores the hidden class it started with", () => {
  const { controller, overlay, panel } = createController();
  controller.openInspector("task_run", { id: "fetch_1", html: "<p>detail</p>" });
  controller.closeInspector();

  assert.equal(overlay.classList.contains("hidden"), true);
  assert.equal(overlay.classList.contains("inspector-overlay-visible"), false);
  assert.equal(panel.classList.contains("inspector-panel-visible"), false);
  assert.equal(controller.isOpen(), false);
});

// A rich body owns its own header and action row. Wrapping it in the generic
// shell would double the header and append a second set of action buttons.
test("a rich body bypasses the generic entity shell", () => {
  const { controller, content } = createController();
  controller.openInspector("task_run", { id: "fetch_1", html: "<div class=\"admin-ops-run-detail\">x</div>" });

  assert.match(content.innerHTML, /admin-ops-run-detail/);
  assert.doesNotMatch(content.innerHTML, /inspector-entity-header/);
  assert.doesNotMatch(content.innerHTML, /inspector-actions/);
});

test("an entity without a rich body still gets the generic shell", () => {
  const { controller, content } = createController();
  controller.openInspector("task_run", {
    id: "fetch_1",
    typeText: "Fetch",
    statusText: "Running",
    statusClass: "healthy"
  });

  assert.match(content.innerHTML, /inspector-entity-header/);
  assert.match(content.innerHTML, /Fetch/);
});
