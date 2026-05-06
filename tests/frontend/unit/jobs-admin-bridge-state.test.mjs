import test from "node:test";
import assert from "node:assert/strict";

import { applyJobsAdminBridgeState } from "../../../frontend/jobs/app/admin-bridge-state.js";

function createClassList(initial = []) {
  const values = new Set(initial);
  return {
    add(...tokens) {
      tokens.forEach(token => values.add(token));
    },
    remove(...tokens) {
      tokens.forEach(token => values.delete(token));
    },
    contains(token) {
      return values.has(token);
    }
  };
}

function createButton() {
  return {
    textContent: "",
    title: "",
    disabled: false,
    dataset: {},
    classList: createClassList(),
    attributes: {},
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    }
  };
}

test("jobs admin bridge state keeps checking, offline, and online states visible", () => {
  const runtimeState = { adminBridgeButtonState: "checking" };
  const buttonEl = createButton();

  applyJobsAdminBridgeState({
    buttonEl,
    state: "checking",
    label: "Admin Checking...",
    title: "Checking admin bridge status",
    runtimeState
  });
  assert.equal(runtimeState.adminBridgeButtonState, "checking");
  assert.equal(buttonEl.classList.contains("hidden"), false);
  assert.equal(buttonEl.classList.contains("checking"), true);
  assert.equal(buttonEl.textContent, "Admin Checking...");
  assert.equal(buttonEl.disabled, true);
  assert.equal(buttonEl.attributes["aria-disabled"], "true");

  applyJobsAdminBridgeState({
    buttonEl,
    state: "offline",
    label: "Admin Offline",
    title: "Admin bridge is offline",
    runtimeState
  });
  assert.equal(runtimeState.adminBridgeButtonState, "offline");
  assert.equal(buttonEl.classList.contains("hidden"), false);
  assert.equal(buttonEl.classList.contains("offline"), true);
  assert.equal(buttonEl.textContent, "Admin Offline");
  assert.equal(buttonEl.disabled, true);

  applyJobsAdminBridgeState({
    buttonEl,
    state: "online",
    label: "Admin Online",
    title: "Open admin panel",
    runtimeState
  });
  assert.equal(runtimeState.adminBridgeButtonState, "online");
  assert.equal(buttonEl.classList.contains("hidden"), false);
  assert.equal(buttonEl.classList.contains("online"), true);
  assert.equal(buttonEl.textContent, "Admin Online");
  assert.equal(buttonEl.disabled, false);
  assert.equal(buttonEl.attributes["aria-disabled"], "false");
});
