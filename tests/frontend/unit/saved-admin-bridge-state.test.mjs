import test from "node:test";
import assert from "node:assert/strict";

import { applySavedAdminBridgeState } from "../../../frontend/saved/app/admin-bridge-state.js";

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

test("saved admin bridge state reflects checking, offline, and online states", () => {
  const viewState = { adminBridgeButtonState: "checking" };
  const buttonEl = createButton();

  applySavedAdminBridgeState({
    buttonEl,
    state: "checking",
    label: "Admin Checking...",
    title: "Checking admin bridge status",
    viewState
  });
  assert.equal(viewState.adminBridgeButtonState, "checking");
  assert.equal(buttonEl.classList.contains("checking"), true);
  assert.equal(buttonEl.disabled, true);
  assert.equal(buttonEl.attributes["aria-disabled"], "true");

  applySavedAdminBridgeState({
    buttonEl,
    state: "offline",
    label: "Admin Offline",
    title: "Admin bridge is offline",
    viewState
  });
  assert.equal(viewState.adminBridgeButtonState, "offline");
  assert.equal(buttonEl.classList.contains("offline"), true);
  assert.equal(buttonEl.textContent, "Admin Offline");
  assert.equal(buttonEl.disabled, true);

  applySavedAdminBridgeState({
    buttonEl,
    state: "online",
    label: "Admin Online",
    title: "Open admin panel",
    viewState
  });
  assert.equal(viewState.adminBridgeButtonState, "online");
  assert.equal(buttonEl.classList.contains("online"), true);
  assert.equal(buttonEl.textContent, "Admin Online");
  assert.equal(buttonEl.disabled, false);
  assert.equal(buttonEl.attributes["aria-disabled"], "false");
});
