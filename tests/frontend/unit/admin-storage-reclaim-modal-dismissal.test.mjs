/**
 * The container reclaim modal is the only place a live run can be watched, so
 * ESC, a click on its scrim and a native dialog's `cancel` must not hide it:
 * doing so would look like a cancel that never happened. **Hide** and **Stop**
 * stay the exits. Each case drives the handlers `init()` binds, because the
 * guarantee is in those handlers, not in the markup.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { controller, payload, refs } from "./helpers/admin-storage-reclaim-fixtures.mjs";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

/** A modal element that records the listeners init() binds. */
function modalElement() {
  const listeners = new Map();
  const element = createElement({
    hidden: true,
    addEventListener(type, handler) {
      listeners.set(type, [...(listeners.get(type) || []), handler]);
    }
  });
  return {
    element,
    fire(type, event) {
      for (const handler of listeners.get(type) || []) handler(event);
    }
  };
}

/** Stands in for the document init() adds its keydown listener to. */
function documentKeydown() {
  const previous = globalThis.document;
  const handlers = [];
  globalThis.document = {
    addEventListener(type, handler) {
      if (type === "keydown") handlers.push(handler);
    }
  };
  return {
    press(event) {
      for (const handler of handlers) handler(event);
    },
    restore() {
      globalThis.document = previous;
    }
  };
}

function keyEvent(overrides = {}) {
  const calls = { prevented: 0, stopped: 0 };
  return {
    calls,
    key: "Escape",
    target: null,
    preventDefault() {
      calls.prevented += 1;
    },
    stopPropagation() {
      calls.stopped += 1;
    },
    ...overrides
  };
}

// A timer that never fires: these cases are about dismissal, not the follow-up read.
const stillClock = { setTimer: () => ({}), clearTimer: () => {} };

function controllerWithModal(target) {
  const modal = modalElement();
  target.adminStorageReclaimModalEl = modal.element;
  const { instance } = controller(target, { get: () => payload() }, stillClock);
  return { instance, modal };
}

test("ESC leaves a live run's modal on screen", () => {
  const target = refs();
  const { instance, modal } = controllerWithModal(target);
  const document = documentKeydown();
  try {
    instance.accept(
      payload({ scope: "modal", status: "running", phase: "copying", runId: "run-9", mode: "reclaim" })
    );
    instance.init();
    assert.equal(modal.element.hidden, false);

    const escape = keyEvent();
    document.press(escape);

    assert.equal(modal.element.hidden, false);
    assert.equal(escape.calls.prevented, 1);
    assert.equal(escape.calls.stopped, 1);
  } finally {
    document.restore();
  }
});

test("a click on the scrim does not hide a live run", () => {
  const target = refs();
  const { instance, modal } = controllerWithModal(target);
  instance.accept(
    payload({ scope: "modal", status: "running", phase: "copying", runId: "run-10", mode: "reclaim" })
  );
  instance.init();

  const scrimClick = keyEvent({ key: "", target: modal.element });
  modal.fire("click", scrimClick);

  assert.equal(modal.element.hidden, false);
  assert.equal(scrimClick.calls.prevented, 1);
});

test("a click inside the card is not a backdrop click", () => {
  const target = refs();
  const { instance, modal } = controllerWithModal(target);
  instance.accept(
    payload({ scope: "modal", status: "running", phase: "copying", runId: "run-11", mode: "reclaim" })
  );
  instance.init();

  const cardClick = keyEvent({ key: "", target: target.adminStorageReclaimModalBodyEl });
  modal.fire("click", cardClick);

  assert.equal(cardClick.calls.prevented, 0);
  assert.equal(modal.element.hidden, false);
});

test("a native dialog's cancel event is refused while the run is live", () => {
  const target = refs();
  const { instance, modal } = controllerWithModal(target);
  instance.accept(
    payload({ scope: "modal", status: "cancelling", phase: "copying", runId: "run-12", mode: "reclaim" })
  );
  instance.init();

  const cancel = keyEvent({ key: "" });
  modal.fire("cancel", cancel);

  assert.equal(cancel.calls.prevented, 1);
  assert.equal(modal.element.hidden, false);
});

test("with no modal up, ESC is not swallowed", () => {
  const target = refs();
  const { instance, modal } = controllerWithModal(target);
  const document = documentKeydown();
  try {
    instance.accept(payload({ status: "complete", phase: "complete", runId: "run-13", mode: "reclaim" }));
    instance.init();
    assert.equal(modal.element.hidden, true);

    const escape = keyEvent();
    document.press(escape);

    assert.equal(escape.calls.prevented, 0);
  } finally {
    document.restore();
  }
});
