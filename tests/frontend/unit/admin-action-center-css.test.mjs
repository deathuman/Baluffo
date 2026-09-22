// Guards for the Action Center panel styles.
//
// Split from admin-css-guards.test.mjs when that file crossed the 400-line test
// budget. The panel's regression risk is specific enough to own a file: it used to
// render all five states through one function, so "All systems operational" and
// "Operational signals unavailable" produced identical styling.

import assert from "node:assert/strict";
import test from "node:test";

import { ruleBodies } from "./helpers/dom-test-helpers.mjs";
import { css, mediaBlock } from "./helpers/admin-css-fixtures.mjs";

test("action center keeps its first-paint reservation and per-state tone", () => {
  // The panel reserves height before the first poll resolves, so the page below it
  // does not jump when signals arrive. startup-css-performance.test.mjs only asserts
  // the property is *present*, which a `min-height: 0` would also satisfy — so the
  // value is parsed here. A `/min-height:\s*(?!0)/` lookahead is not enough: `\s*`
  // backtracks to zero width and the lookahead then inspects the space, not the `0`.
  const items = ruleBodies(css, ".action-center-items");
  assert.ok(items.length, ".action-center-items must have a rule");
  const reservations = items
    .flatMap((body) => [...body.matchAll(/min-height:\s*([^;]+);/g)])
    .map((match) => match[1].trim());
  assert.ok(reservations.length, ".action-center-items must declare a min-height");
  assert.ok(
    reservations.some((value) => Number.parseFloat(value) > 0),
    `the pre-poll height reservation must be non-zero, got: ${reservations.join(", ")}`
  );
});

test("action center gives each state its own accent", () => {
  // If these collapse back into one shared treatment, a passing check becomes
  // visually indistinguishable from a failure — the defect this replaced.
  for (const [tone, token] of [["ok", "--tint-ok"], ["warning", "--tint-warning"]]) {
    const bodies = ruleBodies(css, `.action-center-status-${tone}`);
    assert.ok(bodies.length, `.action-center-status-${tone} must exist`);
    assert.match(bodies.join("\n"), new RegExp(token), `${tone} must use ${token}`);
  }

  // The healthy and unavailable states must not resolve to the same declarations.
  const okBody = ruleBodies(css, ".action-center-status-ok").join("\n");
  const warnBody = ruleBodies(css, ".action-center-status-warning").join("\n");
  assert.notEqual(okBody, warnBody, "healthy and warning states must not share a treatment");

  // Signal severity is expressed on the row, not only in the chip.
  assert.ok(ruleBodies(css, ".action-center-signal-critical").length, "critical signal tone must exist");
  assert.ok(ruleBodies(css, ".action-center-signal-warning").length, "warning signal tone must exist");
});

test("action center drops the old shared item shape and unreachable view-all rule", () => {
  assert.equal(ruleBodies(css, ".action-center-item").length, 0, "the old shared item rule must be gone");
  assert.equal(ruleBodies(css, ".action-center-footer").length, 0, "the footer copy-button rule must be gone");
  // The overflow row was unreachable by construction (see the renderer comment), so
  // its styles must not outlive it.
  assert.equal(ruleBodies(css, ".action-center-view-all").length, 0, "the unreachable view-all rule must be gone");
});

test("action center signal grid stacks on narrow viewports", () => {
  // Three columns (chip, message, actions) fit the card, but a long message must not
  // squeeze the actions out of it on a phone. Below 520px the grid collapses.
  const narrow = mediaBlock("(max-width: 520px)");
  const stacked = ruleBodies(narrow, ".action-center-signal");
  assert.ok(stacked.length, "the narrow signal grid must exist");
  assert.match(stacked.join("\n"), /grid-template-columns:\s*minmax\(0,\s*1fr\)/);
});

test("action center icon sizing stays inside the button", () => {
  // The action buttons carry an SVG plus a text label; without an explicit size the
  // icon renders at the SVG's intrinsic 16px and overflows the small button.
  const icon = ruleBodies(css, ".action-center-signal-btn .action-center-icon");
  assert.ok(icon.length, "the in-button icon size rule must exist");
  assert.match(icon.join("\n"), /width:/);
  assert.match(icon.join("\n"), /height:/);
});
