// A probe-wave receipt measures one wave, not the lane. The Ops row used to print a `clean`
// chip from whatever receipt was on disk — the 0.3.064 container read "clean" from a wave
// that had run 16 days earlier — so a lane nobody had exercised since looked verified.
import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminOpsKpis } from "../../../frontend/admin/render.js";
import { createRenderEl as makeEl } from "./helpers/dom-test-helpers.mjs";

const DAY_MS = 24 * 60 * 60 * 1000;

function receipt(overrides = {}) {
  return {
    batches: 1,
    fetchesStarted: 17,
    fetchesBoundedOut: 0,
    fetchesOutstandingAtExit: 0,
    clientFetchesOutstandingAtExit: 0,
    clientsUnclosed: 0,
    deadlineExits: 0,
    ...overrides
  };
}

function render(probeLifecycle) {
  const el = makeEl();
  renderAdminOpsKpis(el, { probeLifecycle }, "healthy", {});
  return el.innerHTML;
}

test("a fresh receipt is still clean and prints when the wave ran", () => {
  const html = render(receipt({ finishedAt: new Date(Date.now() - 60 * 60 * 1000).toISOString() }));

  assert.match(html, /admin-status-chip healthy">clean</);
  assert.match(html, /last probe wave finished/);
});

test("an old receipt stops claiming the lane is clean", () => {
  const finishedAt = new Date(Date.now() - 30 * DAY_MS).toISOString();
  const html = render(receipt({ finishedAt }));

  assert.match(html, /admin-status-chip warning">stale receipt</);
  assert.doesNotMatch(html, />clean</);
  // The date is the point: an operator can see the measurement is a month old.
  assert.match(html, /last probe wave finished \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC/);
  assert.match(html, /\(30d ago\)/);
});

test("a receipt with no date is undated, not clean", () => {
  const html = render(receipt());

  assert.match(html, /admin-status-chip warning">receipt undated</);
  assert.match(html, /carries no finish time/);
  assert.doesNotMatch(html, />clean</);
});

test("a modification time dates a receipt that carries no finish time", () => {
  const html = render(receipt({ modifiedAt: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString() }));

  assert.match(html, /admin-status-chip healthy">clean</);
  assert.match(html, /\(2h ago\)/);
});

test("an unclosed client still outranks an old receipt", () => {
  const html = render(receipt({
    clientsUnclosed: 1,
    finishedAt: new Date(Date.now() - 30 * DAY_MS).toISOString()
  }));

  assert.match(html, /admin-status-chip critical">client left unclosed</);
});
