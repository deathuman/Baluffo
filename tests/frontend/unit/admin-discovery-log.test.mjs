import test from "node:test";

import assert from "node:assert/strict";

import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";

import { createDiscoveryControllerFixture } from "./helpers/admin-controller-test-helpers.mjs";

test("admin discovery log holds back a half-written line until its newline lands", () => {
  const rows = [];
  const fixture = createDiscoveryControllerFixture();
  // A real client-clock stamp is what made truncated rows read as out-of-order.
  fixture.options.createLogEvent = (scope, message, level) => ({ scope, message, level, timestamp: "CLIENT-CLOCK" });
  fixture.options.appendLogRow = (_container, event) => rows.push(event);
  const controller = createAdminDiscoveryController(fixture.options);

  controller.appendDiscoveryServerLogText(
    "[2026-03-08T10:01:01.000Z] GameDevMap active dry run careers recovery fetch wave 1: fetched 1300/1515"
  );
  assert.equal(rows.length, 0, "an unterminated line must not be rendered on its own");

  controller.appendDiscoveryServerLogText(
    " pages.\n[2026-03-08T10:01:02.000Z] GameDevMap active dry run careers recovery fetch wave 1: fetched 1325/1515 pages.\n"
  );

  assert.deepEqual(rows.map(row => row.message), [
    "GameDevMap active dry run careers recovery fetch wave 1: fetched 1300/1515 pages.",
    "GameDevMap active dry run careers recovery fetch wave 1: fetched 1325/1515 pages."
  ]);
  assert.deepEqual(rows.map(row => row.timestamp), [
    "2026-03-08T10:01:01.000Z",
    "2026-03-08T10:01:02.000Z"
  ]);
  assert.ok(rows.every(row => row.timestamp !== "CLIENT-CLOCK"));
});

test("admin discovery log drops a carried fragment when the log is reset", () => {
  const rows = [];
  const fixture = createDiscoveryControllerFixture();
  fixture.options.createLogEvent = (scope, message, level) => ({ scope, message, level, timestamp: "CLIENT-CLOCK" });
  fixture.options.appendLogRow = (_container, event) => rows.push(event);
  const controller = createAdminDiscoveryController(fixture.options);

  controller.appendDiscoveryServerLogText("[2026-03-08T10:01:01.000Z] stale half-line");
  controller.setDiscoveryLogPlaceholder("Discovery log cleared.");
  controller.appendDiscoveryServerLogText(" carried over\n");

  assert.ok(
    rows.every(row => !/stale half-line/.test(row.message)),
    `carry survived the reset: ${JSON.stringify(rows.map(row => row.message))}`
  );
  assert.ok(rows.some(row => row.message === "carried over"));
});
