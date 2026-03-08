import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizeLogLevel,
  createLogEvent,
  mergeSourceStatusFromReport,
  applySourceFilter
} from "../../../frontend/admin/domain.js";

test("admin domain normalizes log level and event", () => {
  assert.equal(normalizeLogLevel("warning"), "log-warn");
  const event = createLogEvent("fetcher", "hello", "info");
  assert.equal(event.scope, "fetcher");
  assert.equal(event.message, "hello");
});

test("admin domain merges source statuses and filters", () => {
  const rows = mergeSourceStatusFromReport(
    [{ name: "A" }, { name: "B" }],
    { sources: [{ name: "A", status: "error", fetchedCount: 0 }] },
    "pending"
  );
  assert.equal(rows[0]._lastStatus, "error");
  const filtered = applySourceFilter(rows, "error");
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].name, "A");
});
