import test from "node:test";
import assert from "node:assert/strict";
import { parseUnifiedJobsPayload, parseCSVLarge } from "../../../frontend/jobs/data-source.js";

test("jobs data-source parses unified payload variants", () => {
  assert.equal(parseUnifiedJobsPayload([{ id: 1 }], null).length, 1);
  assert.equal(parseUnifiedJobsPayload({ jobs: [{ id: 1 }] }, null).length, 1);
  assert.equal(parseUnifiedJobsPayload({ items: [{ id: 1 }] }, null).length, 1);
});

test("jobs data-source delegates csv parse", () => {
  const rows = parseCSVLarge("a,b", {
    jobsParsing: {
      parseCSVLarge: (_csv, _deps) => [{ id: 1 }]
    },
    parserDeps: {}
  });
  assert.equal(rows.length, 1);
});
