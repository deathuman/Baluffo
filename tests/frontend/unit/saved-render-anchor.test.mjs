import test from "node:test";
import assert from "node:assert/strict";
import { computeAnchorScrollDelta } from "../../../frontend/saved/app.js";

test("saved render anchor computes scroll delta to keep row anchored", () => {
  assert.equal(computeAnchorScrollDelta(220, 260), 40);
  assert.equal(computeAnchorScrollDelta(260, 220), -40);
  assert.equal(computeAnchorScrollDelta(120, 120), 0);
});

test("saved render anchor returns zero for invalid values", () => {
  assert.equal(computeAnchorScrollDelta("x", 100), 0);
  assert.equal(computeAnchorScrollDelta(100, Number.NaN), 0);
});
