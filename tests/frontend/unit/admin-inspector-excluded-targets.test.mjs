import test from "node:test";
import assert from "node:assert/strict";

import { isInspectorExcludedTarget } from "../../../frontend/admin/app/inspector.js";

function el(tag) {
  return {
    closest(selector) {
      return tag && String(selector).split(",").map(part => part.trim()).includes(tag) ? { matched: tag } : null;
    }
  };
}

test("inspector delegate excludes native-toggle and form targets", () => {
  for (const tag of ["summary", "label", "input", "select", "textarea", "button", "a"]) {
    assert.equal(isInspectorExcludedTarget(el(tag)), true, tag);
  }
});

// Excluding `details` made every row inside a disclosure unreachable, because a
// click on a row is also a click on a descendant of that `<details>`. `summary`
// is the real toggle surface, so it alone carries the toggle-in-place contract.
test("inspector delegate keeps rows inside disclosures reachable", () => {
  assert.equal(isInspectorExcludedTarget(el("details")), false);
  assert.equal(isInspectorExcludedTarget(el("div")), false);
});

test("inspector delegate keeps plain card-body and row targets eligible", () => {
  assert.equal(isInspectorExcludedTarget(el("div")), false);
  assert.equal(isInspectorExcludedTarget(el(null)), false);
  assert.equal(isInspectorExcludedTarget({}), false);
  assert.equal(isInspectorExcludedTarget(undefined), false);
});
