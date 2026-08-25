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
  for (const tag of ["summary", "details", "label", "input", "select", "textarea", "button", "a"]) {
    assert.equal(isInspectorExcludedTarget(el(tag)), true, tag);
  }
});

test("inspector delegate keeps plain card-body and row targets eligible", () => {
  assert.equal(isInspectorExcludedTarget(el("div")), false);
  assert.equal(isInspectorExcludedTarget(el(null)), false);
  assert.equal(isInspectorExcludedTarget({}), false);
  assert.equal(isInspectorExcludedTarget(undefined), false);
});
