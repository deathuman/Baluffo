// Element-level sizing guards for admin.css: heading scale and action-button width.
// Split out of admin-css-guards.test.mjs, which covers table geometry and scrolling.
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { ruleBodies } from "./helpers/dom-test-helpers.mjs";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

const adminCss = fs.readFileSync(path.join(repoRoot, "styles", "admin.css"), "utf8");

// Comments are stripped before any structural matching: a comment mentioning a
// selector is not a declaration, and without stripping, a `[^{}]*\{` pattern
// happily bridges prose into the next real rule and reports a phantom hit.
const css = adminCss.replace(/\/\*[\s\S]*?\*\//g, "");
// Every @media block, keyed by its condition.
const mediaBlocks = [...css.matchAll(/@media([^{]*)\{/g)].map((match) => {
  const condition = match[1].trim();
  const start = match.index + match[0].length;
  let depth = 1;
  let index = start;
  while (index < css.length && depth > 0) {
    if (css[index] === "{") depth += 1;
    else if (css[index] === "}") depth -= 1;
    index += 1;
  }
  return { condition, body: css.slice(start, index - 1) };
});

test("admin heading levels descend instead of inverting", () => {
  // `.admin-section-title` is worn by both the H2 section headings and the H4
  // sub-headings, and it used to carry one flat 1.05rem. Measured on the live page
  // that gave H2 16.8px / H3 16px / H4 16.8px: the H4s nested inside an H3 section
  // rendered *larger* than their own parent heading, so the outline read backwards.
  // The sizes now come from the element, and this asserts the resulting order.
  const px = (selector) => {
    const body = ruleBodies(css, selector).join("\n");
    const match = body.match(/font-size:\s*([\d.]+)rem/);
    return match ? Number(match[1]) : null;
  };

  const h2 = px("h2.admin-section-title");
  const h3 = px(".admin-fetcher-title");
  const h4 = px(".admin-section-title");
  assert.ok(h2 !== null, "the H2 section heading must declare its own size");
  assert.ok(h3 !== null, "the H3 section heading must declare a size");
  assert.ok(h4 !== null, "the H4 sub-heading must declare a size");

  assert.ok(h2 > h3, `H2 (${h2}rem) must outrank H3 (${h3}rem)`);
  assert.ok(h3 > h4, `H3 (${h3}rem) must outrank its H4 children (${h4}rem)`);

  // The gaps are pinned, not just the order: a scale that merely descends would let
  // the three levels drift back within a fraction of a pixel of each other, which is
  // how they read as one flat size in the first place.
  assert.equal(h2, 1.2, "the H2 section heading size");
  assert.equal(h3, 1.1, "the H3 section heading size");
  assert.equal(h4, 1, "the H4 sub-heading size");

  // A later rule must not flatten the scale again. The two context overrides
  // (`.admin-discovery-card`, `.action-center-header`) are margin-only by design.
  for (const selector of [".admin-discovery-card .admin-section-title", ".action-center-header .admin-section-title"]) {
    const body = ruleBodies(css, selector).join("\n");
    assert.doesNotMatch(body, /font-size:/, `${selector} must stay margin-only so it cannot re-invert the scale`);
  }
});

test("admin discovery action buttons size to their labels", () => {
  // These buttons used to stretch to fill their row: `flex: 1 1 140px` made
  // "Approve Selected" 527px wide to show 125px of text, and the equal-thirds filter
  // grid made the 19px "All" chip 349px. A stretched button reads as a full-width
  // primary action and leaves the row mostly empty, so both are content-sized now.
  //
  // Matched against the comment-stripped `css`, not `adminCss`: the rule that
  // documents this decision quotes the old `flex: 1 1 140px` value, and matching the
  // raw source would find that prose and report a phantom regression.
  assert.doesNotMatch(
    css,
    /\.admin-discovery-bulk-card \.admin-fetcher-actions \.clear-filters-btn\s*\{[^}]*flex:\s*1/,
    "no width guard may reintroduce a growing bulk button",
  );
  assert.doesNotMatch(
    css,
    /\.admin-discovery-bulk-card \.admin-delete-sources-btn\s*\{[^}]*flex:\s*1/,
    "the delete button must not grow to fill the row either",
  );

  // The filter chips must not be forced into equal fractional columns.
  const filterRule = css.match(/\.admin-discovery-filter-card \.saved-custom-filter-actions\s*\{[^}]*\}/)?.[0] || "";
  assert.ok(filterRule, "the filter action grid must exist");
  assert.doesNotMatch(
    filterRule,
    /repeat\((\d+),\s*minmax\(0,\s*1fr\)\)/,
    "an equal-column grid stretches short filter labels to a fixed share of the row",
  );
  assert.match(filterRule, /max-content/, "filter chips size to their label");

  // No width guard may restore the fixed-column shape either.
  for (const body of mediaBlocks.map(block => block.body)) {
    for (const match of body.matchAll(/\.admin-discovery-filter-card \.saved-custom-filter-actions\s*\{([^}]*)\}/g)) {
      assert.doesNotMatch(
        match[1],
        /repeat\((\d+),\s*minmax\(0,\s*1fr\)\)/,
        "a width guard must not restore the equal-column filter grid",
      );
    }
  }
});
