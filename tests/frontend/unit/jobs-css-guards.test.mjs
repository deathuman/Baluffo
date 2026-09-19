import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

const jobsCss = fs.readFileSync(path.join(repoRoot, "styles", "jobs.css"), "utf8");

// Comments are stripped before structural matching: a comment mentioning a
// selector is not a declaration, and without stripping a `[^{}]*\{` pattern
// bridges prose into the next real rule and reports a phantom hit.
const css = jobsCss.replace(/\/\*[\s\S]*?\*\//g, "");

/** Every `@media` block, as { condition, body }. */
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

const mobileBlock = mediaBlocks
  .filter((block) => block.condition === "(max-width: 900px)")
  .map((block) => block.body)
  .join("\n");

/** All rule bodies whose selector list contains `selector` exactly. */
function ruleBodies(source, selector) {
  const bodies = [];
  for (const match of source.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    const selectors = match[1].split(",").map((part) => part.trim());
    if (selectors.includes(selector)) bodies.push(match[2]);
  }
  return bodies;
}

test("jobs stacked card pairs its metadata cells instead of one per line", () => {
  // The shared block in components.css sets `grid-template-columns: 1fr`, so
  // every cell was as wide as the card — 544px at a 675px viewport — while the
  // values inside it measured 59-85px. Measured waste to the right of the last
  // painted pixel was 48.6% of the row on average at 675px and 63.4% at 900px,
  // and the card was exactly as tall at 900px as at 430px because the track
  // count never changed.
  const rowRule = ruleBodies(mobileBlock, ".jobs-page .job-row")[0];
  assert.ok(rowRule, "the mobile .jobs-page .job-row rule must exist");
  assert.match(
    rowRule,
    /grid-template-columns:\s*repeat\(auto-fit,\s*minmax\(min\([^)]+\),\s*1fr\)\)/,
    "the card must auto-fit columns rather than stack one cell per line"
  );
  // A bare `1fr` (or any single fixed track) is the regression this guards.
  assert.doesNotMatch(rowRule, /grid-template-columns:\s*1fr\b/);
  // The floor must be capped by the container: `minmax(12.75rem, 1fr)` has a
  // hard 204px floor and overflows once the card is narrower than that.
  assert.match(
    rowRule,
    /minmax\(min\(\s*[\d.]+rem\s*,\s*100%\s*\)/,
    "the track floor must be capped by 100% so it cannot overflow the card"
  );
});

test("jobs stacked card title keeps the full row", () => {
  // `.col-title` holds the freshness dot, the title, the new badge, the
  // lifecycle badge and the sector line. Left in a single auto-fit track it
  // would be squeezed to one column while the badges stayed on the same line,
  // so it spans the whole row.
  const titleRule = ruleBodies(mobileBlock, ".jobs-page .col-title")[0];
  assert.ok(titleRule, "the mobile .jobs-page .col-title rule must exist");
  assert.match(titleRule, /grid-column:\s*1\s*\/\s*-1/);
});

test("jobs card track floor stays above the longest unbreakable value", () => {
  // The floor exists to stop the value from being clipped. The longest value
  // that cannot wrap is the single-word company "Blackbirdinteractive", 119px
  // intrinsic. Between 761px and 764px a 12.5rem floor produced 200.55px
  // tracks; the 72px label box (still 72px above the 760px breakpoint) left
  // 117.36px and clipped the value by 1-2px. 12.75rem (204px) is the smallest
  // floor measured to never clip between 900px and 320px.
  const rowRule = ruleBodies(mobileBlock, ".jobs-page .job-row")[0];
  const floor = rowRule.match(/minmax\(min\(\s*([\d.]+)rem/)?.[1];
  assert.ok(floor, "the track floor must be expressed in rem");
  assert.ok(
    Number(floor) >= 12.75,
    `the floor is ${floor}rem; below 12.75rem the longest single-word company clips`
  );
});

test("jobs desktop table tracks stay inside their min-width guard", () => {
  // The six-column table is desktop-only. As `.jobs-page .job-row` (0,2,0) it
  // outranks the shared stacked-card fallback `.job-row` (0,1,0) at every
  // width, so an unguarded table rule makes the mobile layout unreachable and
  // overflows the document. This is the repo's recurring bug class.
  const unguarded = mediaBlocks.length
    ? css.replace(/@media[^{]*\{(?:[^{}]|\{(?:[^{}]|\{[^{}]*\})*\})*\}/g, "")
    : css;
  for (const body of ruleBodies(unguarded, ".jobs-page .job-row")) {
    assert.doesNotMatch(
      body,
      /grid-template-columns:\s*minmax\(0,\s*1\.8fr\)/,
      "the six-column desktop tracks must not appear outside a media query"
    );
  }
  const desktopBlock = mediaBlocks
    .filter((block) => block.condition === "(min-width: 901px)")
    .map((block) => block.body)
    .join("\n");
  const desktopRule = ruleBodies(desktopBlock, ".jobs-page .job-row")[0];
  assert.ok(desktopRule, "the desktop .jobs-page .job-row rule must exist");
  assert.match(desktopRule, /grid-template-columns:\s*minmax\(0,\s*1\.8fr\)/);
});

test("jobs stacked card keeps a non-zero column gap between paired cells", () => {
  // The pairing only reads as pairs if the columns are separated. The shared
  // block sets `gap: 0.45rem` (a single value, so row and column gap are equal
  // and small); the paired layout needs a wider column gap so the label of the
  // right-hand cell is not read as part of the left-hand value. A `gap: 0`
  // collapses the two columns against each other.
  const rowRule = ruleBodies(mobileBlock, ".jobs-page .job-row")[0];
  const gap = rowRule.match(/(?:^|[;\s])gap:\s*([^;]+);/)?.[1];
  assert.ok(gap, "the mobile card must declare a gap");
  const parts = gap.trim().split(/\s+/);
  const columnGap = parts.length > 1 ? parts[1] : parts[0];
  assert.doesNotMatch(columnGap, /^0(?:px|rem|em|%)?$/, "the column gap must not be zero");
  assert.ok(
    parseFloat(columnGap) > 0,
    `the column gap is ${columnGap}; paired columns would touch`
  );
  assert.ok(
    parseFloat(columnGap) >= 0.6,
    `the column gap is ${columnGap}; paired columns need visible separation`
  );
});

test("jobs mobile card rules outrank the shared stacked-card block", () => {
  // The shared block styles `.job-row` (0,1,0). The page sheet wins equal
  // specificity ties because it loads last, but the mobile overrides are
  // written as `.jobs-page .job-row` (0,2,0) so the outcome does not depend on
  // stylesheet order. This asserts the specificity, not just the presence.
  const rowRule = ruleBodies(mobileBlock, ".jobs-page .job-row")[0];
  assert.ok(rowRule, "the mobile rule must be scoped to .jobs-page");
  assert.equal(
    ruleBodies(mobileBlock, ".job-row").length,
    0,
    "an unscoped .job-row rule would tie with components.css and depend on load order"
  );
});
