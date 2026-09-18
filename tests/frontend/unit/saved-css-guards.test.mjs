import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

const savedCss = fs.readFileSync(path.join(repoRoot, "styles", "saved.css"), "utf8");

// Comments are stripped before structural matching: a comment mentioning a
// selector is not a declaration, and without stripping a `[^{}]*\{` pattern
// bridges prose into the next real rule and reports a phantom hit.
const css = savedCss.replace(/\/\*[\s\S]*?\*\//g, "");
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

test("saved table tracks use zero floors so they cannot overflow the document", () => {
  const rowRule = css.match(/\.saved-row-header,\s*\.saved-job-row\s*\{[\s\S]*?\n\}/)?.[0] || "";
  assert.match(rowRule, /grid-template-columns: minmax\(0, 1\.58fr\) minmax\(0, 1\.05fr\) minmax\(0, 0\.68fr\)/);
  assert.doesNotMatch(rowRule, /minmax\(\s*\d+(?:\.\d+)?rem/);
});

test("saved job title row wraps instead of overflowing its cell", () => {
  // `.col-title` is a flex row holding the title, the inline source badge and
  // the freshness dot. Without `flex-wrap` those items cannot break, so a long
  // title plus its badges pushed the row wider than the track and the whole page
  // gained a horizontal scrollbar: measured page overflow was +9px at 811px,
  // +40px at 780px, +220px at 600px and +460px at 360px before the fix.
  const titleRule = ruleBodies(css, ".saved-job-row .col-title")[0];
  assert.ok(titleRule, ".saved-job-row .col-title must be styled");
  assert.match(titleRule, /display:\s*flex/);
  assert.match(titleRule, /flex-wrap:\s*wrap/);
  assert.match(titleRule, /min-width:\s*0/);
});

test("saved phase bar wraps on narrow widths instead of forcing a 723px minimum", () => {
  // The mobile bar used to be a horizontal flex row whose eight steps each
  // carried `min-width: 5.65rem` — a hard 723px floor inside a container that is
  // ~360px at the narrow end. It could only scroll sideways, and its absolutely
  // positioned connector line stretched to the full scrolled width.
  const barRule = ruleBodies(mobileBlock, ".phase-bar")[0];
  assert.ok(barRule, "the mobile .phase-bar rule must exist");
  assert.match(barRule, /display:\s*grid/);
  assert.match(barRule, /grid-template-columns:\s*repeat\(auto-fit/);
  assert.doesNotMatch(barRule, /overflow-x:\s*auto/);

  // With the steps wrapped across several rows there is no single straight line
  // to draw through them, so the connector must be suppressed rather than left
  // floating across the middle of the grid.
  assert.match(mobileBlock, /\.phase-bar::before,\s*\.phase-bar::after\s*\{\s*display:\s*none;\s*\}/);

  // The hard per-step minimum must be lifted in the same block.
  const stepRule = ruleBodies(mobileBlock, ".phase-timeline-step")
    .filter((body) => /min-width/.test(body));
  assert.ok(stepRule.length, "the mobile step min-width override must exist");
  for (const body of stepRule) {
    assert.match(body, /min-width:\s*0/);
  }
});

test("saved phase row is a single zero-floor column in the shared card range", () => {
  // `170px 1fr` cannot shrink below 170px plus its gap, so the row overflowed a
  // 360px viewport. The mobile block must collapse it to one fluid column.
  const phaseRow = ruleBodies(mobileBlock, ".saved-phase-row")[0];
  assert.ok(phaseRow, "the mobile .saved-phase-row rule must exist");
  assert.match(phaseRow, /grid-template-columns:\s*1fr/);
  assert.doesNotMatch(phaseRow, /\d+px\s+1fr/);
});

test("saved mobile rows keep their remove-button gutter as a variable", () => {
  // The remove button is absolutely positioned from the same custom properties
  // the row pads by, so overriding one without the other puts the button on top
  // of the content. The gutter and the size are set in two separate mobile rules
  // (the card rule and the compact-row rule), so the pair is checked as a union.
  const rowBodies = ruleBodies(mobileBlock, ".saved-job-row");
  assert.ok(rowBodies.length, "the mobile .saved-job-row rules must exist");
  const joined = rowBodies.join("\n");
  assert.match(joined, /--saved-remove-gutter:/);
  assert.match(joined, /--saved-remove-size:/);
  assert.ok(
    rowBodies.some((body) => /grid-template-columns:\s*1fr/.test(body)),
    "the mobile row must collapse to a single column",
  );

  const buttonRule = ruleBodies(mobileBlock, ".remove-inline-btn")[0];
  assert.ok(buttonRule, "the mobile .remove-inline-btn rule must exist");
  assert.match(buttonRule, /left:\s*calc\(\(var\(--saved-remove-gutter\)/);
});
