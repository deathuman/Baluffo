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

test("saved mobile rows pair their metadata fields instead of one per line", () => {
  // As a card the row was a single column, so each of the six cells owned a
  // full-width line: at 811px every cell measured 648px wide while the values
  // inside were 27-115px, spending 322px of height on five short strings and
  // leaving ~450px of every line empty. An `auto-fit` grid pairs the short
  // fields up — two per line where the width allows, one where it does not —
  // which measures 254px for the same content.
  const rowBodies = ruleBodies(mobileBlock, ".saved-job-row");
  assert.ok(rowBodies.length, "the mobile .saved-job-row rules must exist");
  const gridRule = rowBodies.find((body) => /grid-template-columns/.test(body));
  assert.ok(gridRule, "the mobile row must declare its tracks");
  assert.match(gridRule, /repeat\(auto-fit/);

  // The floor must be wrapped in `min()`: a bare `minmax(18rem, 1fr)` is a hard
  // 288px floor, and at 360px the row's content box is 252px, so the track
  // overflowed by 36px and the title's clip box ran 30px under the delete
  // button. `min(18rem, 100%)` collapses the floor once the container is smaller.
  const tracks = gridRule.match(/grid-template-columns:\s*([^;]+);/)?.[1] || "";
  assert.match(tracks, /minmax\(min\(\s*\d+(?:\.\d+)?rem,\s*100%\s*\),\s*1fr\)/);

  // Title and link carry the full width: the title is the card's heading, and
  // the link row is a centred icon cluster or "No link", neither of which pairs
  // meaningfully with a field beside it.
  const spanRule = ruleBodies(mobileBlock, ".saved-job-row > .col-title")
    .concat(ruleBodies(mobileBlock, ".saved-job-row > .col-link"))
    .join("\n");
  assert.match(spanRule, /grid-column:\s*1\s*\/\s*-1/);
});

test("saved mobile card labels line up and values start on one x", () => {
  // The label was `flex: 0 0 72px` with mixed `text-align`: three labels were
  // left-aligned and three centred, and since the glyphs are 26-61px wide the
  // labels began at x=101, 106, 123... — visibly ragged. A fixed-width label
  // column with a consistent alignment is what puts every value on the same x.
  const labelRule = ruleBodies(mobileBlock, ".saved-job-row .job-cell::before")[0];
  assert.ok(labelRule, "the mobile cell label rule must exist");
  assert.match(labelRule, /flex:\s*0 0 \d/);
  assert.match(labelRule, /text-align:\s*left/);

  // Contract and Type are chips that desktop centres inside their track; on a
  // card that leaves them floating away from their own label.
  const chipRule = ruleBodies(mobileBlock, ".saved-job-row .col-contract")
    .concat(ruleBodies(mobileBlock, ".saved-job-row .col-type"))
    .join("\n");
  assert.match(chipRule, /text-align:\s*left/);

  // `.saved-link-actions` is `width: 100%; justify-content: center`, so on a
  // full-width row "No link" sat at x=431 in the middle of the card while the
  // other five values started at x=191.
  const linkRule = ruleBodies(mobileBlock, ".saved-job-row .col-link .saved-link-actions")[0];
  assert.ok(linkRule, "the mobile link-actions rule must exist");
  assert.match(linkRule, /justify-content:\s*flex-start/);

  // The company name is nowrap+ellipsis on desktop where its column is fixed;
  // in a card the column is fluid, so wrapping beats truncating a name to
  // "Really Long Co...".
  const companyRule = ruleBodies(mobileBlock, ".saved-job-row .job-company-compact")[0];
  assert.ok(companyRule, "the mobile company-name rule must exist");
  assert.match(companyRule, /white-space:\s*normal/);
});

test("saved mobile remove button moves out of the content gutter", () => {
  // It used to be centred on the whole row (`top: 50%`), which on a 322px card
  // put it at y=769 — below the title it deletes and alongside the COMPANY cell
  // — and it held a 68px left gutter open for the full height of the row.
  const buttonRule = ruleBodies(mobileBlock, ".remove-inline-btn")[0];
  assert.ok(buttonRule, "the mobile .remove-inline-btn rule must exist");
  assert.match(buttonRule, /left:\s*auto/);
  assert.match(buttonRule, /right:/);
  assert.match(buttonRule, /transform:\s*none/);
  assert.doesNotMatch(buttonRule, /top:\s*50%/);

  // The title must reserve room for the button it now shares a line with. The
  // value must be non-zero: `padding-right: 0` still matches a bare
  // `padding-right` check while leaving the title free to run under the button.
  const titlePad = ruleBodies(mobileBlock, ".saved-job-row > .col-title")
    .map((body) => body.match(/padding-right:\s*([^;]+);/)?.[1])
    .filter(Boolean);
  assert.ok(titlePad.length, "the mobile title must reserve space for the button");
  for (const value of titlePad) {
    const trimmed = value.trim();
    assert.notEqual(trimmed, "0", "the reserved space must not be zero");
    assert.doesNotMatch(trimmed, /^0(?:px|rem|em|%)?$/, "the reserved space must not be zero");
  }
});
