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
// Everything outside any @media block.
const unguarded = css.replace(/@media[^{]*\{[\s\S]*?\n\}\n?/g, "");
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

const desktopGuard = mediaBlocks
  .filter((block) => block.condition === "(min-width: 901px)")
  .map((block) => block.body)
  .join("\n");
const mobileBlock = mediaBlocks
  .filter((block) => block.condition === "(max-width: 900px)")
  .map((block) => block.body)
  .join("\n");
const cardBand = mediaBlocks
  .filter((block) => block.condition === "(min-width: 901px) and (max-width: 1100px)")
  .map((block) => block.body)
  .join("\n");

/** All rule bodies whose selector list contains `selector` exactly. */

// Heading scale and action-button sizing live in admin-css-sizing-guards.test.mjs.

test("admin table geometry stays behind the desktop guard", () => {
  // `.admin-row-header, .admin-user-row` (0,1,0) ties the shared stacked-card
  // rules in components.css (0,1,0) but admin.css loads after components.css,
  // so an unguarded page-sheet rule wins the tie and the mobile block can never
  // apply. That left the header visible *and* the inline `::before` labels
  // injected, while the eight wide tracks overflowed an `overflow: hidden`
  // ancestor — content the user could not even scroll to.
  const guardedGeometry = ruleBodies(desktopGuard, ".admin-user-row")
    .filter((body) => /grid-template-columns:/.test(body));
  assert.equal(guardedGeometry.length, 1, "the eight-column geometry must be declared in the desktop guard");
  assert.match(guardedGeometry[0], /minmax\(0, 1\.2fr\)/);

  for (const selector of [".admin-user-row", ".admin-row-header"]) {
    for (const body of ruleBodies(unguarded, selector)) {
      assert.doesNotMatch(
        body,
        /grid-template-columns:/,
        `${selector} must not declare tracks outside the desktop guard`,
      );
    }
  }

  // A rem floor sets a hard minimum the table cannot shrink below, which is
  // what produced the overflow band between 901px and the table's natural width.
  assert.doesNotMatch(desktopGuard, /minmax\(\s*\d+(?:\.\d+)?rem/);
});

test("admin header visibility is controlled only from inside width guards", () => {
  // `display: none` for the header belongs to the shared mobile block. An
  // unguarded page-sheet `display: grid` would outrank it by source order, and
  // the header would stay visible while the inline labels are also injected.
  for (const body of ruleBodies(unguarded, ".admin-row-header")) {
    assert.doesNotMatch(
      body,
      /display:\s*grid/,
      "an unguarded .admin-row-header display:grid re-hides the mobile header toggle",
    );
  }
});

test("admin source tables scroll instead of clipping their overflow", () => {
  // These tables use max-content tracks, so they overflow above the 900px card
  // breakpoint too. Every one of them sits inside a `.jobs-list`
  // (`overflow: hidden`), which made the overflow unreachable rather than
  // scrollable.
  for (const container of [
    "#admin-pending-sources",
    "#admin-active-sources",
    "#admin-rejected-sources",
    "#admin-ops-history",
  ]) {
    const rule = adminCss.match(
      new RegExp(`${container}[^{}]*\\{[^}]*overflow-x:\\s*(auto|scroll)`),
    );
    assert.ok(rule, `${container} must scroll horizontally`);
  }

  // The current-runs panel declares its own overflow later in the file, so a
  // shared rule earlier would lose the tie.
  const currentRuns = adminCss.match(/\.admin-ops-current-runs,\s*\.admin-ops-completed-runs\s*\{[\s\S]*?\n\}/)?.[0] || "";
  assert.match(currentRuns, /overflow-x:\s*auto/);
});

test("admin source tracks are container-independent and size their scroller to content", () => {
  // The header and the rows are two *separate* grid containers (the header lives
  // in `.jobs-table-header`, the rows in `.jobs-table-body`), so any track that
  // resolves against its own content desyncs the two once they scroll:
  // `max-content` sizes the header's "Name" to its 35px label while the row sizes
  // the same column to its 228px value, and `fr` resolves differently in the
  // header's width than in the body's. Measured with `fr` tracks, the header and
  // row cells disagreed at every width at or below 900px; with fixed tracks they
  // stay within 1px, before and after a 200px scroll.
  //
  // Only the mobile block is checked: above 901px the tables are not
  // `max-content`, so both grids are the same width and `fr` is safe there.
  // `\bfr\b` would NOT catch `1fr` (there is no word boundary between the digit
  // and the unit), hence the numeric form.
  const frUnit = /\d(?:\.\d+)?fr\b/;
  const baseTracks = ruleBodies(mobileBlock, ".admin-source-row")
    .concat(ruleBodies(mobileBlock, ".admin-source-row-header"))
    .filter((body) => /grid-template-columns:\s*52px/.test(body));
  assert.ok(baseTracks.length, "the mobile source track list must exist");
  for (const body of baseTracks) {
    const tracks = body.match(/grid-template-columns:([^;]+);/)?.[1] || "";
    assert.doesNotMatch(tracks, frUnit, "source tracks must not use fr: it desyncs header from rows");
    assert.doesNotMatch(tracks, /max-content/, "source tracks must not use max-content: it desyncs header from rows");
  }

  // `.jobs-table-body` is `overflow-y: auto` for the virtual window, which
  //    also makes it a horizontal scroll container: left at container width it
  //    clipped each row's last column and the parent never scrolled. Both the
  //    header and the body must be allowed to exceed the container so
  //    `.jobs-list` is the single horizontal scroller — otherwise the header and
  //    body scroll out of step.
  for (const source of ["#admin-pending-sources", "#admin-active-sources", "#admin-rejected-sources"]) {
    for (const part of ["jobs-table-header", "jobs-table-body"]) {
      const selector = `${source} > .${part}`;
      const bodies = ruleBodies(mobileBlock, selector);
      assert.ok(bodies.length, `${selector} must be sized in the mobile block`);
      assert.ok(
        bodies.some((body) => /width:\s*max-content/.test(body)),
        `${selector} must size to content so .jobs-list is the horizontal scroller`,
      );
    }
  }
});

test("admin source rows keep table treatment in the shared card range", () => {
  // Source rows carry `.admin-user-row` as well as `.admin-source-row`, so the
  // shared mobile card block matches them too. They must be restored to a
  // table: their `data-label` values are suppressed here so the visible header
  // is the only source of column names, and the flex container the card block
  // implies also defeats the cells' `text-overflow: ellipsis`.
  assert.match(
    mobileBlock,
    /\.admin-source-row-header,\s*\.admin-source-row\s*\{[\s\S]*display:\s*grid[\s\S]*grid-template-columns:\s*52px/,
  );
  assert.match(mobileBlock, /\.admin-source-row\s*\{[\s\S]*height:\s*var\(--admin-source-row-height, 52px\)/);
  assert.match(mobileBlock, /\.admin-source-row \.admin-cell::before[\s\S]*?content:\s*none/);

  // The headers must stay visible: without them the scrolled columns are
  // unidentifiable, because the row labels are suppressed just above. Both
  // selectors matter — components.css hides `.admin-row-header` and
  // `.admin-source-row-header` in its own `@media (max-width: 900px)`.
  assert.match(mobileBlock, /\.admin-row-header,\s*\.admin-source-row-header\s*\{\s*display:\s*grid;\s*\}/);
});

test("admin user table cards out between 901px and 1100px", () => {
  // Above 901px the eight-column table still fits, but only by starving its two
  // fluid columns: measured at 905px the Name cell was 10px and User ID 11px, so
  // text wrapped one character per line and the row stood 155px tall. No
  // overflow or clipping metric sees that — the text fits its own box, it is
  // simply unreadable. Hence a card band rather than a scroll.
  assert.match(cardBand, /#admin-users-list \.admin-user-row\s*\{[\s\S]*grid-template-columns:\s*1fr/);
  assert.match(cardBand, /#admin-users-list \.admin-cell::before\s*\{[\s\S]*?content:\s*attr\(data-label\)/);
  assert.match(cardBand, /#admin-users-list \.admin-row-header\s*\{\s*display:\s*none;\s*\}/);

  // Scoped to the user list: `.admin-user-row` is also carried by the source
  // rows and the Operations History rows, which must stay tables.
  for (const selector of [".admin-user-row", ".admin-source-row", ".admin-ops-history-row"]) {
    for (const body of ruleBodies(cardBand, selector)) {
      assert.doesNotMatch(
        body,
        /grid-template-columns:\s*1fr/,
        `${selector} must not be carded by the user-list band`,
      );
    }
  }

  // The band must not reach past 1100px, where the table is readable again.
  assert.doesNotMatch(cardBand, /min-width:\s*1101px/);
});

test("operations history keeps its header and rows aligned and reachable", () => {
  // The header and the rows are separate grids here, so the only thing keeping
  // them in step is that both declare the same tracks. Rows also inherit
  // `overflow: hidden` from `.admin-source-row`, so a track list that exceeds the
  // container clips its last column *and* leaves it unreachable: the row never
  // overflows its own parent, so no ancestor scroller has anything to scroll.
  //
  // Only the base (unguarded) track list is checked. The `520px` block
  // deliberately replaces it with five fluid columns that fit the container.
  const tracksFor = (selector) => ruleBodies(unguarded, selector)
    .filter((body) => /grid-template-columns:/.test(body))
    .map((body) => (body.match(/grid-template-columns:([^;]+);/)?.[1] || "").replace(/\s+/g, " ").trim());

  // `minmax(20rem, 1fr)` contains a space, so columns have to be split on paren
  // depth rather than on whitespace.
  const splitColumns = (tracks) => {
    const columns = [];
    let depth = 0;
    let current = "";
    for (const char of tracks) {
      if (char === "(") depth += 1;
      if (char === ")") depth -= 1;
      if (/\s/.test(char) && depth === 0) {
        if (current) columns.push(current);
        current = "";
        continue;
      }
      current += char;
    }
    if (current) columns.push(current);
    return columns;
  };

  const rowTracks = tracksFor(".admin-ops-history-row");
  assert.ok(rowTracks.length, "the ops-history track list must exist");
  assert.deepEqual(
    tracksFor(".admin-ops-history-header"),
    rowTracks,
    "header and rows must declare identical tracks, or the two grids desync"
  );
  for (const tracks of rowTracks) {
    assert.doesNotMatch(tracks, /max-content/, "ops tracks must not use max-content: it desyncs header from rows");
  }

  // An `fr` track is only safe when both grids resolve against the same definite
  // width. Under `width: max-content` an `fr` track contributes its own
  // max-content size, which differs between a short header label and a long row
  // value, and the two grids then disagree — measured 848px header against an
  // 850px row at a 1000px viewport, with the progress cell ellipsized at every
  // width from 360px to 1600px. Sizing both wrappers to the container width and
  // giving them the same `min-width` floor keeps them on one definite width, so no
  // item cap is needed and the `fr` track stays free to absorb the card's slack.
  //
  // A bounded `fr` track is what lets the row fill the card instead of leaving the
  // leftover width unallocated on the right.
  assert.ok(
    rowTracks.some((tracks) => /\d(?:\.\d+)?fr\b/.test(tracks)),
    "the ops tracks need a bounded fr column to absorb slack"
  );

  // The wrapper must be the horizontal scroller, and the body must be allowed to
  // exceed it, or the body clips the last column instead of scrolling.
  for (const part of ["jobs-table-header", "jobs-table-body"]) {
    const selector = `.admin-ops-completed-runs > .${part}`;
    const bodies = ruleBodies(css, selector);
    assert.ok(bodies.length, `${selector} must be sized so its wrapper scrolls`);
    assert.ok(
      bodies.some((body) => /width:\s*100%/.test(body) && /min-width:\s*[\d.]+rem/.test(body)),
      `${selector} must take the container width with a min-width floor so both grids share one definite width`
    );
  }

  // Guarded away from the 520px block, where five fluid tracks already fit: the
  // floor there would force a sideways scroll on a layout that needs none.
  const opsSizing = css.match(/@media\s*\(min-width:\s*521px\)\s*\{[\s\S]*?\.admin-ops-completed-runs\s*>\s*\.jobs-table-body\s*\{[\s\S]*?min-width:\s*[\d.]+rem/);
  assert.ok(opsSizing, "the ops width floor must stay behind the 521px guard");

  // The floor must not be smaller than the track sum plus its gutters, or the
  // tracks would be squeezed below the widths their content needs and the cells
  // would ellipsize again. `column-gap: 0.5rem` across six gutters is 3rem.
  const REM_PX = 16;
  const opsRowTracks = tracksFor(".admin-ops-history-row")[0];
  const opsColumns = splitColumns(opsRowTracks);
  assert.equal(opsColumns.length, 7, "the ops row must declare seven tracks");
  const opsGap = Number(
    (ruleBodies(unguarded, ".admin-ops-history-row").find((body) => /column-gap:/.test(body))
      ?.match(/column-gap:\s*([\d.]+)rem/)?.[1]) || 0
  );
  assert.ok(opsGap > 0, "the ops tracks need a non-zero column-gap or FINISHED and ACTIONS read as one word");
  const trackSumRem = opsColumns.reduce((total, column) => {
    const frFloor = column.match(/minmax\(\s*([\d.]+)rem/);
    if (frFloor) return total + Number(frFloor[1]);
    const fixed = column.match(/^([\d.]+)rem$/);
    assert.ok(fixed, `every ops track must be a fixed rem length or a bounded minmax, got ${column}`);
    return total + Number(fixed[1]);
  }, 0);
  const requiredFloorRem = trackSumRem + opsGap * (opsColumns.length - 1);
  const declaredFloorRem = Number(
    css.match(/\.admin-ops-completed-runs\s*>\s*\.jobs-table-body\s*\{[\s\S]*?min-width:\s*([\d.]+)rem/)?.[1]
  );
  assert.ok(
    declaredFloorRem >= requiredFloorRem,
    `the ops width floor (${declaredFloorRem}rem) must cover the tracks plus gutters (${requiredFloorRem}rem)`
  );
  assert.ok(
    declaredFloorRem * REM_PX <= 1200,
    `the ops width floor (${declaredFloorRem}rem) must stay within a usable viewport`
  );

  // The section labels are children of the `overflow-x: auto` wrapper, so without
  // pinning they scroll out of the box: measured at a 900px viewport the
  // recent-runs label left it entirely and the row read as a bare "2) -".
  for (const [label, selector] of [
    ["Current Runs", ".admin-ops-current-runs > .admin-ops-history-title"],
    ["Recent Runs", ".admin-ops-completed-runs > summary"]
  ]) {
    const sticky = ruleBodies(css, selector).find((body) => /position:\s*sticky/.test(body));
    assert.ok(sticky, `the ${label} label must be pinned so it stays readable while the table scrolls`);
    assert.match(sticky, /left:\s*0/, `the ${label} label must pin to the scroller's left edge`);
  }

  // The 520px block replaces the tracks with five fluid columns that fit.
  const narrowBlock = mediaBlocks.filter((b) => b.condition === "(max-width: 520px)").map((b) => b.body).join("\n");
  assert.match(narrowBlock, /\.admin-ops-history-row\s*\{[\s\S]*minmax\(0, 0\.75fr\)/);

  // Five tracks for seven cells, so both trailing cells must be hidden together.
  // Leaving one behind wraps it onto an implicit second grid row, which the 52px
  // `overflow: hidden` on `.admin-source-row` then clips — the abort button
  // rendered but could not be seen or clicked.
  const hiddenInNarrow = new Set();
  for (const match of narrowBlock.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
    if (!/display:\s*none/.test(match[2])) continue;
    match[1].split(",").map((part) => part.trim()).forEach((selector) => hiddenInNarrow.add(selector));
  }
  for (const nth of [6, 7]) {
    for (const prefix of [".admin-ops-history-header > div", ".admin-ops-history-row > .admin-cell"]) {
      const selector = `${prefix}:nth-child(${nth})`;
      assert.ok(
        hiddenInNarrow.has(selector),
        `the narrow layout must hide ${selector} or it wraps onto a clipped second row`
      );
    }
  }
});

test("admin source rows keep their virtualization height pinned", () => {
  // The source tables are virtualized against a fixed row height
  // (--admin-source-visible-rows). If a mobile rule lets the row grow, the
  // virtual window and the rendered rows disagree and rows get clipped.
  const pinned = ruleBodies(mobileBlock, ".admin-source-row")
    .filter((body) => /height:/.test(body));
  assert.ok(pinned.length, "the mobile source-row height rule must exist");
  for (const body of pinned) {
    assert.match(body, /min-height:\s*var\(--admin-source-row-height, 52px\)/);
    assert.match(body, /max-height:\s*var\(--admin-source-row-height, 52px\)/);
  }
});
