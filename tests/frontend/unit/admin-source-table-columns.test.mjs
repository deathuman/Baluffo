// Structure guards for the registry source tables (`frontend/admin/render/sources.js`).
//
// These are deliberately *structural* rather than snapshot assertions: the header
// and the rows are two separate grid containers whose tracks must stay identical,
// and the cell count must equal the track count. A mismatch in either does not
// throw — it silently wraps a cell onto an implicit second grid row, which the
// fixed 52px `overflow: hidden` on `.admin-source-row` then clips, so the field
// renders but can be neither seen nor clicked.
import test from "node:test";
import assert from "node:assert/strict";

import { renderSourcesTableHtml } from "../../../frontend/admin/render/sources.js";

const ROWS = [
  {
    id: "src-1",
    name: "Clay Token Game Studio (GameDevMap)",
    studio: "Clay Token Game Studio",
    adapter: "static",
    listing_url: "http://claytoken.net/careers?l=thai",
    _lastStatus: "warning"
  },
  {
    id: "src-2",
    name: "Improbable (Ashby)",
    studio: "Improbable",
    adapter: "ashby",
    listing_url: "https://jobs.ashbyhq.com/improbable",
    _lastStatus: "healthy"
  }
];

function render(rows = ROWS, mode = "pending", options = {}) {
  return renderSourcesTableHtml(
    rows,
    mode,
    row => String(row.jobsFound ?? 1),
    row => row._lastStatus || "not_run",
    () => ({ label: "Auto-approvable", tone: "healthy", title: "" }),
    options
  );
}

const headerCells = (html) => {
  const start = html.indexOf('class="admin-row-header admin-source-row-header"');
  if (start === -1) return [];
  const header = html.slice(start, html.indexOf("</div></div>", start));
  return [...header.matchAll(/<div>([^<]*)<\/div>/g)].map(m => m[1]);
};

const rowCellCounts = (html) =>
  [...html.matchAll(/<div class="admin-user-row admin-source-row"[^>]*>([\s\S]*?)<\/div>\s*<\/div>/g)]
    .map(m => (m[1].match(/class="admin-cell/g) || []).length);

test("source table header and rows agree on the cell count", () => {
  const html = render();
  const headers = headerCells(html);
  assert.ok(headers.length, "the header must render");
  for (const count of rowCellCounts(html)) {
    assert.equal(
      count,
      headers.length,
      "every row must have exactly as many cells as the header has tracks, or a cell wraps and is clipped"
    );
  }
});

test("source table columns are the varying fields, with the URL promoted", () => {
  // Studio was dropped: the registry already appends the studio as the Name's
  // trailing "(Source)" parenthetical, so it printed the same value twice on
  // essentially every row (measured 524/525). The URL replaced it — it is the one
  // field that tells near-duplicate rows apart, and it used to be tooltip-only.
  assert.deepEqual(headerCells(render()), ["Select", "Name", "Adapter", "URL", "Status", "Jobs", "Approval"]);
  assert.doesNotMatch(render(), /data-label="Studio"/, "the redundant Studio column must be gone");
  assert.match(render(), /data-label="URL"/, "the URL must be a real column, not tooltip-only");
});

test("source table renders the URL value with a full-value tooltip", () => {
  const html = render();
  assert.match(html, /http:\/\/claytoken\.net\/careers\?l=thai/);
  assert.match(html, /https:\/\/jobs\.ashbyhq\.com\/improbable/);
  // The cell truncates, so the untruncated value must stay reachable.
  assert.match(
    html,
    /class="admin-cell admin-source-url-cell" data-label="URL"><span class="admin-uid" data-tooltip="http:\/\/claytoken\.net\/careers\?l=thai"/,
  );
});

test("source table falls back to a dash when a row has no URL", () => {
  const html = render([{ id: "x", name: "No URL Studio (Manual Website)", adapter: "static" }]);
  assert.match(html, /admin-source-url-cell" data-label="URL"><span class="admin-uid"[^>]*>—<\/span>/);
});

test("source ID affordance is an inline SVG, not an ASCII glyph", () => {
  const html = render();
  assert.doesNotMatch(
    html,
    /admin-source-id-inline"[^>]*>i</,
    "the literal ASCII `i` must be gone: every other icon on the page is an inline SVG",
  );
  assert.match(html, /class="admin-source-id-glyph" viewBox="0 0 24 24"/);
  assert.match(html, /<path fill="currentColor"/);
  // The affordance must survive the swap: same tooltip, same aria-label.
  assert.match(html, /class="admin-source-id-inline" data-tooltip="src-1" aria-label="Source ID: src-1"/);
});

test("source table keeps the virtualization contract intact", () => {
  // The window is a slice of the full array; the header must not be rendered once
  // per window chunk, and the spacers must account for the rows outside it.
  const rows = Array.from({ length: 100 }, (_, i) => ({
    id: `s${i}`,
    name: `Studio ${i} (Ashby)`,
    adapter: "ashby",
    listing_url: `https://jobs.ashbyhq.com/s${i}`,
    _lastStatus: "healthy"
  }));
  const html = render(rows, "pending", { virtual: true, startIndex: 20, endIndex: 32, rowHeightPx: 52 });
  assert.equal((html.match(/admin-source-row-header/g) || []).length, 1, "exactly one header");
  assert.equal(rowCellCounts(html).length, 12, "only the window is rendered");
  assert.match(html, /data-window-start="20" data-window-end="32"/);
  assert.match(html, /style="height: 1040px;"/, "top spacer = 20 rows * 52px");
  assert.match(html, /style="height: 3536px;"/, "bottom spacer = 68 rows * 52px");
});

test("source table reports an empty bucket without a header", () => {
  assert.match(render([], "pending"), /class="no-results"/);
});
