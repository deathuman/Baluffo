import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..", "..", "..");

test("jobs html update button uses user-facing update copy", () => {
  const html = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  assert.match(html, /id="jobs-pipeline-run-btn"/);
  assert.match(html, />Update jobs<\/button>/);
  assert.match(
    html,
    /data-tooltip="Find new openings and rebuild the local job list\. This usually takes a few minutes; first updates can take up to 1 hour\."/
  );
  assert.doesNotMatch(html, /Run Discovery \+ Fetch \+ Sync/);
  assert.doesNotMatch(html, /id="jobs-pipeline-run-btn"[^>]+title=/);
});

test("jobs toolbar keeps a fixed status row for pipeline progress and last-updated", () => {
  // ponytail: the caption and the results summary live in the always-rendered
  // status row below the actions row — that is what keeps the toolbar height and
  // button positions invariant while a run is active. The Last-updated stamp
  // shares the button line instead (pinned left), so it is asserted against the
  // actions row, not the status row.
  const html = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  const actionsStart = html.indexOf('<div class="toolbar-actions">');
  const actionsEnd = html.indexOf('<div class="jobs-toolbar-status"');
  const statusStart = html.indexOf('<div class="jobs-toolbar-status" data-ui="jobs-toolbar-status">');
  const summaryAt = html.indexOf('id="results-summary"');
  const captionAt = html.indexOf('id="jobs-pipeline-progress-caption"');
  const lastUpdatedAt = html.indexOf('id="jobs-last-updated"');
  const runBtnAt = html.indexOf('id="jobs-pipeline-run-btn"');
  const reloadBtnAt = html.indexOf('id="refresh-jobs-btn"');
  const badgeAt = html.indexOf('id="refresh-jobs-needed-badge"');
  assert.ok(actionsStart >= 0, "actions row must exist");
  assert.ok(statusStart > actionsStart, "status row must follow the actions row");
  assert.ok(
    summaryAt > statusStart && summaryAt < captionAt,
    "results summary must lead the status row so it sits directly above the table"
  );
  assert.ok(captionAt > summaryAt, "pipeline caption must live in the status row");
  // The stamp rides the button line, ahead of the button so `margin-right: auto`
  // pins it to the left edge while the button stays right-aligned.
  assert.ok(lastUpdatedAt > actionsStart, "last-updated must live in the actions row");
  assert.ok(lastUpdatedAt < actionsEnd, "last-updated must live in the actions row, not the status row");
  assert.ok(lastUpdatedAt < runBtnAt, "last-updated must precede the Update jobs button");
  assert.equal(reloadBtnAt, -1, "Reload button is removed; updates load automatically");
  assert.equal(badgeAt, -1, "Updates-found badge is removed with the Reload flow");
});

test("jobs toolbar spacing and compact pagination cannot silently regress", () => {
  const html = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  const css = fs.readFileSync(path.join(repoRoot, "styles", "jobs.css"), "utf8");
  // A small gap keeps the caption line from looking crammed against the button.
  assert.match(css, /\.jobs-page \.jobs-toolbar-status\s*\{[\s\S]*margin-top:\s*0\.25rem/);
  // The stamp is pinned left on the shared button line.
  assert.match(css, /\.jobs-page \.jobs-last-updated\s*\{[\s\S]*margin-right:\s*auto/);
  // Narrow screens wrap the stamp onto its own row below the full-width button.
  assert.match(css, /@media \(max-width: 900px\)[\s\S]*\.jobs-page \.jobs-last-updated\s*\{[\s\S]*order:\s*2/);
  // The dim-while-running rule must follow the stamp out of the status row.
  assert.match(css, /\.jobs-page \.jobs-toolbar\.running \.jobs-last-updated\s*\{[\s\S]*opacity:\s*0\.55/);
  // Compact pager: the strip is still reserved (first-paint invariant) but small.
  const paginationRule = css.match(/\.jobs-page \.pagination\s*\{[\s\S]*?\}/)?.[0] || "";
  assert.match(paginationRule, /min-height:\s*1\.75rem/);
  const pageBtnRule = css.match(/\.jobs-page \.page-btn\s*\{[\s\S]*?\}/)?.[0] || "";
  assert.match(pageBtnRule, /min-height:\s*1\.75rem/);
  assert.match(pageBtnRule, /min-width:\s*1\.75rem/);
  // The pager still sits inside the toolbar block, above the table header.
  assert.ok(html.indexOf('id="pagination"') > html.indexOf('id="jobs-list"'));
});

test("desktop html meaningful operational buttons expose polished tooltips", () => {
  const jobsHtml = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  const savedHtml = fs.readFileSync(path.join(repoRoot, "saved.html"), "utf8");
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");

  [
    [jobsHtml, /id="country-picker-clear-btn"[^>]+data-tooltip="Clear the current country selection\."/],
    [jobsHtml, /id="customize-quick-filters-btn"[^>]+data-tooltip="Choose which preset filters are shown\."/],
    [jobsHtml, /id="quick-filters-reset-btn"[^>]+data-tooltip="Restore the default quick filter presets\."/],
    [savedHtml, /id="history-panel-toggle-btn"[\s\S]*<svg viewBox="0 0 24 24"/],
    [savedHtml, /class="activity-toggle-label">Activity timeline<\/span>/],
    [adminHtml, /id="admin-run-discovery-btn"[^>]+data-tooltip="Run source discovery with the default bridge preset\."/],
    [adminHtml, /id="admin-load-discovery-btn"[^>]+data-tooltip="Load the latest source discovery report\."/],
    [adminHtml, /id="admin-add-manual-source-btn"[^>]+data-tooltip="Add the entered source URL to the review queue\."/],
    [adminHtml, /id="admin-approve-sources-btn"[^>]+data-tooltip="Move selected pending sources to active\."/],
    [adminHtml, /id="admin-reject-sources-btn"[^>]+data-tooltip="Move selected pending sources to rejected\."/],
    [adminHtml, /id="admin-restore-rejected-btn"[^>]+data-tooltip="Restore selected rejected sources to pending\."/],
    [adminHtml, /id="admin-delete-sources-btn"[^>]+data-tooltip="Delete selected reviewed sources from the local registry\."/]
  ].forEach(([html, pattern]) => assert.match(html, pattern));

  assert.doesNotMatch(savedHtml, /id="add-custom-job-btn"[^>]+data-tooltip=/);
  assert.doesNotMatch(savedHtml, /id="export-backup-btn"[^>]+data-tooltip=/);
  assert.doesNotMatch(savedHtml, /id="import-backup-btn"[^>]+data-tooltip=/);
  assert.doesNotMatch(savedHtml, /id="activity-refresh-btn"[^>]+data-tooltip=/);
  assert.doesNotMatch(savedHtml, /id="activity-close-btn"[^>]+data-tooltip=/);
  assert.doesNotMatch(adminHtml, /id="admin-refresh-btn"/);
  assert.doesNotMatch(`${jobsHtml}\n${savedHtml}\n${adminHtml}`, /id="(?:country-picker-clear-btn|customize-quick-filters-btn|quick-filters-reset-btn|refresh-jobs-btn|add-custom-job-btn|export-backup-btn|import-backup-btn|activity-refresh-btn|admin-run-discovery-btn|admin-load-discovery-btn|admin-add-manual-source-btn|admin-approve-sources-btn|admin-reject-sources-btn|admin-restore-rejected-btn|admin-delete-sources-btn)"[^>]+\stitle=/);
});

test("jobs table tracks stay behind the desktop guard so narrow layouts cannot overflow", () => {
  const jobsCss = fs.readFileSync(path.join(repoRoot, "styles", "jobs.css"), "utf8");

  // The six-column table must only be declared inside the desktop guard. As
  // unguarded `.jobs-page .job-row` rules they are (0,2,0) and outrank the
  // stacked-card fallback in components.css `.job-row` (0,1,0) at every width,
  // which silently disabled the mobile layout and overflowed the document.
  const guarded = jobsCss.match(/@media \(min-width: 901px\) \{[\s\S]*?\n\}/)?.[0] || "";
  assert.match(guarded, /\.jobs-page \.job-row-header\s*\{[\s\S]*grid-template-columns:/);
  assert.match(guarded, /\.jobs-page \.job-row,\s*\.jobs-page \.job-row-link\s*\{[\s\S]*grid-template-columns:/);

  // No unguarded jobs rule may declare tracks.
  const unguarded = jobsCss.replace(/@media[^{]*\{[\s\S]*?\n\}\n?/g, "");
  assert.doesNotMatch(unguarded, /\.jobs-page \.job-row(?:-header|-link)?[^{]*\{[^}]*grid-template-columns:/);
  assert.doesNotMatch(unguarded, /\.jobs-page \.job-row(?:-header|-link)?[^{]*\{[^}]*column-gap:/);

  // Fluid tracks need a 0 floor; a rem floor sets a hard minimum the table
  // cannot shrink below and re-introduces the overflow band.
  assert.doesNotMatch(guarded, /minmax\(\s*\d+(?:\.\d+)?rem/);
});

test("jobs table-only cell styling stays behind the desktop guard", () => {
  const jobsCss = fs.readFileSync(path.join(repoRoot, "styles", "jobs.css"), "utf8");
  const guarded = jobsCss.match(/@media \(min-width: 901px\) \{[\s\S]*?\n\}/)?.[0] || "";
  const unguarded = jobsCss.replace(/@media[^{]*\{[\s\S]*?\n\}\n?/g, "");

  // Selectors are compared exactly (after trimming) rather than by prefix: a
  // descendant rule such as `.jobs-page .col-save .job-inline-save-btn` legitimately
  // centres the round save glyph, and must not be read as centring the cell.
  const rulesIn = (source) => [...source.matchAll(/([^{}]+)\{([^{}]*)\}/g)].map((match) => ({
    selectors: match[1].split(",").map((part) => part.trim()).filter(Boolean),
    body: match[2],
  }));
  const bodiesFor = (source, selector) => rulesIn(source)
    .filter((rule) => rule.selectors.includes(selector))
    .map((rule) => rule.body);

  // Centring the contract/type chips and single-line ellipsis for the title and
  // company are table behaviours. Unguarded they are (0,2,0)/(0,3,0) and beat
  // the shared stacked-card rules in components.css (0,1,0), which left the
  // card layout with mixed alignment (title/company/location left, contract and
  // type centred) and a long title ellipsized instead of wrapping.
  const guardedCells = bodiesFor(guarded, ".jobs-page .col-contract")
    .concat(bodiesFor(guarded, ".jobs-page .col-type"))
    .concat(bodiesFor(guarded, ".jobs-page .col-save"));
  assert.ok(
    guardedCells.some((body) => /justify-content:\s*center/.test(body)),
    "the desktop guard must own the centred contract/type/save cells",
  );
  const guardedTitles = bodiesFor(guarded, ".jobs-page .job-row .job-title-compact")
    .concat(bodiesFor(guarded, ".jobs-page .job-row .job-company-compact"));
  assert.ok(
    guardedTitles.some((body) => /white-space:\s*nowrap/.test(body)),
    "the desktop guard must own single-line title/company ellipsis",
  );

  // None of them may survive outside the guard.
  for (const selector of [
    ".jobs-page .col-contract",
    ".jobs-page .col-type",
    ".jobs-page .col-save",
    ".jobs-page .job-row .job-title-compact",
    ".jobs-page .job-row .job-company-compact",
  ]) {
    for (const body of bodiesFor(unguarded, selector)) {
      assert.doesNotMatch(
        body,
        /justify-content:/,
        `${selector} must not set justify-content outside the desktop guard`,
      );
      assert.doesNotMatch(
        body,
        /white-space:/,
        `${selector} must not set white-space outside the desktop guard`,
      );
    }
  }
});

test("jobs card cells align their labels and clear the freshness dot", () => {
  const componentsCss = fs.readFileSync(path.join(repoRoot, "styles", "components.css"), "utf8");
  const css = componentsCss.replace(/\/\*[\s\S]*?\*\//g, "");
  const mobileStart = css.indexOf("@media (max-width: 900px)");
  assert.ok(mobileStart > -1, "the shared mobile block must exist");
  const mobile = css.slice(mobileStart);
  const bodiesFor = (source, selector) => [...source.matchAll(/([^{}]+)\{([^{}]*)\}/g)]
    .filter((match) => match[1].split(",").map((part) => part.trim()).includes(selector))
    .map((match) => match[2]);

  // The freshness dot is absolutely positioned inside the card, so its `left`
  // must clear the card's own left padding. It sat at 2rem while the row padded
  // by 2.5rem, which put the dot under the first glyph of the title: measured a
  // -2px gap at every width at or below 900px, i.e. an overlap. At 1rem the gap
  // is +14px.
  const dotRule = bodiesFor(mobile, ".col-freshness")[0];
  assert.ok(dotRule, "the mobile .col-freshness rule must exist");
  assert.match(dotRule, /position:\s*absolute/);
  assert.match(dotRule, /left:\s*1rem/);

  // With the label on the left, `space-between` pushed the value to the far
  // right edge of a full-width card, so values did not line up with each other.
  const cellRule = bodiesFor(mobile, ".job-cell")[0];
  assert.ok(cellRule, "the mobile .job-cell rule must exist");
  assert.match(cellRule, /justify-content:\s*flex-start/);
  assert.doesNotMatch(cellRule, /justify-content:\s*space-between/);
});

test("saved html exposes compact grouping controls and group header styling", () => {
  const savedHtml = fs.readFileSync(path.join(repoRoot, "saved.html"), "utf8");
  const savedCss = fs.readFileSync(path.join(repoRoot, "styles", "saved.css"), "utf8");

  assert.match(savedHtml, /id="saved-workspace-strip" data-ui="saved-workspace-strip" class="saved-workspace-strip hidden"[^>]+hidden/);
  assert.match(savedHtml, /id="saved-group-bar" data-ui="saved-group-bar"/);
  assert.match(savedHtml, /class="saved-group-label">Group<\/span>/);
  assert.match(savedHtml, /data-ui="group-btn" data-saved-group="none">None<\/button>/);
  assert.match(savedHtml, /data-ui="group-btn" data-saved-group="stage">Stage<\/button>/);
  assert.match(savedCss, /\.saved-sort-bar,\s*\.saved-group-bar\s*\{[\s\S]*display: flex;/);
  assert.match(savedCss, /\.saved-sort-btn,\s*\.saved-group-btn\s*\{[\s\S]*border-radius: 999px;/);
  assert.match(savedCss, /\.saved-group-section\s*\{[\s\S]*display: block;[\s\S]*margin: 0 0 0\.68rem;/);
  assert.match(savedCss, /\.saved-group-header\s*\{[\s\S]*border-top: 1px solid[\s\S]*border-bottom: 1px solid/);
  assert.match(savedCss, /\.saved-group-title\s*\{[\s\S]*text-transform: uppercase;/);
  assert.doesNotMatch(savedCss, /\.saved-group-section\s*\{[^}]*box-shadow:/);
});

test("jobs html exposes desktop update controls in the header shell", () => {
  const html = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  assert.match(html, /id="desktop-update-toggle-btn"/);
  assert.match(html, /id="desktop-update-panel"/);
  assert.match(html, /id="desktop-update-primary-btn"/);
  assert.match(html, /id="desktop-update-release-notes"/);
});

test("jobs html exposes first-slice read-only lifecycle filters", () => {
  const html = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  assert.match(html, /value="likely_removed">Recently removed<\/option>/);
  assert.match(html, /value="reappeared">Reappeared<\/option>/);
  assert.match(html, /value="preserved_source_failed">Preserved because source failed<\/option>/);
  assert.match(html, /styles\/jobs\.css/);
  assert.match(html, /frontend\/jobs\/index\.js/);
  assert.doesNotMatch(html, /preserved_source_skipped/);
});

test("desktop page startup shells avoid passive loading placeholders", () => {
  const jobsHtml = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  const savedHtml = fs.readFileSync(path.join(repoRoot, "saved.html"), "utf8");
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");

  assert.doesNotMatch(jobsHtml, /Loading jobs|Loading configured sources/);
  assert.match(jobsHtml, /class="jobs-table-header"/);
  assert.match(jobsHtml, /id="data-sources-list"[^>]*><\/ul>/);

  assert.doesNotMatch(savedHtml, /Loading saved jobs|No activity yet|Admin Checking/);
  assert.match(savedHtml, /id="saved-jobs-list"[^>]*><\/div>/);
  assert.match(savedHtml, /id="activity-panel-body"[^>]*><\/div>/);

  assert.doesNotMatch(adminHtml, /Loading admin overview|Loading operational signals|No discovery report loaded yet/);
  assert.match(adminHtml, /id="admin-action-center-items"[\s\S]*Checking operational signals\.\.\./);
});

test("desktop page titles keep the Baluffo window identity token", () => {
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");
  const savedHtml = fs.readFileSync(path.join(repoRoot, "saved.html"), "utf8");

  assert.match(adminHtml, /<title>Baluffo Admin<\/title>/);
  assert.match(savedHtml, /<title>Baluffo Saved Jobs<\/title>/);
});

test("admin html collapses advanced bulk actions before runtime layout", () => {
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");
  assert.match(adminHtml, /data-ui="admin-bulk-busy-message"[^>]+hidden/);
  assert.match(adminHtml, /<details data-ui="admin-advanced-bulk-actions" class="admin-advanced-bulk-details">/);
  assert.match(adminHtml, /<summary class="admin-advanced-bulk-summary">Advanced bulk actions<\/summary>/);
  assert.ok(adminHtml.indexOf('id="admin-approve-sources-btn"') < adminHtml.indexOf('data-ui="admin-advanced-bulk-actions"'));
  assert.ok(adminHtml.indexOf('id="admin-reject-sources-btn"') < adminHtml.indexOf('data-ui="admin-advanced-bulk-actions"'));
  assert.ok(adminHtml.indexOf('id="admin-restore-rejected-btn"') > adminHtml.indexOf('data-ui="admin-advanced-bulk-actions"'));
  assert.ok(adminHtml.indexOf('id="admin-demote-active-btn"') > adminHtml.indexOf('data-ui="admin-advanced-bulk-actions"'));
  assert.ok(adminHtml.indexOf('id="admin-delete-sources-btn"') > adminHtml.indexOf('data-ui="admin-advanced-bulk-actions"'));
  assert.match(adminHtml, /id="admin-approve-sources-btn"[\s\S]*Approve Selected<\/button>/);
  assert.match(adminHtml, /id="admin-reject-sources-btn"[\s\S]*Reject Selected<\/button>/);
  assert.match(adminHtml, /id="admin-restore-rejected-btn"[\s\S]*Restore Selected<\/button>/);
  assert.match(adminHtml, /id="admin-demote-active-btn"[\s\S]*Demote zero-jobs to Pending<\/button>/);
  assert.match(adminHtml, /id="admin-delete-sources-btn"[\s\S]*Delete Selected<\/button>/);
});

test("admin html groups operations health into overview discovery source-policy and dedup tabs", () => {
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");
  assert.match(adminHtml, /role="tablist" aria-label="Operations health sections"/);
  assert.match(adminHtml, /data-ops-tab="overview"/);
  assert.match(adminHtml, /data-ops-tab="discovery"/);
  assert.match(adminHtml, /data-ops-tab="source-policy"/);
  assert.match(adminHtml, /data-ops-tab="dedup"/);
  assert.doesNotMatch(adminHtml, /<h4 class="admin-section-title">Fetcher Metrics<\/h4>/);
  assert.match(adminHtml, /data-ui="admin-ops-tab-badge" data-ops-tab="overview" aria-hidden="true" title="Loading count">\.\.\.<\/span>/);
  assert.match(adminHtml, /data-ui="admin-ops-tab-badge" data-ops-tab="discovery" aria-hidden="true" title="Loading count">\.\.\.<\/span>/);
  assert.match(adminHtml, /data-ui="admin-ops-tab-badge" data-ops-tab="source-policy" aria-hidden="true" title="Loading count">\.\.\.<\/span>/);
  assert.match(adminHtml, /data-ui="admin-ops-tab-badge" data-ops-tab="dedup" aria-hidden="true" title="Loading count">\.\.\.<\/span>/);
  assert.match(adminHtml, /id="admin-ops-tab-overview"/);
  assert.match(adminHtml, /id="admin-ops-tab-discovery"[^>]+hidden/);
  assert.match(adminHtml, /id="admin-discovery-review"/);
  assert.match(adminHtml, /id="admin-ops-tab-source-policy"[^>]+hidden/);
  assert.match(adminHtml, /id="admin-ops-tab-dedup"[^>]+hidden/);
  assert.match(adminHtml, /<h4 class="admin-section-title">Operations Activity<\/h4>\s*<div id="admin-ops-history"/);
  assert.ok(adminHtml.indexOf('id="admin-ops-history"') < adminHtml.indexOf('role="tablist" aria-label="Operations health sections"'));
  assert.match(adminHtml, /<details class="admin-ops-trends-details">\s*<summary>Run trends<\/summary>\s*<div id="admin-ops-trends"/);
});
