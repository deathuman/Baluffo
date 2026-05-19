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

test("desktop html meaningful operational buttons expose polished tooltips", () => {
  const jobsHtml = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  const savedHtml = fs.readFileSync(path.join(repoRoot, "saved.html"), "utf8");
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");

  [
    [jobsHtml, /id="country-picker-clear-btn"[^>]+data-tooltip="Clear the current country selection\."/],
    [jobsHtml, /id="customize-quick-filters-btn"[^>]+data-tooltip="Choose which preset filters are shown\."/],
    [jobsHtml, /id="quick-filters-reset-btn"[^>]+data-tooltip="Restore the default quick filter presets\."/],
    [jobsHtml, /id="refresh-jobs-btn"[^>]+data-tooltip="Reload the current local jobs data without checking sources\."/],
    [savedHtml, /id="history-panel-toggle-btn"[\s\S]*<svg viewBox="0 0 24 24"/],
    [savedHtml, /class="activity-toggle-label">Activity timeline<\/span>/],
    [adminHtml, /id="admin-refresh-btn"[^>]+data-tooltip="Reload users, totals, sources, and operational panels\."/],
    [adminHtml, /id="admin-refresh-ops-btn"[^>]+data-tooltip="Refresh operations health, run history, and alert summaries\."/],
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
  assert.doesNotMatch(`${jobsHtml}\n${savedHtml}\n${adminHtml}`, /id="(?:country-picker-clear-btn|customize-quick-filters-btn|quick-filters-reset-btn|refresh-jobs-btn|add-custom-job-btn|export-backup-btn|import-backup-btn|activity-refresh-btn|admin-refresh-btn|admin-refresh-ops-btn|admin-run-discovery-btn|admin-load-discovery-btn|admin-add-manual-source-btn|admin-approve-sources-btn|admin-reject-sources-btn|admin-restore-rejected-btn|admin-delete-sources-btn)"[^>]+\stitle=/);
});

test("saved activity toggle keeps text hidden behind the icon affordance", () => {
  const savedCss = fs.readFileSync(path.join(repoRoot, "styles", "saved.css"), "utf8");
  assert.match(savedCss, /\.activity-toggle-label,\s*\.activity-recent-badge\s*\{\s*display: none;/);
  assert.doesNotMatch(savedCss, /saved-ux-preview/);
  assert.doesNotMatch(savedCss, /\.saved-activity-close-btn\s*\{\s*display: none;/);
  assert.match(savedCss, /\.activity-recent-badge:not\(:empty\)\s*\{[\s\S]*position: absolute;[\s\S]*top: -0\.36rem;[\s\S]*right: -0\.36rem;/);
});

test("saved activity timeline uses the shared rounded scrollbar treatment", () => {
  const savedCss = fs.readFileSync(path.join(repoRoot, "styles", "saved.css"), "utf8");
  assert.match(savedCss, /\.activity-panel-body\s*\{[\s\S]*scrollbar-width: thin;[\s\S]*scrollbar-color: var\(--surface-18\) var\(--surface-1\);/);
  assert.match(savedCss, /\.activity-panel-body::-webkit-scrollbar-thumb\s*\{[\s\S]*background: var\(--surface-18\);[\s\S]*border-radius: 999px;[\s\S]*border: 2px solid var\(--surface-1\);/);
});

test("shared popups expose explicit light-theme presentation", () => {
  const componentsCss = fs.readFileSync(path.join(repoRoot, "styles", "components.css"), "utf8");
  assert.match(componentsCss, /\[data-theme="light"\]\s+\.popup-overlay\s*\{[\s\S]*rgba\(29,\s*39,\s*58,\s*0\.46\)/);
  assert.match(componentsCss, /\[data-theme="light"\]\s+\.popup\s*\{[\s\S]*#ffffff;[\s\S]*border-color:/);
  assert.match(componentsCss, /@supports \(\(-webkit-backdrop-filter: blur\(18px\)\) or \(backdrop-filter: blur\(18px\)\)\)\s*\{[\s\S]*\[data-theme="light"\]\s+\.popup\s*\{/);
  assert.match(componentsCss, /\[data-theme="light"\]\s+\.popup\s+\.popup-btn-primary\s*\{[\s\S]*#4769b2/);
  assert.match(componentsCss, /\[data-theme="light"\]\s+\.popup\s+\.popup-btn-tertiary\s*\{[\s\S]*#365175/);
  assert.match(componentsCss, /\[data-theme="light"\]\s+\.popup\s+select:not\(\[multiple\]\)\s*\{[\s\S]*stroke='%2340516a'/);
});

test("saved html exposes compact grouping controls and group header styling", () => {
  const savedHtml = fs.readFileSync(path.join(repoRoot, "saved.html"), "utf8");
  const savedCss = fs.readFileSync(path.join(repoRoot, "styles", "saved.css"), "utf8");

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

test("saved rows use compact hierarchy and bounded title layout", () => {
  const savedCss = fs.readFileSync(path.join(repoRoot, "styles", "saved.css"), "utf8");
  const renderController = fs.readFileSync(
    path.join(repoRoot, "frontend", "saved", "app", "runtime", "render-controller.js"),
    "utf8"
  );

  assert.match(renderController, /<div class="col-title">Position<\/div>\s*<div class="col-company">Company<\/div>\s*<div class="col-location">Location<\/div>/);
  assert.doesNotMatch(renderController, /<div class="col-sector">/);
  assert.doesNotMatch(renderController, /<div class="col-city">/);
  assert.doesNotMatch(renderController, /<div class="col-country">/);
  assert.doesNotMatch(renderController, /arrow\.textContent\s*=/);
  assert.match(renderController, /arrow\.classList\.toggle\("expanded", expanded\)/);
  assert.match(
    savedCss,
    /grid-template-columns: minmax\(17\.5rem, 1\.58fr\) minmax\(12\.5rem, 1\.05fr\) minmax\(10rem, 0\.68fr\) 7\.2rem 6\.2rem 3\.8rem;/
  );
  assert.match(savedCss, /\.saved-page \.jobs-table-header\s*\{[\s\S]*position: sticky;[\s\S]*top: 0;[\s\S]*z-index: 90;[\s\S]*width: 100%;[\s\S]*background-color: var\(--surface-2\);[\s\S]*box-shadow:/);
  assert.match(savedCss, /\.saved-row-header > div,\s*\.saved-job-row > \.job-cell\s*\{[\s\S]*width: 100%;/);
  assert.match(savedCss, /\.saved-title-main\s*\{[\s\S]*white-space: nowrap;[\s\S]*text-overflow: ellipsis;/);
  assert.match(
    savedCss,
    /\.saved-job-row \.job-company-compact,[\s\S]*\.saved-job-row \.job-city-sub\s*\{[\s\S]*white-space: nowrap;[\s\S]*text-overflow: ellipsis;/
  );
  assert.match(savedCss, /\.saved-row-header \.col-title,[\s\S]*\.saved-job-row \.col-location\s*\{[\s\S]*justify-self: stretch;/);
  assert.match(savedCss, /\.saved-job-row \.job-company-compact,[\s\S]*\.saved-job-row \.job-country-main\s*\{[\s\S]*font-weight: 600;/);
  assert.match(savedCss, /--saved-remove-size: 2\.25rem;/);
  assert.match(savedCss, /\.remove-inline-btn\s*\{[\s\S]*border-radius: 8px;/);
  assert.match(savedCss, /\.remove-inline-btn\s*\{[\s\S]*border-color: color-mix\(in srgb, #8f2f2f 58%, var\(--surface-12\)\);/);
  assert.match(savedCss, /\.remove-inline-btn svg\s*\{[\s\S]*width: 1\.05rem;[\s\S]*height: 1\.05rem;/);
  assert.match(savedCss, /\.details-toggle-arrow\s*\{[\s\S]*width: 0\.38rem;[\s\S]*border-right: 1\.5px solid currentColor;[\s\S]*transform: translateY\(-0\.02rem\) rotate\(-45deg\);/);
  assert.match(savedCss, /\.details-toggle-arrow\.expanded\s*\{[\s\S]*transform: translateY\(-0\.12rem\) rotate\(45deg\);/);
  assert.match(savedCss, /\.saved-job-block\s*\{[\s\S]*border-radius: 0 0 12px 12px;[\s\S]*box-shadow: 0 14px 34px/);
  assert.doesNotMatch(savedCss, /\.saved-job-block\.selected\b/);
  assert.match(savedCss, /\.outcome-compact\s*\{[\s\S]*display: flex;/);
  assert.match(savedCss, /\.phase-step-time\s*\{[\s\S]*display: none;/);
  assert.match(savedCss, /\.saved-phase-row\s*\{[\s\S]*grid-template-columns: minmax\(0, 1fr\);/);
  assert.match(savedCss, /\.phase-bar::before,[\s\S]*\.phase-bar::after\s*\{[\s\S]*top: 0\.66rem;/);
  assert.match(savedCss, /\.phase-bar::after\s*\{[\s\S]*--phase-progress-ratio/);
  assert.match(savedCss, /@media \(max-width: 900px\)\s*\{[\s\S]*\.saved-row-header\s*\{[\s\S]*display: none;/);
  assert.match(savedCss, /\.phase-timeline-step\.active \.phase-step-node\s*\{[\s\S]*width: 1\.55rem;[\s\S]*box-shadow:[\s\S]*rgba\(187, 134, 252, 0\.58\)/);
  assert.match(savedCss, /\.phase-timeline-step\.applied-reached:not\(\.active\) \.phase-step-node\s*\{[\s\S]*box-shadow: 0 0 0\.48rem rgba\(187, 134, 252, 0\.16\);/);
  assert.match(savedCss, /\.phase-step-applied-date\s*\{[\s\S]*white-space: nowrap;/);
  assert.doesNotMatch(savedCss, /\.phase-timeline-step\[data-phase-time\]::after/);
  assert.match(savedCss, /\.saved-tracking-action-row\s*\{[\s\S]*grid-template-columns: minmax\(14rem, auto\) minmax\(20rem, 1fr\) minmax\(15rem, auto\)/);
  assert.match(savedCss, /\.tracking-status-slot\s*\{[\s\S]*min-width: 0;/);
  assert.match(savedCss, /\.tracking-current-line\s*\{[\s\S]*display: flex;/);
  assert.match(savedCss, /\.tracking-final-indicator\s*\{[\s\S]*border-radius: 999px;/);
  assert.match(savedCss, /\.outcome-menu-toggle\s*\{[\s\S]*white-space: nowrap;/);
  assert.match(savedCss, /\.phase-change-popover\s*\{[\s\S]*position: absolute;/);
  assert.match(savedCss, /\.outcome-menu-toggle\s*\{[\s\S]*border-radius: 999px;[\s\S]*background: linear-gradient/);
  assert.match(savedCss, /\.outcome-menu > summary\.outcome-menu-toggle::after\s*\{[\s\S]*border-right: 1\.5px solid var\(--accent\);[\s\S]*transform: translateY\(-0\.12rem\) rotate\(45deg\);/);
  assert.match(savedCss, /\.outcome-menu-popover\s*\{[\s\S]*position: absolute;[\s\S]*min-width: 14rem;[\s\S]*box-shadow: 0 16px 34px/);
  assert.match(savedCss, /\.outcome-menu-item:hover,\s*\.outcome-menu-item:focus-visible\s*\{[\s\S]*outline: none;/);
  assert.match(savedCss, /@media \(max-width: 900px\)\s*\{[\s\S]*\.phase-bar\s*\{[\s\S]*overflow-x: auto;/);
  assert.doesNotMatch(savedCss, /\.outcome-bar\b/);
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
  assert.match(html, /frontend\/jobs\/index\.js\?v=12/);
  assert.doesNotMatch(html, /preserved_source_skipped/);
});

test("desktop page titles keep the Baluffo window identity token", () => {
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");
  const savedHtml = fs.readFileSync(path.join(repoRoot, "saved.html"), "utf8");

  assert.match(adminHtml, /<title>Baluffo Admin<\/title>/);
  assert.match(savedHtml, /<title>Baluffo Saved Jobs<\/title>/);
});

test("admin html no longer renders fetcher or discovery live-items markup", () => {
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");
  assert.doesNotMatch(adminHtml, /data-ui="admin-discovery-live-items"/);
  assert.doesNotMatch(adminHtml, /admin-discovery-live-card/);
  assert.doesNotMatch(adminHtml, /data-ui="admin-fetcher-live-items"/);
});

test("admin html leaves advanced bulk actions to the default runtime layout", () => {
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");
  assert.doesNotMatch(adminHtml, /admin-advanced-bulk-actions/);
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
  assert.match(adminHtml, /data-ui="admin-ops-tab-badge" data-ops-tab="overview" aria-hidden="true">0<\/span>/);
  assert.match(adminHtml, /data-ui="admin-ops-tab-badge" data-ops-tab="discovery" aria-hidden="true">0<\/span>/);
  assert.match(adminHtml, /data-ui="admin-ops-tab-badge" data-ops-tab="source-policy" aria-hidden="true">0<\/span>/);
  assert.match(adminHtml, /data-ui="admin-ops-tab-badge" data-ops-tab="dedup" aria-hidden="true">0<\/span>/);
  assert.match(adminHtml, /id="admin-ops-tab-overview"/);
  assert.match(adminHtml, /id="admin-ops-tab-discovery"[^>]+hidden/);
  assert.match(adminHtml, /id="admin-discovery-review"/);
  assert.match(adminHtml, /id="admin-ops-tab-source-policy"[^>]+hidden/);
  assert.match(adminHtml, /id="admin-ops-tab-dedup"[^>]+hidden/);
  assert.match(adminHtml, /<h4 class="admin-section-title">Operations Activity<\/h4>\s*<div id="admin-ops-history"/);
  assert.ok(adminHtml.indexOf('id="admin-ops-history"') < adminHtml.indexOf('role="tablist" aria-label="Operations health sections"'));
  assert.match(adminHtml, /<details class="admin-ops-trends-details">\s*<summary>Run trends<\/summary>\s*<div id="admin-ops-trends"/);
});
