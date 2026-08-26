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
