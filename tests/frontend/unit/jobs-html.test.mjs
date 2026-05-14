import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..", "..", "..");

test("jobs html pipeline button includes tooltip about long-running pipeline", () => {
  const html = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  assert.match(html, /id="jobs-pipeline-run-btn"/);
  assert.match(
    html,
    /data-tooltip="Runs discovery, fetch, and sync pipeline\. Can take more than 5 minutes\."/
  );
  assert.doesNotMatch(html, /id="jobs-pipeline-run-btn"[^>]+title=/);
});

test("desktop html meaningful operational buttons expose polished tooltips", () => {
  const jobsHtml = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  const savedHtml = fs.readFileSync(path.join(repoRoot, "saved.html"), "utf8");
  const adminHtml = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");

  [
    [jobsHtml, /id="country-picker-clear-btn"[^>]+data-tooltip="Clear the current country selection\."/],
    [jobsHtml, /id="customize-quick-filters-btn"[^>]+data-tooltip="Choose which quick filter presets are shown\."/],
    [jobsHtml, /id="quick-filters-reset-btn"[^>]+data-tooltip="Restore the default quick filter presets\."/],
    [jobsHtml, /id="refresh-jobs-btn"[^>]+data-tooltip="Reload the current jobs feed from local data\."/],
    [savedHtml, /id="add-custom-job-btn"[^>]+data-tooltip="Create a personal saved job entry\."/],
    [savedHtml, /id="global-phase-override-btn"[^>]+data-tooltip="Temporarily allow phase changes that are normally locked\."/],
    [savedHtml, /id="export-backup-btn"[^>]+data-tooltip="Export saved jobs, notes, and optional files to a backup\."/],
    [savedHtml, /id="import-backup-btn"[^>]+data-tooltip="Import a saved jobs backup into the current local profile\."/],
    [savedHtml, /id="activity-refresh-btn"[^>]+data-tooltip="Reload the activity timeline\."/],
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

  assert.doesNotMatch(`${jobsHtml}\n${savedHtml}\n${adminHtml}`, /id="(?:country-picker-clear-btn|customize-quick-filters-btn|quick-filters-reset-btn|refresh-jobs-btn|add-custom-job-btn|global-phase-override-btn|export-backup-btn|import-backup-btn|activity-refresh-btn|admin-refresh-btn|admin-refresh-ops-btn|admin-run-discovery-btn|admin-load-discovery-btn|admin-add-manual-source-btn|admin-approve-sources-btn|admin-reject-sources-btn|admin-restore-rejected-btn|admin-delete-sources-btn)"[^>]+\stitle=/);
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
  assert.match(html, /frontend\/jobs\/index\.js\?v=4/);
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
