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
    /title="Runs discovery, fetch, and sync pipeline\. Can take more than 5 minutes\."/
  );
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
  assert.match(adminHtml, /<h4 class="admin-section-title">Run History<\/h4>\s*<div id="admin-ops-history"/);
});
