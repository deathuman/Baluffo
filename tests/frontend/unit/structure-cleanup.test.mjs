import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..", "..", "..");

function repoPath(...parts) {
  return path.join(ROOT, ...parts);
}

test("cleanup structure: removed wrappers/bootstraps remain removed", () => {
  const removed = [
    "jobs.js",
    "saved.js",
    "admin.js",
    "jobs-bootstrap.js",
    "admin-bootstrap.js",
    "baluffo-ui-utils.js",
    "baluffo-data-utils.js",
    path.join("frontend", "jobs", "state.js"),
    path.join("frontend", "saved", "state.js"),
    path.join("frontend", "admin", "state.js"),
    path.join("frontend", "jobs", "handlers.js"),
    path.join("frontend", "saved", "handlers.js"),
    path.join("frontend", "admin", "handlers.js")
  ];

  for (const rel of removed) {
    assert.equal(fs.existsSync(repoPath(rel)), false, `Expected removed file to stay deleted: ${rel}`);
  }
});

test("cleanup structure: page indexes boot direct from sibling app modules", () => {
  const checks = [
    path.join("frontend", "jobs", "index.js"),
    path.join("frontend", "saved", "index.js"),
    path.join("frontend", "admin", "index.js")
  ];
  for (const rel of checks) {
    const source = fs.readFileSync(repoPath(rel), "utf8");
    assert.match(source, /from "\.\/app\.js"/, `Expected direct app import in ${rel}`);
  }
});

test("cleanup structure: admin app defines centralized fetcher preset metadata", () => {
  const source = fs.readFileSync(repoPath("frontend", "admin", "app.js"), "utf8");
  assert.match(source, /const FETCHER_PRESET_META\s*=\s*\{/);
  assert.match(source, /const FETCHER_FALLBACK_MESSAGES\s*=\s*\{/);
  assert.match(source, /\bdefault:\s*\{/);
  assert.match(source, /\bincremental:\s*\{/);
  assert.match(source, /\bforce_full:\s*\{/);
  assert.match(source, /\bretry_failed:\s*\{/);
  assert.match(source, /function applyFetcherPresetMetadata\(\)/);
  assert.doesNotMatch(source, /compatibility URI fallback/i);
});

test("cleanup structure: jobs modules avoid legacy sheets symbol naming", () => {
  const jobsApp = fs.readFileSync(repoPath("frontend", "jobs", "app.js"), "utf8");
  const jobsDataSource = fs.readFileSync(repoPath("frontend", "jobs", "data-source.js"), "utf8");
  const sourceMetadata = fs.readFileSync(repoPath("frontend", "jobs", "source-metadata.js"), "utf8");

  assert.doesNotMatch(jobsApp, /\bLEGACY_SHEETS_SOURCE\b/);
  assert.doesNotMatch(jobsApp, /\blegacySheetsSource\b/);
  assert.doesNotMatch(jobsDataSource, /\blegacySheetsSource\b/);
  assert.doesNotMatch(sourceMetadata, /\blegacySheetsSource\b/);
});
