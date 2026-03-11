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

function readImports(relPath) {
  const source = fs.readFileSync(repoPath(relPath), "utf8");
  const imports = [];
  const importRegex = /from\s+["']([^"']+)["']/g;
  let match = importRegex.exec(source);
  while (match) {
    imports.push(match[1]);
    match = importRegex.exec(source);
  }
  return imports;
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

test("cleanup structure: canonical app runtime modules exist for each slice", () => {
  const slices = ["jobs", "saved", "admin"];
  for (const slice of slices) {
    const runtimePath = repoPath("frontend", slice, "app", "runtime.js");
    const domPath = repoPath("frontend", slice, "app", "dom.js");
    assert.equal(fs.existsSync(runtimePath), true, `Missing runtime module for ${slice}`);
    assert.equal(fs.existsSync(domPath), true, `Missing DOM module for ${slice}`);
  }
});

test("cleanup structure: admin app defines centralized fetcher preset metadata", () => {
  const source = fs.readFileSync(repoPath("frontend", "admin", "app", "runtime.js"), "utf8");
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

  assert.doesNotMatch(jobsApp, /\bLEGACY_SHEETS_SOURCE\b/);
  assert.doesNotMatch(jobsApp, /\blegacySheetsSource\b/);
  assert.doesNotMatch(jobsDataSource, /\blegacySheetsSource\b/);
  assert.doesNotMatch(jobsApp, /\blegacySheetsSource\b/);
});

test("cleanup structure: jobs and saved keep canonical slice file shape", () => {
  const requiredPerSlice = [
    "app.js",
    "actions.js",
    "domain.js",
    "data-source.js",
    "render.js",
    "services.js",
    "index.js",
    path.join("state-sync", "index.js")
  ];
  const slices = ["jobs", "saved", "admin"];

  for (const slice of slices) {
    for (const rel of requiredPerSlice) {
      const absolute = repoPath("frontend", slice, rel);
      assert.equal(fs.existsSync(absolute), true, `Missing required ${slice} module: ${rel}`);
    }
  }
});

test("cleanup structure: app modules import only canonical local layers", () => {
  const localLayerPattern = /^\.\/((actions|domain|data-source|render|services|state-sync\/index)\.js|app\/[A-Za-z0-9-]+\.js)$/;
  const sharedPattern = /^(\.\.\/shared\/|(\.\.\/){1,3}|\/)/;
  const slices = ["jobs", "saved", "admin"];

  for (const slice of slices) {
    const imports = readImports(path.join("frontend", slice, "app.js"));
    const localImports = imports.filter(specifier => specifier.startsWith("./"));
    for (const specifier of localImports) {
      assert.match(
        specifier,
        localLayerPattern,
        `Unexpected local app import in frontend/${slice}/app.js: ${specifier}`
      );
    }
    for (const specifier of imports) {
      assert.equal(
        sharedPattern.test(specifier) || specifier.startsWith("./"),
        true,
        `Unexpected import specifier in frontend/${slice}/app.js: ${specifier}`
      );
    }
  }
});

test("cleanup structure: app runtime modules import only canonical layers and local app helpers", () => {
  const runtimeLocalPattern = /^(\.\.\/(actions|domain|data-source|render|services|state-sync\/index)\.js|\.\/[A-Za-z0-9-]+\.js)$/;
  const sharedPattern = /^(\.\.\/shared\/|(\.\.\/){2,3}|\/)/;
  const slices = ["jobs", "saved", "admin"];

  for (const slice of slices) {
    const imports = readImports(path.join("frontend", slice, "app", "runtime.js"));
    for (const specifier of imports) {
      assert.equal(
        runtimeLocalPattern.test(specifier) || sharedPattern.test(specifier),
        true,
        `Unexpected import specifier in frontend/${slice}/app/runtime.js: ${specifier}`
      );
    }
  }
});

test("cleanup structure: non-app modules never import slice app entry", () => {
  const featureFiles = [
    path.join("frontend", "jobs", "app", "runtime.js"),
    path.join("frontend", "jobs", "actions.js"),
    path.join("frontend", "jobs", "domain.js"),
    path.join("frontend", "jobs", "data-source.js"),
    path.join("frontend", "jobs", "render.js"),
    path.join("frontend", "jobs", "services.js"),
    path.join("frontend", "jobs", "state-sync", "index.js"),
    path.join("frontend", "saved", "app", "runtime.js"),
    path.join("frontend", "saved", "actions.js"),
    path.join("frontend", "saved", "domain.js"),
    path.join("frontend", "saved", "data-source.js"),
    path.join("frontend", "saved", "render.js"),
    path.join("frontend", "saved", "services.js"),
    path.join("frontend", "saved", "state-sync", "index.js"),
    path.join("frontend", "admin", "app", "runtime.js"),
    path.join("frontend", "admin", "actions.js"),
    path.join("frontend", "admin", "domain.js"),
    path.join("frontend", "admin", "data-source.js"),
    path.join("frontend", "admin", "render.js"),
    path.join("frontend", "admin", "services.js"),
    path.join("frontend", "admin", "state-sync", "index.js")
  ];

  for (const rel of featureFiles) {
    const imports = readImports(rel);
    for (const specifier of imports) {
      assert.notEqual(
        specifier,
        "./app.js",
        `Disallowed dependency drift: ${rel} must not import app.js`
      );
    }
  }
});

test("cleanup structure: domain and render layers do not cross-import in feature slices", () => {
  const slices = ["jobs", "saved", "admin"];
  for (const slice of slices) {
    const domainImports = readImports(path.join("frontend", slice, "domain.js"));
    const renderImports = readImports(path.join("frontend", slice, "render.js"));
    assert.equal(
      domainImports.includes("./render.js"),
      false,
      `Disallowed layer dependency: frontend/${slice}/domain.js must not import render.js`
    );
    assert.equal(
      renderImports.includes("./domain.js"),
      false,
      `Disallowed layer dependency: frontend/${slice}/render.js must not import domain.js`
    );
  }
});

test("cleanup structure: shared layer does not depend on feature slices or wrong shared bucket", () => {
  const sharedUiImports = readImports(path.join("frontend", "shared", "ui", "index.js"));
  const sharedDataImports = readImports(path.join("frontend", "shared", "data", "index.js"));
  const featureSliceImportPattern = /^(\.\.\/)+(jobs|saved|admin)\//;

  for (const specifier of sharedUiImports) {
    assert.equal(
      featureSliceImportPattern.test(specifier),
      false,
      `shared/ui must not import feature slice modules: ${specifier}`
    );
  }
  for (const specifier of sharedDataImports) {
    assert.equal(
      featureSliceImportPattern.test(specifier),
      false,
      `shared/data must not import feature slice modules: ${specifier}`
    );
    assert.equal(
      specifier.startsWith("../ui/"),
      false,
      `shared/data must not import shared/ui modules: ${specifier}`
    );
  }
});
