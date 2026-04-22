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

function countLines(relPath) {
  return fs.readFileSync(repoPath(relPath), "utf8").split(/\r?\n/).length;
}

function listJsFiles(dir) {
  const out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const absolute = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      out.push(...listJsFiles(absolute));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".js")) {
      out.push(absolute);
    }
  }
  return out;
}

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
    const runtimeDir = repoPath("frontend", slice, "app", "runtime");
    assert.equal(fs.existsSync(runtimePath), true, `Missing runtime module for ${slice}`);
    assert.equal(fs.existsSync(domPath), true, `Missing DOM module for ${slice}`);
    assert.equal(fs.existsSync(runtimeDir), true, `Missing runtime helper directory for ${slice}`);
  }
});

test("cleanup structure: admin app keeps centralized fetcher preset metadata behind the stable root", () => {
  const rootRel = path.join("frontend", "admin", "app", "fetcher.js");
  const rootSource = fs.readFileSync(repoPath(rootRel), "utf8");
  const rootImports = readImports(rootRel);
  const leafSource = fs.readFileSync(repoPath("frontend", "admin", "app", "fetcher", "presets.js"), "utf8");

  assert.equal(rootImports.includes("./fetcher/presets.js"), true);
  assert.match(rootSource, /export\s*\{\s*FETCHER_PRESET_META\s*\}\s*from\s*"\.\/fetcher\/presets\.js"/);
  assert.doesNotMatch(rootSource, /const FETCHER_PRESET_META\s*=\s*\{/);
  assert.match(leafSource, /const FETCHER_PRESET_META\s*=\s*\{/);
  assert.match(leafSource, /const FETCHER_FALLBACK_MESSAGES\s*=\s*\{/);
  assert.match(leafSource, /\bdefault:\s*\{/);
  assert.match(leafSource, /\bincremental:\s*\{/);
  assert.match(leafSource, /\bforce_full:\s*\{/);
  assert.match(leafSource, /\bretry_failed:\s*\{/);
  assert.match(leafSource, /function applyFetcherPresetMetadata\(/);
  assert.doesNotMatch(leafSource, /compatibility URI fallback/i);
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
  const runtimeLocalPattern = /^(\.\.\/(actions|domain|data-source|render|services|state|parsing-utils|state-sync\/index)\.js|\.\/[A-Za-z0-9-]+\.js|\.\/runtime\/[A-Za-z0-9-]+\.js)$/;
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

test("cleanup structure: app runtime helper modules stay slice-local", () => {
  const slices = ["jobs", "saved", "admin"];
  const allowedImportPattern = /^(\.\.\/[A-Za-z0-9-]+\.js|\.\/[A-Za-z0-9-]+\.js)$/;
  const allowedSharedRuntimeImportPattern = /^\.\.\/\.\.\/\.\.\/shared\/[A-Za-z0-9_/-]+\.js$/;
  const allowedCompositionSliceImportPattern = /^\.\.\/\.\.\/(actions|domain|render|services)\.js$/;
  const blockedCrossSlicePattern = /^(\.\.\/)+(jobs|saved|admin)\//;

  for (const slice of slices) {
    const runtimeDir = repoPath("frontend", slice, "app", "runtime");
    const files = fs.readdirSync(runtimeDir).filter(name => name.endsWith(".js"));
    for (const fileName of files) {
      const rel = path.join("frontend", slice, "app", "runtime", fileName);
      const imports = readImports(rel);
      for (const specifier of imports) {
        assert.equal(
          blockedCrossSlicePattern.test(specifier),
          false,
          `Runtime helper must not import cross-slice modules: ${rel} -> ${specifier}`
        );
        const isCompositionHelper = slice === "admin" && fileName === "composition.js";
        assert.equal(
          allowedImportPattern.test(specifier) ||
            allowedSharedRuntimeImportPattern.test(specifier) ||
            (isCompositionHelper && allowedCompositionSliceImportPattern.test(specifier)) ||
            specifier.startsWith("/"),
          true,
          `Unexpected runtime helper import in ${rel}: ${specifier}`
        );
      }
    }
  }
});

test("cleanup structure: runtime entrypoints stay within the current size budget", () => {
  const budgets = {
    jobs: 1920,
    saved: 1920,
    admin: 320
  };

  for (const [slice, maxLines] of Object.entries(budgets)) {
    const rel = path.join("frontend", slice, "app", "runtime.js");
    const lines = countLines(rel);
    assert.equal(
      lines <= maxLines,
      true,
      `frontend/${slice}/app/runtime.js is ${lines} lines, above the ${maxLines}-line budget`
    );
  }
});

test("cleanup structure: admin controller roots stay within the current size budget", () => {
  const budgets = {
    fetcher: 450,
    discovery: 420,
    registry: 260,
    ops: 240
  };

  for (const [name, maxLines] of Object.entries(budgets)) {
    const rel = path.join("frontend", "admin", "app", `${name}.js`);
    const lines = countLines(rel);
    assert.equal(
      lines <= maxLines,
      true,
      `${rel} is ${lines} lines, above the ${maxLines}-line budget`
    );
  }
});

test("cleanup structure: admin controller roots stay thin composition surfaces", () => {
  const cases = [
    {
      name: "registry",
      functions: ["createAdminRegistryController"],
      importPattern: /^\.\/registry\/[A-Za-z0-9-]+\.js$/
    },
    {
      name: "ops",
      functions: ["createAdminOpsController"],
      importPattern: /^\.\/ops\/[A-Za-z0-9-]+\.js$/
    }
  ];

  for (const { name, functions, importPattern } of cases) {
    const rel = path.join("frontend", "admin", "app", `${name}.js`);
    const source = fs.readFileSync(repoPath(rel), "utf8");
    const imports = readImports(rel);
    const ownedFunctions = Array.from(
      source.matchAll(/\bfunction\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(/g),
      match => match[1]
    );

    assert.deepEqual(
      ownedFunctions,
      functions,
      `${rel} should only own the stable controller surface`
    );
    assert.doesNotMatch(
      source,
      /\bclass\s+[A-Za-z_][A-Za-z0-9_]*\s*/,
      `${rel} must not regain class ownership`
    );
    for (const specifier of imports) {
      assert.match(
        specifier,
        importPattern,
        `Unexpected admin ${name} root dependency: ${specifier}`
      );
    }
  }
});

test("cleanup structure: admin runtime root delegates controller composition", () => {
  const rel = path.join("frontend", "admin", "app", "runtime.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);

  assert.equal(
    imports.includes("./runtime/composition.js"),
    true,
    "frontend/admin/app/runtime.js must import ./runtime/composition.js"
  );
  assert.doesNotMatch(
    source,
    /\bfunction\s+composeControllers\s*\(/,
    "frontend/admin/app/runtime.js must not keep inline controller composition"
  );
});

test("cleanup structure: admin domain root stays a thin export surface", () => {
  const rel = path.join("frontend", "admin", "domain.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);

  assert.equal(
    countLines(rel) <= 40,
    true,
    `frontend/admin/domain.js is ${countLines(rel)} lines, above the 40-line thin-surface budget`
  );
  assert.doesNotMatch(
    source,
    /\bfunction\s+[A-Za-z_][A-Za-z0-9_]*\s*\(/,
    "frontend/admin/domain.js must stay a re-export surface"
  );
  assert.doesNotMatch(
    source,
    /\bclass\s+[A-Za-z_][A-Za-z0-9_]*\s*/,
    "frontend/admin/domain.js must not regain class ownership"
  );
  for (const specifier of imports) {
    assert.match(
      specifier,
      /^\.\/domain\/[A-Za-z0-9-]+\.js$/,
      `Unexpected admin domain root dependency: ${specifier}`
    );
  }
});

test("cleanup structure: admin render root stays a thin export surface", () => {
  const rel = path.join("frontend", "admin", "render.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);

  assert.equal(
    countLines(rel) <= 40,
    true,
    `frontend/admin/render.js is ${countLines(rel)} lines, above the 40-line thin-surface budget`
  );
  assert.doesNotMatch(
    source,
    /\bfunction\s+[A-Za-z_][A-Za-z0-9_]*\s*\(/,
    "frontend/admin/render.js must stay a re-export surface"
  );
  assert.doesNotMatch(
    source,
    /\bclass\s+[A-Za-z_][A-Za-z0-9_]*\s*/,
    "frontend/admin/render.js must not regain class ownership"
  );
  for (const specifier of imports) {
    assert.match(
      specifier,
      /^\.\/render\/[A-Za-z0-9-]+\.js$/,
      `Unexpected admin render root dependency: ${specifier}`
    );
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

test("cleanup structure: page slices reach local-data only through slice services", () => {
  const slices = ["jobs", "saved", "admin"];
  const allowedImport = "../local-data/services.js";

  for (const slice of slices) {
    const serviceRel = path.join("frontend", slice, "services.js");
    const serviceImports = readImports(serviceRel);
    assert.equal(
      serviceImports.includes(allowedImport),
      true,
      `Expected frontend/${slice}/services.js to own the local-data boundary`
    );

    const sliceRoot = repoPath("frontend", slice);
    for (const absolute of listJsFiles(sliceRoot)) {
      const rel = path.relative(ROOT, absolute);
      if (rel === serviceRel) {
        continue;
      }
      const imports = readImports(rel);
      for (const specifier of imports) {
        assert.equal(
          specifier.includes("local-data/services.js"),
          false,
          `Only slice services.js may import local-data/services.js: ${rel} -> ${specifier}`
        );
      }
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
