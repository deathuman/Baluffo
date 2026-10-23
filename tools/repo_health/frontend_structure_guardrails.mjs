import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..", "..");

function repoPath(...parts) {
  return path.join(ROOT, ...parts);
}

function readImports(relPath) {
  const source = fs.readFileSync(repoPath(relPath), "utf8");
  const imports = [];
  // Both forms count: `import { x } from "./m.js"` and the side-effect
  // `import "./m.js"` a page entry uses to compose its runtime with its body.
  // A static dynamic `import("./m.js")` is deliberately not an import here --
  // the boundary rules are about what a module drags onto the page's own graph.
  const importRegex = /\bfrom\s+["']([^"']+)["']|(?:^|[^\w$.])import\s+["']([^"']+)["']/g;
  let match = importRegex.exec(source);
  while (match) {
    imports.push(normalizeImportSpecifier(match[1] || match[2]));
    match = importRegex.exec(source);
  }
  return imports;
}

function normalizeImportSpecifier(specifier) {
  return String(specifier || "").replace(/[?#].*$/, "");
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
  // A page index no longer boots the app inline: it composes a runtime with the
  // page body, which is what lets the container and desktop builds reuse the body
  // with their own local-data client. The direct app import moved with the body,
  // so the invariant is checked as a pair -- the index boots exactly that body,
  // and the body boots exactly its sibling app module.
  const checks = [
    path.join("frontend", "jobs"),
    path.join("frontend", "saved"),
    path.join("frontend", "admin")
  ];
  for (const dir of checks) {
    const rel = path.join(dir, "index.js");
    const bodyRel = path.join(dir, "page-entry.js");
    const imports = readImports(rel);
    assert.equal(imports.includes("./page-entry.js"), true, `Expected ${rel} to boot ./page-entry.js`);
    assert.equal(imports.includes("./app.js"), false, `${rel} must keep the boot body in page-entry.js`);
    const bodyImports = readImports(bodyRel);
    assert.equal(bodyImports.includes("./app.js"), true, `Expected direct app import in ${bodyRel}`);
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
  // A render leaf is as canonical as the `render.js` barrel next to it, and the
  // Admin runtime must use leaves: importing the barrel from the shell drags the
  // lazily loaded Ops renderers back onto the boot path.
  const runtimeLocalPattern = /^(\.\.\/(actions|domain|data-source|render|services|state|parsing-utils|state-sync\/index)\.js|\.\.\/render\/[A-Za-z0-9-]+\.js|\.\/[A-Za-z0-9-]+\.js|\.\/runtime\/[A-Za-z0-9-]+\.js)$/;
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
  // Composition may take a render/dataset leaf as well as the layer barrel: the
  // Admin shell imports `render/overview.js` directly so the barrel's ops
  // renderers stay out of its boot bundle.
  const allowedCompositionSliceImportPattern = /^\.\.\/\.\.\/(actions|domain|render|services)(\/[A-Za-z0-9-]+)?\.js$/;
  // A runtime helper may reach one level into its own slice's subdirectory when
  // it wires a controller that slice/app/runtime.js must not inline (it has a
  // 320-line budget). Slice-internal only: jobs/, saved/, and admin/ stay
  // blocked, as does anything reaching a sibling slice.
  const allowedSliceSubdirPattern = /^\.\.\/(ops|render|domain|state-sync|app)\/[A-Za-z0-9-]+\.js$/;
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
            allowedSliceSubdirPattern.test(specifier) ||
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
    jobs: 360,
    saved: 360,
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

test("cleanup structure: jobs runtime root delegates composition and page flow", () => {
  const rel = path.join("frontend", "jobs", "app", "runtime.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);

  assert.equal(
    imports.includes("./runtime/composition.js"),
    true,
    "frontend/jobs/app/runtime.js must import ./runtime/composition.js"
  );
  assert.equal(
    imports.includes("./runtime/boot.js"),
    true,
    "frontend/jobs/app/runtime.js must import ./runtime/boot.js"
  );
  assert.equal(
    imports.includes("./runtime/page-flow.js"),
    true,
    "frontend/jobs/app/runtime.js must import ./runtime/page-flow.js"
  );
  assert.doesNotMatch(
    source,
    /\bcreateJobs(RuntimeState|FiltersController|AuthController|PipelineController|StartupPreviewController|EventsController|FeedController|UrlPersistence)\s*\(/,
    "frontend/jobs/app/runtime.js must not recreate the jobs controller/runtime graph inline"
  );
});

test("cleanup structure: saved runtime root delegates composition and leaf helpers", () => {
  const rel = path.join("frontend", "saved", "app", "runtime.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);
  const requiredImports = [
    "./runtime/composition.js",
    "./runtime/boot.js",
    "./runtime/phase-time.js",
    "./runtime/mutations.js",
    "./runtime/chrome.js",
    "./runtime/notes.js"
  ];

  for (const specifier of requiredImports) {
    assert.equal(
      imports.includes(specifier),
      true,
      `frontend/saved/app/runtime.js must import ${specifier}`
    );
  }
  assert.doesNotMatch(
    source,
    /\bcreateSaved(PageState|ActivityController|AttachmentsController|CustomJobController|RenderController|AuthController)\s*\(/,
    "frontend/saved/app/runtime.js must not recreate the saved controller/runtime graph inline"
  );
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

test("cleanup structure: admin ops render root stays a thin export surface", () => {
  const rel = path.join("frontend", "admin", "render", "ops.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);

  assert.equal(
    countLines(rel) <= 20,
    true,
    `frontend/admin/render/ops.js is ${countLines(rel)} lines, above the 20-line thin-surface budget`
  );
  assert.doesNotMatch(
    source,
    /\bfunction\s+[A-Za-z_][A-Za-z0-9_]*\s*\(/,
    "frontend/admin/render/ops.js must stay a re-export surface"
  );
  assert.doesNotMatch(
    source,
    /\bclass\s+[A-Za-z_][A-Za-z0-9_]*\s*/,
    "frontend/admin/render/ops.js must not regain class ownership"
  );
  for (const specifier of imports) {
    assert.match(
      specifier,
      /^\.\/ops-(summary|history)\.js$/,
      `Unexpected admin ops render dependency: ${specifier}`
    );
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

test("cleanup structure: desktop local-data client stays a thin root", () => {
  const rel = path.join("frontend", "shared", "local-data", "desktop-client.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);

  assert.equal(
    countLines(rel) <= 60,
    true,
    `frontend/shared/local-data/desktop-client.js is ${countLines(rel)} lines, above the 60-line thin-surface budget`
  );
  for (const specifier of [
    "./bridge-client.js",
    "./desktop/lifecycle-session.js",
    "./desktop/state.js"
  ]) {
    assert.equal(
      imports.includes(specifier),
      true,
      `Expected desktop-client.js to delegate through ${specifier}`
    );
  }
  // The desktop composition is the only place the window session is wired in;
  // the hosted composition reaches the same client without it.
  assert.match(source, /startDesktopLifecycle/);
  assert.match(source, /stopDesktopLifecycle/);
  assert.doesNotMatch(
    source,
    /\bcreateLocalDataRuntime\s*\(/,
    "desktop-client.js must not rebuild the runtime implementation"
  );
  assert.doesNotMatch(
    source,
    /\brequestJson\s*\(/,
    "desktop-client.js must not regain request helper ownership"
  );
  assert.doesNotMatch(
    source,
    /window\.addEventListener\(/,
    "desktop-client.js must not regain lifecycle event wiring"
  );
});

test("cleanup structure: the bridge client never reaches the browser or desktop session code", () => {
  // This module is the hosted composition's whole local-data surface. If it ever
  // imports the browser client or the session lifecycle, both runtimes pay for
  // the other one again -- the build guards catch it only after someone builds.
  const rel = path.join("frontend", "shared", "local-data", "bridge-client.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);

  for (const specifier of [
    "./browser-client.js",
    "./desktop-client.js",
    "./desktop/lifecycle-session.js",
    "../local-data/indexeddb-adapter.js"
  ]) {
    assert.equal(imports.includes(specifier), false, `${rel} must not import ${specifier}`);
  }
  // The desktop session is a callback, not an import.
  assert.match(source, /startLifecycle/);
  assert.doesNotMatch(source, /window\.addEventListener\(/);
});

test("cleanup structure: shipped compositions own one runtime each", () => {
  const root = path.join("frontend", "shared", "local-data");
  const container = readImports(path.join(root, "app-client.container.js"));
  const desktop = readImports(path.join(root, "app-client.desktop.js"));

  assert.equal(container.includes("./bridge-client.js"), true);
  for (const forbidden of ["./browser-client.js", "./desktop-client.js", "./app-client.js"]) {
    assert.equal(container.includes(forbidden), false, `the hosted composition must not import ${forbidden}`);
  }
  assert.equal(desktop.includes("./desktop-client.js"), true);
  for (const forbidden of ["./browser-client.js", "./bridge-client.js", "./app-client.js"]) {
    assert.equal(desktop.includes(forbidden), false, `the desktop composition must not import ${forbidden}`);
  }

  // The page bodies are runtime-agnostic so both builds can reuse them.
  for (const slice of ["jobs", "admin", "saved"]) {
    const body = path.join("frontend", slice, "page-entry.js");
    const bodyImports = readImports(body);
    assert.equal(
      bodyImports.some(specifier => specifier.includes("local-data")),
      false,
      `${body} must not name a local-data runtime`
    );
  }
});

test("cleanup structure: jobs desktop update root stays a thin export surface", () => {
  const rel = path.join("frontend", "jobs", "app", "desktop-update.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);

  assert.equal(
    countLines(rel) <= 20,
    true,
    `frontend/jobs/app/desktop-update.js is ${countLines(rel)} lines, above the 20-line thin-surface budget`
  );
  assert.doesNotMatch(
    source,
    /\bfunction\s+[A-Za-z_][A-Za-z0-9_]*\s*\(/,
    "frontend/jobs/app/desktop-update.js must stay a re-export surface"
  );
  assert.doesNotMatch(
    source,
    /\bclass\s+[A-Za-z_][A-Za-z0-9_]*\s*/,
    "frontend/jobs/app/desktop-update.js must not regain class ownership"
  );
  for (const specifier of imports) {
    assert.match(
      specifier,
      /^\.\/desktop-update-(model|controller)\.js$/,
      `Unexpected jobs desktop-update root dependency: ${specifier}`
    );
  }
});

test("cleanup structure: jobs domain root stays a thin export surface", () => {
  const rel = path.join("frontend", "jobs", "domain.js");
  const source = fs.readFileSync(repoPath(rel), "utf8");
  const imports = readImports(rel);

  assert.equal(
    countLines(rel) <= 40,
    true,
    `frontend/jobs/domain.js is ${countLines(rel)} lines, above the 40-line thin-surface budget`
  );
  assert.doesNotMatch(
    source,
    /\bfunction\s+[A-Za-z_][A-Za-z0-9_]*\s*\(/,
    "frontend/jobs/domain.js must stay a re-export surface"
  );
  assert.doesNotMatch(
    source,
    /\bclass\s+[A-Za-z_][A-Za-z0-9_]*\s*/,
    "frontend/jobs/domain.js must not regain class ownership"
  );
  for (const specifier of imports) {
    assert.match(
      specifier,
      /^\.\/domain\/(query|feed|view)\.js$/,
      `Unexpected jobs domain root dependency: ${specifier}`
    );
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
