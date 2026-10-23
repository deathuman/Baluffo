#!/usr/bin/env node
import { build } from "esbuild";
import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// A split page keeps what its first paint needs in the entry asset and gives the
// surfaces it reaches through a dynamic `import()` their own chunks. Admin is the
// page where that pays: 620 KB of shell becomes 303,552, because the Ops surface
// is 273 KB and nothing before the first paint touches it.
//
// Jobs is deliberately *not* split, although the desktop build splits it. The
// only surface there is the pipeline controller, and the split is not free: the
// controller shares `app/pipeline.js` with the shell, so esbuild lifts that into
// a third asset the entry still imports statically. The shell ends up at 280,135
// bytes (86,877 -> 83,274 gzip across two requests) and the first row measured
// flat -- 394 ms against 394 ms, 10 alternating pairs at 4x CPU throttle, with a
// paired median of 4 ms. A 5% shell reduction below the noise floor does not pay
// for a second request, so the container jobs page keeps one asset.
//
// What all three pages share is the container-only stubs below: the desktop
// update panel on jobs, the browser (IndexedDB) local-data client on every page,
// and the desktop session lifecycle (heartbeat, close signal, active-work close
// veto) inside the bridge client. Together they take the jobs shell from
// 328,836 bytes to 254,269.
// The hosted composition: the bridge local-data client without the desktop
// session lifecycle and without the browser (IndexedDB) client. The page body is
// the same module the dev entry loads (`page-entry.js`); only the client differs.
export const CONTAINER_COMPOSITION = "frontend/shared/local-data/app-client.container.js";

export const PAGES = [
  {
    name: "admin",
    html: "admin.html",
    entry: "frontend/admin/index.js",
    pageEntry: "frontend/admin/page-entry.js",
    composition: CONTAINER_COMPOSITION,
    splitting: true,
    lazySurfaces: ["frontend/admin/app/ops.js"]
  },
  {
    name: "jobs",
    html: "jobs.html",
    entry: "frontend/jobs/index.js",
    pageEntry: "frontend/jobs/page-entry.js",
    composition: CONTAINER_COMPOSITION
  },
  {
    name: "saved",
    html: "saved.html",
    entry: "frontend/saved/index.js",
    pageEntry: "frontend/saved/page-entry.js",
    composition: CONTAINER_COMPOSITION
  }
];

// A page's shipped entry is its composition plus its page body. It is handed to
// esbuild as stdin rather than as a file: the pair is a build-time fact (which
// runtime this artifact serves), and a checked-in file would only exist to be
// imported by one builder. The composition is a real module either way, so the
// graph is what keeps the other runtime's code out.
export function pageEntrySource(page) {
  const resolveDir = path.posix.dirname(page.pageEntry);
  const relative = path.posix.relative(resolveDir, page.composition);
  const compositionSpecifier = relative.startsWith(".") ? relative : `./${relative}`;
  return `import "${compositionSpecifier}";\nimport "./${path.posix.basename(page.pageEntry)}";\n`;
}

// What the page must download, parse and run before it can paint, plus the
// surfaces that must sit behind a dynamic import. Mirrors the desktop build's
// guard so a split that quietly rots is a build failure and not a slower page.
export const SHELL_BUDGETS = {
  admin: { shellMaxBytes: 300 * 1024 },
  jobs: { shellMaxBytes: 256 * 1024 },
  saved: { shellMaxBytes: 222 * 1024 }
};

export function assertContainerShell({
  pageName,
  shellBytes,
  shellInputs = [],
  lazyChunkInputs = [],
  lazySurfaces = []
}) {
  const maxBytes = Number(SHELL_BUDGETS[pageName]?.shellMaxBytes);
  if (!Number.isFinite(maxBytes) || maxBytes <= 0) {
    throw new Error(`No shell budget for the ${pageName} page; add one to SHELL_BUDGETS.`);
  }
  if (shellBytes > maxBytes) {
    throw new Error(
      `${pageName} shell is ${shellBytes} bytes (${Math.round(shellBytes / 1024)} KB), `
      + `${shellBytes - maxBytes} bytes over its ${maxBytes}-byte budget. Move the new `
      + `weight behind a dynamic import, or raise "${pageName}.shellMaxBytes" in `
      + "SHELL_BUDGETS if the container page's first paint genuinely needs it."
    );
  }
  const shell = new Set(shellInputs);
  for (const surface of lazySurfaces) {
    if (shell.has(surface)) {
      throw new Error(
        `${pageName} loads ${surface} from its own asset again: something imports it `
        + "statically, so its code is parsed before the page's first paint."
      );
    }
    if (!lazyChunkInputs.includes(surface)) {
      throw new Error(
        `${pageName} no longer reaches ${surface} through a dynamic import; the `
        + "surface was removed or inlined, so the split it declares is stale."
      );
    }
  }
  return { shellBytes, maxBytes };
}

const EARLY_DATA_PATH = path.join(ROOT, "frontend", "jobs", "early-data.js");

// The desktop update panel is desktop-only chrome: boot mounts it behind
// `isDesktopRuntimeMode()`, and no container page can ever show it. The
// container asset is a single file, so the desktop build's lazily loaded
// surface would otherwise be parsed on every hosted jobs page load. Resolve it
// to the same controller contract with inert methods instead.
const DESKTOP_UPDATE_SURFACE_STUB = `
export function createJobsDesktopUpdateSurface() {
  return {
    mount: async () => {},
    startAutoCheck: () => {},
    checkForUpdates: async () => {},
    handlePrimaryAction: async () => {}
  };
}
`;

// Modules a container page must never reach. The compositions keep them out by
// construction -- the hosted page imports `app-client.container.js`, which
// imports neither the browser client nor the desktop session -- but a page module
// could still import one directly (a page helper reaching for
// `desktop-client.js` is the easy mistake), and the page would keep working while
// carrying the code. Rendered as a build failure instead.
export const CONTAINER_UNREACHABLE_MODULES = [
  "frontend/shared/local-data/app-client.js",
  "frontend/shared/local-data/app-client.desktop.js",
  "frontend/shared/local-data/desktop-client.js",
  "frontend/shared/local-data/browser-client.js",
  "frontend/shared/local-data/desktop/lifecycle-session.js",
  "frontend/local-data/indexeddb-adapter.js"
];

export function assertContainerComposition({ pageName, pageInputs = [] }) {
  const present = CONTAINER_UNREACHABLE_MODULES.filter(input => pageInputs.includes(input));
  if (present.length === 0) return;
  throw new Error(
    `${pageName} bundles a runtime the container never runs (${present.join(", ")}). `
      + "A hosted page boots `app-client.container.js`; if a page module needs the "
      + "bridge API or navigation, import `bridge-client.js` or "
      + "`desktop/navigation.js` instead of the dev or desktop composition."
  );
}

function moduleStubPlugin({ name, filter, stubPath, contents }) {
  const namespace = `${name}-stub`;
  return {
    name,
    setup(buildApi) {
      buildApi.onResolve({ filter }, () => ({ path: stubPath, namespace }));
      buildApi.onLoad({ filter: /.*/, namespace }, () => ({ contents, loader: "js" }));
    }
  };
}

// The one surface the composition split cannot express: the update panel is
// reached through a dynamic `import()` that only fires in desktop mode, and the
// container jobs page is deliberately not split (see PAGES), so esbuild would
// inline the whole panel into the shell. Resolve it to the same controller
// contract with inert methods.
function containerOnlyStubPlugins() {
  return [
    moduleStubPlugin({
      name: "container-desktop-only-stub",
      filter: /desktop-update-surface\.js$/,
      stubPath: "desktop-update-surface",
      contents: DESKTOP_UPDATE_SURFACE_STUB
    })
  ];
}

function parseArgs(argv) {
  const args = { outDir: path.join(ROOT, ".container-frontend") };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--out-dir") {
      args.outDir = path.resolve(argv[index + 1] || "");
      index += 1;
    }
  }
  return args;
}

function stripQueryPlugin() {
  return {
    name: "strip-import-query",
    setup(buildApi) {
      buildApi.onResolve({ filter: /[?#]/ }, async args => {
        if (args.pluginData?.queryStripped) return null;
        const cleanPath = args.path.split(/[?#]/, 1)[0];
        return buildApi.resolve(cleanPath, {
          importer: args.importer,
          kind: args.kind,
          namespace: args.namespace,
          pluginData: { queryStripped: true },
          resolveDir: args.resolveDir
        });
      });
    }
  };
}

function hashBytes(bytes) {
  return createHash("sha256").update(bytes).digest("hex").slice(0, 12);
}

async function writeGzipSidecar(filePath, bytes) {
  await writeFile(`${filePath}.gz`, gzipSync(bytes, { level: 9 }));
}

function relativeOutputPath(tempDir, outputPath) {
  return path.relative(tempDir, path.resolve(ROOT, outputPath)).split(path.sep).join("/");
}

// The metafile is the authority on what each asset contains: reading imports
// back out of minified output would be guessing, and a module that moves between
// the shell and a chunk must be visible to the guard below.
function pageMetafile(metafile, tempDir, page) {
  const outputs = new Map(
    Object.entries(metafile.outputs).map(([outputPath, output]) => [
      relativeOutputPath(tempDir, outputPath),
      output
    ])
  );
  // The entry is the virtual stdin module, so its output is identified by the
  // entry name (`entryNames: page.name`) rather than by an `entryPoint` path.
  const entryMeta = outputs.get(`${page.name}.js`);
  if (!entryMeta) {
    throw new Error(`esbuild produced no ${page.name}.js entry for ${page.pageEntry}.`);
  }
  const staticChunks = new Set(
    (entryMeta?.imports || [])
      .filter(entryImport => entryImport.kind === "import-statement")
      .map(entryImport => relativeOutputPath(tempDir, entryImport.path))
  );
  return { outputs, entryMeta, staticChunks };
}

// Chunks the entry imports statically are on the critical path just like the
// entry itself; the page cannot paint until they have run too.
function shellChunkBytes({ outputs, staticChunks }) {
  let total = 0;
  for (const chunkPath of staticChunks) total += Number(outputs.get(chunkPath)?.bytes) || 0;
  return total;
}

function shellInputs({ outputs, entryMeta, staticChunks }) {
  const inputs = new Set(Object.keys(entryMeta?.inputs || {}));
  for (const chunkPath of staticChunks) {
    for (const input of Object.keys(outputs.get(chunkPath)?.inputs || {})) inputs.add(input);
  }
  return [...inputs];
}

function allPageInputs({ outputs }) {
  return [...new Set(
    [...outputs.values()].flatMap(output => Object.keys(output.inputs || {}))
  )];
}

function lazyChunkInputs({ outputs, staticChunks }, page) {
  return [...outputs.entries()]
    .filter(([chunkPath]) => chunkPath !== `${page.name}.js` && !staticChunks.has(chunkPath))
    .flatMap(([, output]) => Object.keys(output.inputs || {}));
}

// The jobs page loads a classic script that starts its first data requests ahead
// of the module graph. A container page ships one bundled script, so there is no
// separate file to request -- but the head start still matters: the startup
// snapshot is generated on demand (~600 ms on a cold data dir) and that wait must
// overlap the asset download and parse, not follow it. Inline the same file with
// the same dataset contract rather than duplicating its logic here.
export function inlineEarlyDataScript(html, source, { required = false } = {}) {
  const tagRe = /\s*<script\s+src="frontend\/jobs\/early-data\.js"[\s\S]*?<\/script>/;
  if (!tagRe.test(html)) {
    if (!required) return html;
    // The hoist is the container page's only first-render head start: the page
    // loads one asset, so without it the snapshot request waits for the download
    // and parse instead of overlapping them. Losing that tag must fail the build
    // rather than quietly ship the slower page the hoist exists to replace.
    throw new Error(
      "jobs.html no longer loads frontend/jobs/early-data.js as a classic script; "
        + "the container build inlines it so the first-render requests start at parse time."
    );
  }
  if (String(source).includes("</script")) {
    throw new Error("Cannot inline frontend/jobs/early-data.js: it contains a closing script tag.");
  }
  return html.replace(
    tagRe,
    "\n  <!-- Same hoist as frontend/jobs/early-data.js, inlined because the container page\n"
    + "       has no module files to run ahead of; the dataset drives it. -->\n"
    + "  <script\n"
    + '    data-startup-snapshot-url="data/jobs-unified-startup.json"\n'
    + '    data-fetch-report-url="data/jobs-fetch-report.json"\n'
    + "  >\n"
    + `${String(source).trim()}\n`
    + "  </script>"
  );
}

// A chunk the entry imports statically is only discovered once the entry has
// parsed, so without a preload the split would trade one download for two
// serialized ones. Lazy chunks are deliberately left alone: preloading them
// would put their bytes back on the first paint's critical path.
function replaceModuleScript(html, assetPath, staticChunks = []) {
  const scriptRe = /<script\s+type="module"\s+src="[^"]+"\s*><\/script>/;
  if (!scriptRe.test(html)) {
    throw new Error("Could not find module script tag in container page HTML.");
  }
  const preloads = [...staticChunks]
    .sort()
    .map(chunk => `  <link rel="modulepreload" href="/container-assets/assets/${chunk}">`)
    .join("\n");
  const tag = `<script type="module" src="/container-assets/${assetPath}"></script>`;
  return html.replace(scriptRe, preloads ? `${preloads}\n  ${tag}` : tag);
}

export async function buildPage(page, outDir, earlyDataSource = "") {
  const tempDir = path.join(outDir, ".tmp", page.name);
  await mkdir(tempDir, { recursive: true });
  const hoistSource = earlyDataSource || await readFile(EARLY_DATA_PATH, "utf8");
  const result = await build({
    absWorkingDir: ROOT,
    bundle: true,
    chunkNames: `chunks/${page.name}-[hash]`,
    entryNames: page.name,
    stdin: {
      contents: pageEntrySource(page),
      resolveDir: path.join(ROOT, path.posix.dirname(page.pageEntry)),
      sourcefile: `${page.name}-entry.js`,
      loader: "js"
    },
    external: ["node:*"],
    format: "esm",
    legalComments: "none",
    metafile: true,
    minify: true,
    outdir: tempDir,
    sourcemap: false,
    splitting: Boolean(page.splitting),
    target: ["es2022"],
    write: false,
    plugins: [stripQueryPlugin(), ...containerOnlyStubPlugins()]
  });
  const jsOutput = result.outputFiles.find(file => file.path === path.join(tempDir, `${page.name}.js`));
  if (!jsOutput) throw new Error(`No JavaScript output produced for ${page.name}.`);
  const chunks = result.outputFiles
    .filter(file => file.path.endsWith(".js") && file !== jsOutput)
    .map(file => ({
      relativePath: path.relative(tempDir, file.path).split(path.sep).join("/"),
      contents: file.contents
    }));
  const hash = hashBytes(jsOutput.contents);
  const assetName = `${page.name}.${hash}.js`;
  const assetRelPath = `assets/${assetName}`;

  // The guard runs before anything is written, so a page that lost its split or
  // outgrew its budget fails the build instead of replacing a working page with
  // a slower one.
  const sourceHtml = await readFile(path.join(ROOT, page.html), "utf8");
  const analysis = pageMetafile(result.metafile, tempDir, page);
  assertContainerComposition({
    pageName: page.name,
    pageInputs: allPageInputs(analysis)
  });
  const staticChunks = [...analysis.staticChunks].sort();
  const generatedHtml = inlineEarlyDataScript(
    replaceModuleScript(sourceHtml, assetRelPath, staticChunks),
    hoistSource,
    { required: page.name === "jobs" }
  );
  const { shellBytes, maxBytes } = assertContainerShell({
    pageName: page.name,
    shellBytes: jsOutput.contents.length + shellChunkBytes(analysis),
    shellInputs: shellInputs(analysis),
    lazyChunkInputs: lazyChunkInputs(analysis, page),
    lazySurfaces: page.lazySurfaces || []
  });

  const assetPath = path.join(outDir, assetRelPath);
  await mkdir(path.dirname(assetPath), { recursive: true });
  await writeFile(assetPath, jsOutput.contents);
  await writeGzipSidecar(assetPath, jsOutput.contents);
  for (const chunk of chunks) {
    // Chunks keep the entry's `chunks/` layout: the entry imports them by
    // relative path (`./chunks/...`), so it must resolve under the same base.
    const chunkPath = path.join(outDir, "assets", chunk.relativePath);
    await mkdir(path.dirname(chunkPath), { recursive: true });
    await writeFile(chunkPath, chunk.contents);
    await writeGzipSidecar(chunkPath, chunk.contents);
  }
  await writeFile(path.join(outDir, page.html), generatedHtml);

  return {
    html: page.html,
    entry: page.pageEntry,
    composition: page.composition,
    asset: assetRelPath,
    bytes: jsOutput.contents.length,
    gzipBytes: gzipSync(jsOutput.contents, { level: 9 }).length,
    sha256: createHash("sha256").update(jsOutput.contents).digest("hex"),
    shellBytes,
    shellBudgetBytes: maxBytes,
    // `staticChunks` are on the page's critical path and preloaded by the HTML;
    // everything in `chunks` that is not listed here is fetched after paint.
    staticChunks: staticChunks.map(chunkPath => `assets/${chunkPath}`),
    chunks: chunks.map(chunk => ({
      asset: `assets/${chunk.relativePath}`,
      bytes: chunk.contents.length,
      gzipBytes: gzipSync(chunk.contents, { level: 9 }).length
    }))
  };
}

async function main() {
  const { outDir } = parseArgs(process.argv.slice(2));
  await rm(outDir, { recursive: true, force: true });
  await mkdir(outDir, { recursive: true });
  const pages = {};
  for (const page of PAGES) {
    pages[page.name] = await buildPage(page, outDir);
  }
  await rm(path.join(outDir, ".tmp"), { recursive: true, force: true });
  await writeFile(
    path.join(outDir, "manifest.json"),
    `${JSON.stringify({ generatedAt: new Date().toISOString(), pages }, null, 2)}\n`
  );
  console.log(`Container frontend assets written to ${outDir}`);
}

// Imported by the container-bundle unit test; only a direct invocation writes.
const invokedDirectly = Boolean(process.argv[1])
  && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (invokedDirectly) {
  main().catch(error => {
    console.error(error);
    process.exit(1);
  });
}
