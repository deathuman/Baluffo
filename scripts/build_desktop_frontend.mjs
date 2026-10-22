#!/usr/bin/env node
/**
 * Bundle the desktop pages into one asset each, the way the container image
 * bundles its pages in `Dockerfile`.
 *
 * The packaged site serves the shipped tree through `SimpleHTTPRequestHandler`,
 * so an unbundled page costs one request per module -- ~115 files / ~700 KB in
 * the jobs page's graph alone. This rewrites `admin.html`, `jobs.html` and
 * `saved.html` inside a ship version folder to load one hashed asset each, and
 * leaves the unbundled module graph in place for `runtime-bootstrap` repairs and
 * for development.
 *
 * Admin and jobs split: each entry is a shell (page chrome, and the parts of the
 * page the first paint needs) and the surfaces behind it are reached through a
 * dynamic `import()`, so esbuild emits them as separate chunks that the entry
 * fetches after the shell is up. Admin defers the Ops surface
 * (`frontend/admin/app/ops/lazy.js`); jobs defers the pipeline status watch and
 * the desktop update panel. Each page still loads exactly one module tag; the
 * chunks are an implementation detail of that asset.
 *
 * Unlike the container build, the jobs page's classic `early-data.js` tag stays:
 * that file ships in the version tree, and its data requests still start at parse
 * time, ahead of the single asset fetch.
 *
 * Invoked by `scripts/desktop_frontend_bundle.py` during the ship build, before
 * the version folder is hashed.
 *
 * Usage: node scripts/build_desktop_frontend.mjs --root <ship version dir>
 */
import { build } from "esbuild";
import { createHash } from "node:crypto";
import { access, mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

// Mirrors the container build's page list, so a page cannot be bundled in one
// shipped artifact and forgotten in the other.
//
// The entries compose a runtime with the page body. The packaged app always runs
// against the bridge it launches, so these pages boot
// `app-client.desktop.js` -- the bridge client plus the window's session
// lifecycle -- and never the browser (IndexedDB) client the dev dispatcher
// carries. That is ~45 KB per page that the packaged shell used to parse, and the
// guard below fails the build if it comes back.
const DESKTOP_COMPOSITION = "frontend/shared/local-data/app-client.desktop.js";

export const PAGES = [
  {
    name: "admin",
    html: "admin.html",
    entry: "frontend/admin/index.js",
    pageEntry: "frontend/admin/page-entry.js",
    composition: DESKTOP_COMPOSITION,
    splitting: true
  },
  {
    name: "jobs",
    html: "jobs.html",
    entry: "frontend/jobs/index.js",
    pageEntry: "frontend/jobs/page-entry.js",
    composition: DESKTOP_COMPOSITION,
    splitting: true
  },
  {
    name: "saved",
    html: "saved.html",
    entry: "frontend/saved/index.js",
    pageEntry: "frontend/saved/page-entry.js",
    composition: DESKTOP_COMPOSITION
  }
];

// Modules a packaged page must never reach: the offline client, and the hosted
// or dev compositions that would bring it along.
export const DESKTOP_UNREACHABLE_MODULES = [
  "frontend/shared/local-data/app-client.js",
  "frontend/shared/local-data/app-client.container.js",
  "frontend/shared/local-data/browser-client.js",
  "frontend/local-data/indexeddb-adapter.js"
];

export function assertDesktopComposition({ pageName, pageInputs = [] }) {
  const present = DESKTOP_UNREACHABLE_MODULES.filter(input => pageInputs.includes(input));
  if (present.length === 0) return;
  throw new Error(
    `${pageName} bundles a runtime the packaged app never runs (${present.join(", ")}). `
      + "The desktop entry composes `app-client.desktop.js` with `page-entry.js`; a page "
      + "module that needs the bridge API should import `bridge-client.js`, not the dev "
      + "dispatcher."
  );
}

function pageEntrySource(page) {
  const resolveDir = path.posix.dirname(page.pageEntry);
  const relative = path.posix.relative(resolveDir, page.composition);
  const compositionSpecifier = relative.startsWith(".") ? relative : `./${relative}`;
  return `import "${compositionSpecifier}";\nimport "./${path.posix.basename(page.pageEntry)}";\n`;
}
const MODULE_SCRIPT_RE = /<script\s+type="module"\s+src="[^"]+"\s*><\/script>/g;

// A page's shell is what it must download, parse and run before its first
// paint: the entry asset plus every chunk the rewritten HTML preloads for it.
// The budgets leave a few KB of headroom and are deliberately tight -- growth is
// allowed, but only by editing the table below, which makes the cost visible in
// review instead of arriving as a quieter first paint.
export const PAGE_BUDGETS = {
  admin: {
    shellMaxBytes: 293 * 1024,
    lazySurfaces: [{ name: "Ops surface", module: "frontend/admin/app/ops.js" }]
  },
  jobs: {
    shellMaxBytes: 235 * 1024,
    lazySurfaces: [
      {
        name: "desktop update surface",
        module: "frontend/jobs/app/desktop-update-surface.js"
      },
      {
        name: "pipeline controller",
        module: "frontend/jobs/app/runtime/pipeline-controller.js"
      }
    ]
  },
  saved: {
    shellMaxBytes: 216 * 1024,
    lazySurfaces: []
  }
};

// Two ways a split rots, both silent at runtime and both caught here:
// the shell grows back past its budget, or a surface that was moved behind a
// dynamic import is imported by the shell again and rides the critical path.
// Runs before anything is written, so a failing build leaves the version tree
// exactly as it found it (the packaged page keeps working).
export function assertPageBundleBudget({
  pageName,
  entryBytes,
  staticChunkBytes = 0,
  shellInputs = [],
  lazyChunks = [],
  budget
}) {
  const maxBytes = Number(budget?.shellMaxBytes);
  if (!Number.isFinite(maxBytes) || maxBytes <= 0) {
    throw new Error(`No shell budget for the ${pageName} page; add one to PAGE_BUDGETS.`);
  }
  const shellBytes = Number(entryBytes) + Number(staticChunkBytes);
  const overBy = shellBytes - maxBytes;
  if (overBy > 0) {
    throw new Error(
      `${pageName} shell is ${shellBytes} bytes (${(shellBytes / 1024).toFixed(0)} KB), `
      + `${overBy} bytes over its ${maxBytes}-byte budget. Move the new weight behind a `
      + `dynamic import, or raise "${pageName}.shellMaxBytes" in PAGE_BUDGETS if the first `
      + "paint genuinely needs it."
    );
  }
  const shell = new Set(shellInputs);
  for (const surface of budget.lazySurfaces || []) {
    const owner = lazyChunks.find(chunk => (chunk.inputs || []).includes(surface.module));
    if (!owner) {
      throw new Error(
        `${pageName} lazy surface "${surface.name}" (${surface.module}) is not in any lazy chunk: `
        + "the shell imports it again, so its code is back on the page's critical path."
      );
    }
    if (shell.has(surface.module)) {
      throw new Error(
        `${pageName} lazy surface "${surface.name}" (${surface.module}) is also bundled into the shell.`
      );
    }
  }
  return { shellBytes, maxBytes };
}

function parseArgs(argv) {
  const args = { root: "" };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--root") {
      args.root = argv[index + 1] || "";
      index += 1;
      continue;
    }
    throw new Error(`Unknown or incomplete argument: ${value}`);
  }
  if (!args.root) throw new Error("Usage: build_desktop_frontend.mjs --root <ship version dir>");
  return args;
}

// Query-string imports are a frontend convention here (`?v=` cache-busting on
// module specifiers); esbuild cannot resolve them, the container build strips
// them the same way.
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

function pageOutDir(page) {
  return path.join(ROOT, ".tmp", "desktop-frontend", page.name);
}

async function bundleEntry(page, budget) {
  const result = await build({
    absWorkingDir: ROOT,
    bundle: true,
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
    outdir: pageOutDir(page),
    sourcemap: false,
    splitting: Boolean(page.splitting),
    entryNames: page.name,
    chunkNames: page.splitting ? `chunks/${page.name}-[hash]` : undefined,
    target: ["es2022"],
    write: false,
    plugins: [stripQueryPlugin()]
  });
  const outputs = result.outputFiles.filter(file => file.path.endsWith(".js"));
  const entry = outputs.find(file => file.path === path.join(pageOutDir(page), `${page.name}.js`));
  if (!entry) throw new Error(`esbuild produced no JavaScript for ${page.entry}`);
  const chunks = outputs
    .filter(file => file !== entry)
    .map(file => ({
      relativePath: path.relative(pageOutDir(page), file.path).split(path.sep).join("/"),
      contents: file.contents
    }))
    .sort((left, right) => left.relativePath.localeCompare(right.relativePath));
  // Chunks the entry imports statically are on the shell's critical path and are
  // only discovered after the entry parses. The metafile is the authority here
  // (the output is minified, so reading import statements back out of it would
  // be guessing at syntax); the page gets a `modulepreload` per entry.
  // The entry is the virtual stdin module, so its output is the one named after
  // the page (`entryNames: page.name`) rather than one with an `entryPoint` path.
  const entryOutput = Object.entries(result.metafile.outputs).find(
    ([outputPath]) => path.basename(outputPath) === `${page.name}.js`
  )?.[1];
  if (!entryOutput) throw new Error(`esbuild produced no ${page.name}.js entry for ${page.pageEntry}.`);
  const staticChunks = (entryOutput?.imports || [])
    .filter(entryImport => entryImport.kind === "import-statement")
    .map(entryImport =>
      path.relative(pageOutDir(page), path.resolve(ROOT, entryImport.path)).split(path.sep).join("/")
    );

  // The metafile is also the authority on *what* landed in each asset: a module
  // moved back into the shell disappears from the lazy chunk's inputs, which is
  // the signal the budget check below watches for.
  const outputMetaByPath = new Map(
    Object.entries(result.metafile.outputs).map(([outputPath, output]) => [
      outputPath.replaceAll("\\", "/"),
      output
    ])
  );
  const chunkInputs = chunk => Object.keys(
    outputMetaByPath.get(
      path.relative(ROOT, path.join(pageOutDir(page), chunk.relativePath)).split(path.sep).join("/")
    )?.inputs || {}
  );
  const staticChunkSet = new Set(staticChunks);
  const shellInputs = new Set(Object.keys(entryOutput?.inputs || {}));
  const staticChunkBytes = chunks
    .filter(chunk => staticChunkSet.has(chunk.relativePath))
    .reduce((total, chunk) => {
      for (const input of chunkInputs(chunk)) shellInputs.add(input);
      return total + chunk.contents.length;
    }, 0);
  const lazyChunks = chunks
    .filter(chunk => !staticChunkSet.has(chunk.relativePath))
    .map(chunk => ({ relativePath: chunk.relativePath, inputs: chunkInputs(chunk) }));

  assertDesktopComposition({
    pageName: page.name,
    pageInputs: [
      ...new Set([
        ...shellInputs,
        ...lazyChunks.flatMap(chunk => chunk.inputs)
      ])
    ]
  });

  const { shellBytes, maxBytes } = assertPageBundleBudget({
    pageName: page.name,
    entryBytes: entry.contents.length,
    staticChunkBytes,
    shellInputs: [...shellInputs],
    lazyChunks,
    budget
  });

  return { entry: entry.contents, chunks, staticChunks, shellBytes, shellBudgetBytes: maxBytes };
}

function assetLayout(page) {
  const assetDir = path.posix.dirname(page.entry);
  return { assetDir, assetPrefix: `${page.name}.` };
}

// Chunks live under a page-owned `chunks/` directory so pruning one page can
// never remove another page's asset or its own module graph.
async function pruneStaleAssets(assetDir, assetPrefix) {
  const names = await readdir(assetDir).catch(error => {
    if (error?.code === "ENOENT") return [];
    throw error;
  });
  await Promise.all(
    names
      .filter(name => name.startsWith(assetPrefix) && name.endsWith(".js"))
      .map(name => rm(path.join(assetDir, name), { force: true }))
  );
  await rm(path.join(assetDir, "chunks"), { recursive: true, force: true });
}

function dropPreviousPreloads(sourceHtml, assetDir) {
  const escaped = assetDir.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return sourceHtml.replace(
    new RegExp(`\\s*<link\\s+rel="modulepreload"\\s+href="${escaped}/chunks/[^"]+"\\s*/?>`, "g"),
    ""
  );
}

function rewritePage(sourceHtml, assetRelPath, staticChunks, assetDir, pagePath) {
  const tags = sourceHtml.match(MODULE_SCRIPT_RE) || [];
  if (tags.length !== 1) {
    throw new Error(`Expected one module script tag in ${pagePath}, found ${tags.length}`);
  }
  // Re-running the bundler on an already-bundled page must not stack another
  // set of preloads on top of the previous run's.
  const cleaned = dropPreviousPreloads(sourceHtml, assetDir);
  const preloads = staticChunks
    .map(chunk => `  <link rel="modulepreload" href="${assetDir}/${chunk}">`)
    .join("\n");
  const tag = `<script type="module" src="${assetRelPath}"></script>`;
  // One tag, one asset: replacing every module tag would hide a second entry
  // point, so the tag count is asserted above instead.
  return cleaned.replace(MODULE_SCRIPT_RE, preloads ? `${preloads}\n  ${tag}` : tag);
}

export async function bundlePage(versionRoot, page, budget = PAGE_BUDGETS[page.name]) {
  const pagePath = path.join(versionRoot, page.html);
  try {
    await access(pagePath);
  } catch {
    throw new Error(`No ${page.html} to bundle in ${versionRoot}`);
  }

  const { entry, chunks, staticChunks, shellBytes, shellBudgetBytes } = await bundleEntry(page, budget);
  const hash = createHash("sha256").update(entry).digest("hex").slice(0, 12);
  const { assetDir, assetPrefix } = assetLayout(page);
  const assetName = `${assetPrefix}${hash}.js`;
  const assetRelPath = `${assetDir}/${assetName}`;
  const assetDiskDir = path.join(versionRoot, assetDir);

  await mkdir(assetDiskDir, { recursive: true });
  await pruneStaleAssets(assetDiskDir, assetPrefix);
  await writeFile(path.join(assetDiskDir, assetName), entry);
  const writtenChunks = [];
  for (const chunk of chunks) {
    const chunkPath = path.join(assetDiskDir, chunk.relativePath);
    await mkdir(path.dirname(chunkPath), { recursive: true });
    await writeFile(chunkPath, chunk.contents);
    writtenChunks.push({
      asset: `${assetDir}/${chunk.relativePath}`,
      bytes: chunk.contents.length
    });
  }

  // Normalise line endings before rewriting. The preload strip below matches
  // with a leading `\s*`, which swallows the previous line's ending, and the
  // block it re-inserts is joined with "\n" -- so a CRLF page (a Windows
  // working copy under `core.autocrlf=true`, while the blob is LF) lost one
  // byte per preload line on the second run and the idempotency test failed.
  // Rewriting from LF input makes the output stable regardless of checkout.
  const source = (await readFile(pagePath, "utf8")).replace(/\r\n/g, "\n");
  const html = rewritePage(source, assetRelPath, staticChunks, assetDir, pagePath);
  await writeFile(pagePath, html);

  return {
    name: page.name,
    html: page.html,
    entry: page.entry,
    asset: assetRelPath,
    bytes: entry.length,
    hash,
    chunks: writtenChunks,
    // Reported so a build log shows the headroom instead of only the failure.
    shellBytes,
    shellBudgetBytes
  };
}

async function main() {
  const { root } = parseArgs(process.argv.slice(2));
  const versionRoot = path.resolve(root);
  const pages = [];
  for (const page of PAGES) {
    pages.push(await bundlePage(versionRoot, page));
  }
  console.log(JSON.stringify({ pages }));
}

// Imported by the bundle tests to exercise the budget guard against the real
// bundler; only a direct invocation bundles anything.
const invokedDirectly = Boolean(process.argv[1])
  && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (invokedDirectly) {
  main().catch(error => {
    console.error(String(error?.message || error));
    process.exit(1);
  });
}
