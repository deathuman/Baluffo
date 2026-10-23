import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";

import {
  CONTAINER_COMPOSITION,
  CONTAINER_UNREACHABLE_MODULES,
  PAGES,
  SHELL_BUDGETS,
  assertContainerComposition,
  assertContainerShell,
  buildPage,
  inlineEarlyDataScript,
  pageEntrySource
} from "../../../scripts/build_container_frontend.mjs";

// Two failure modes are silent, and these assertions drive the real bundler
// because both of them look like a working build:
//
// - The container jobs page has no module files ahead of it: one bundled asset
//   is the whole graph, so its first-render head start is the `early-data.js`
//   hoist, inlined at parse time. The build used to leave the page untouched
//   when the classic tag was gone.
// - A page's shell is one asset, so a surface the page only reaches after its
//   first paint is parsed before that paint unless it is split into a chunk.
const JOBS_PAGE = PAGES.find(page => page.name === "jobs");
const ADMIN_PAGE = PAGES.find(page => page.name === "admin");
const ADMIN_OPS_MARKER_SOURCE = new URL(
  "../../../frontend/admin/app/ops/health-pipeline-schedule.js",
  import.meta.url
);
const INLINE_HOIST_RE =
  /<script\s+data-startup-snapshot-url="data\/jobs-unified-startup\.json"[\s\S]*?startJobsEarlyData[\s\S]*?<\/script>/;

async function buildPageInTemp(page, run) {
  const outDir = await mkdtemp(path.join(tmpdir(), `baluffo-container-${page.name}-`));
  try {
    return await run({ page: await buildPage(page, outDir), outDir });
  } finally {
    await rm(outDir, { recursive: true, force: true });
  }
}

const buildJobsPage = run => buildPageInTemp(JOBS_PAGE, run);

async function readAsset(outDir, asset) {
  return readFile(path.join(outDir, asset), "utf8");
}

function lazyChunks(page) {
  return page.chunks.filter(chunk => !page.staticChunks.includes(chunk.asset));
}

async function assertChunkFilesWritten({ page, outDir }) {
  for (const chunk of page.chunks) {
    const chunkPath = path.join(outDir, chunk.asset);
    assert.equal((await stat(chunkPath)).size, chunk.bytes, `${chunk.asset} size mismatch`);
    const gzipSidecar = await stat(`${chunkPath}.gz`);
    assert.ok(gzipSidecar.size > 0, `${chunk.asset} needs its gzip sidecar`);
    assert.ok(chunk.bytes > 0);
  }
}

test("the container jobs page inlines the early-data hoist ahead of its shell", async () => {
  await buildJobsPage(async ({ page, outDir }) => {
    const html = await readFile(path.join(outDir, "jobs.html"), "utf8");

    assert.match(html, INLINE_HOIST_RE, "jobs.html must inline the early-data hoist");
    assert.match(html, /data-fetch-report-url="data\/jobs-fetch-report\.json"/);
    assert.doesNotMatch(
      html,
      /<script\s+src="frontend\/jobs\/early-data\.js"/,
      "the container page must not request early-data.js as a separate file"
    );
    assert.ok(
      html.indexOf("startJobsEarlyData") < html.indexOf('type="module"'),
      "the hoist must run before the module script, not after it"
    );

    const moduleSrc = html.match(/<script type="module" src="([^"]+)"><\/script>/)?.[1];
    assert.match(String(moduleSrc), /^\/container-assets\/assets\/jobs\.[a-f0-9]{12}\.js$/);
    const assetPath = path.join(outDir, moduleSrc.replace("/container-assets/", ""));
    assert.equal((await stat(assetPath)).size, page.bytes);
    assert.ok((await stat(`${assetPath}.gz`)).size > 0, "the asset needs its gzip sidecar");
  });
});

test("the container jobs page stays one asset within its shell budget", async () => {
  // Jobs is the measured-negative case: its only post-paint surface is 5% of
  // the shell, and splitting it added a shared chunk without moving the first
  // row (see PAGES in the build script). If a future split is justified, this
  // test is the one to rewrite along with that note.
  await buildJobsPage(async ({ page, outDir }) => {
    assert.deepEqual(page.chunks, [], "the container jobs page must stay one asset");
    assert.equal(page.shellBytes, page.bytes);
    assert.ok(page.shellBytes <= SHELL_BUDGETS.jobs.shellMaxBytes);

    const html = await readFile(path.join(outDir, "jobs.html"), "utf8");
    assert.doesNotMatch(html, /modulepreload/, "one asset means nothing to preload");
    assert.equal(html.match(/<script type="module"/g)?.length, 1);
  });
});

test("the container admin page keeps the ops surface in a lazy chunk", async () => {
  // Probe the marker in source first: a literal the minifier cannot rename, so
  // its absence from the entry means the ops surface never entered the shell.
  assert.match(
    await readFile(ADMIN_OPS_MARKER_SOURCE, "utf8"),
    /Pipeline schedule interval must be between 1 and 168 hours/
  );

  await buildPageInTemp(ADMIN_PAGE, async ({ page, outDir }) => {
    const html = await readFile(path.join(outDir, "admin.html"), "utf8");
    const entry = await readAsset(outDir, page.asset);
    assert.doesNotMatch(entry, /between 1 and 168 hours/);

    const lazy = lazyChunks(page);
    const sources = await Promise.all(
      lazy.map(async chunk => ({ chunk, source: await readAsset(outDir, chunk.asset) }))
    );
    const ops = sources.find(({ source }) => source.includes("between 1 and 168 hours"));
    assert.ok(ops, "the ops surface must live in a lazy chunk, not the admin shell");
    assert.doesNotMatch(
      html,
      new RegExp(`modulepreload"[^>]*${ops.chunk.asset.split("/").pop()}`),
      "a lazy chunk must not be preloaded onto the first paint's critical path"
    );

    // Static chunks are the opposite: only discovery through the entry would
    // serialize their download behind it, so the HTML preloads them.
    assert.ok(page.staticChunks.length > 0);
    for (const staticChunk of page.staticChunks) {
      assert.ok(
        html.includes(`<link rel="modulepreload" href="/container-assets/${staticChunk}">`),
        `${staticChunk} is on the critical path and must be preloaded`
      );
    }
    assert.ok(page.shellBytes <= SHELL_BUDGETS.admin.shellMaxBytes);
    assert.ok(page.chunks.length > page.staticChunks.length, "the split must defer something");
    await assertChunkFilesWritten({ page, outDir });
  });
});

test("the container jobs asset excludes the desktop-only update surface", async () => {
  const controllerSource = await readFile(
    new URL("../../../frontend/jobs/app/desktop-update-controller.js", import.meta.url),
    "utf8"
  );
  assert.match(controllerSource, /Baluffo is already up to date\./);

  await buildJobsPage(async ({ outDir, page }) => {
    const entry = await readAsset(outDir, page.asset);
    assert.doesNotMatch(
      entry,
      /Baluffo is already up to date\./,
      "desktop update copy must not be parsed on a container page load"
    );
  });
});

test("every container page drops the browser local-data stack", async () => {
  // The IndexedDB client is unreachable in container mode (the server always
  // serves `mode: "container", localDataMode: "bridge"`). A literal from the
  // adapter the minifier cannot rename is the probe: its absence means the whole
  // subtree is gone, and the hosted composition in the graph is why.
  assert.match(
    await readFile(
      new URL("../../../frontend/local-data/indexeddb-adapter.js", import.meta.url),
      "utf8"
    ),
    /IndexedDB open failed\./
  );

  for (const pageConfig of PAGES) {
    await buildPageInTemp(pageConfig, async ({ page, outDir }) => {
      const sources = [
        await readAsset(outDir, page.asset),
        ...(await Promise.all(page.chunks.map(chunk => readAsset(outDir, chunk.asset))))
      ];
      const bundle = sources.join("\n");
      assert.doesNotMatch(
        bundle,
        /IndexedDB open failed\./,
        `${page.name} still bundles the IndexedDB adapter`
      );
      assert.equal(page.composition, CONTAINER_COMPOSITION);
      // Container pages do keep the bridge client: that is the local-data path
      // they actually use, and stripping it would empty saved jobs.
      assert.match(bundle, /desktop-local-data/);
    });
  }
});

test("the container build rejects a page that carries another runtime's composition", () => {
  const session = "frontend/shared/local-data/desktop/lifecycle-session.js";
  const devDispatcher = "frontend/shared/local-data/app-client.js";
  assert.deepEqual(
    assertContainerComposition({
      pageName: "jobs",
      pageInputs: [
        "frontend/jobs/page-entry.js",
        CONTAINER_COMPOSITION,
        "frontend/shared/local-data/desktop/lifecycle.js"
      ]
    }),
    undefined
  );
  for (const module of [session, devDispatcher, "frontend/local-data/indexeddb-adapter.js"]) {
    assert.ok(CONTAINER_UNREACHABLE_MODULES.includes(module));
    assert.throws(
      () => assertContainerComposition({ pageName: "saved", pageInputs: [module] }),
      /bundles a runtime the container never runs/
    );
  }
});

test("a page's shipped entry is its composition plus its page body", async () => {
  const jobs = PAGES.find(page => page.name === "jobs");
  assert.equal(
    pageEntrySource(jobs),
    'import "../shared/local-data/app-client.container.js";\nimport "./page-entry.js";\n'
  );
  // The dev entry keeps the dispatcher for repo pages; the page body itself
  // names no runtime, which is what lets both builds reuse it.
  const devEntry = await readFile(
    new URL("../../../frontend/jobs/index.js", import.meta.url),
    "utf8"
  );
  assert.match(devEntry, /shared\/local-data\/app-client\.js/);
  assert.match(devEntry, /\.\/page-entry\.js/);
  const pageEntry = await readFile(
    new URL("../../../frontend/jobs/page-entry.js", import.meta.url),
    "utf8"
  );
  assert.doesNotMatch(pageEntry, /local-data/);
});

test("every container page drops the desktop session lifecycle", async () => {
  // A container page boots with `enableLifecycle: false`, so the heartbeat,
  // close signal and active-work close veto never run there. The two close
  // reasons are literals the minifier keeps, and `lifecycle.js`'s bootstrap
  // message is the control: it must survive, or the split took the bootstrap
  // with it.
  const sessionSource = await readFile(
    new URL("../../../frontend/shared/local-data/desktop/lifecycle-session.js", import.meta.url),
    "utf8"
  );
  for (const exported of ["startDesktopLifecycle", "stopDesktopLifecycle"]) {
    assert.match(
      sessionSource,
      new RegExp(`export function ${exported}\\(`),
      `the container stub contract must keep ${exported}`
    );
  }

  for (const pageConfig of PAGES) {
    await buildPageInTemp(pageConfig, async ({ page, outDir }) => {
      const sources = [
        await readAsset(outDir, page.asset),
        ...(await Promise.all(page.chunks.map(chunk => readAsset(outDir, chunk.asset))))
      ];
      const bundle = sources.join("\n");
      assert.doesNotMatch(
        bundle,
        /confirmed_active_work_close|active_work_close_attempt/,
        `${page.name} still bundles the desktop close-signal handlers`
      );
      assert.match(
        bundle,
        /\[desktop-local-data\] bootstrap failed/,
        `${page.name} lost the bridge bootstrap from lifecycle.js`
      );
    });
  }
});

test("the container shell guard rejects a split that has rotted", () => {
  const surface = "frontend/admin/app/ops.js";

  assert.deepEqual(
    assertContainerShell({
      pageName: "admin",
      shellBytes: 1000,
      shellInputs: ["frontend/admin/index.js"],
      lazyChunkInputs: [surface],
      lazySurfaces: [surface]
    }),
    { shellBytes: 1000, maxBytes: SHELL_BUDGETS.admin.shellMaxBytes }
  );

  // Back in the shell: something imports it statically again.
  assert.throws(
    () => assertContainerShell({
      pageName: "admin",
      shellBytes: 1000,
      shellInputs: [surface],
      lazyChunkInputs: [surface],
      lazySurfaces: [surface]
    }),
    /from its own asset again/
  );

  // Declared but no longer dynamic: the declaration is stale.
  assert.throws(
    () => assertContainerShell({
      pageName: "admin",
      shellBytes: 1000,
      shellInputs: ["frontend/admin/index.js"],
      lazyChunkInputs: [],
      lazySurfaces: [surface]
    }),
    /no longer reaches/
  );

  assert.throws(
    () => assertContainerShell({
      pageName: "admin",
      shellBytes: SHELL_BUDGETS.admin.shellMaxBytes + 1,
      shellInputs: [],
      lazyChunkInputs: [surface],
      lazySurfaces: [surface]
    }),
    /over its \d+-byte budget/
  );
});

test("a jobs page without the classic hoist tag fails the build", () => {
  assert.throws(
    () => inlineEarlyDataScript("<html><body></body></html>", "source", { required: true }),
    /early-data\.js/
  );
  // Pages without the tag are untouched, and a hoist that could break out of its
  // own script element is refused rather than shipped.
  assert.equal(inlineEarlyDataScript("<html></html>", "source"), "<html></html>");
  assert.throws(
    () => inlineEarlyDataScript(
      '<script src="frontend/jobs/early-data.js"></script>',
      'var broken = "</script";',
      { required: true }
    ),
    /closing script tag/
  );
});
