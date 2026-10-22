import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import {
  DESKTOP_UNREACHABLE_MODULES,
  PAGES,
  PAGE_BUDGETS,
  assertDesktopComposition,
  bundlePage
} from "../../../scripts/build_desktop_frontend.mjs";

const execFileAsync = promisify(execFile);
const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const SCRIPT = path.join(REPO_ROOT, "scripts", "build_desktop_frontend.mjs");
const OUT_ROOT = path.join(REPO_ROOT, "_out", "desktop-frontend-bundle-test");

const JOBS_PAGE = PAGES.find(page => page.name === "jobs");
const MODULE_SCRIPT_RE = /<script\s+type="module"\s+src="([^"]+)"\s*><\/script>/g;
const EARLY_DATA_RE = /<script\s+src="frontend\/jobs\/early-data\.js"/;

async function buildVersionDir() {
  await fs.rm(OUT_ROOT, { recursive: true, force: true });
  await fs.mkdir(OUT_ROOT, { recursive: true });
  for (const page of PAGES) {
    await fs.writeFile(
      path.join(OUT_ROOT, page.html),
      await fs.readFile(path.join(REPO_ROOT, page.html), "utf8")
    );
  }
  return OUT_ROOT;
}

async function bundle() {
  const { stdout } = await execFileAsync(process.execPath, [SCRIPT, "--root", OUT_ROOT], {
    cwd: REPO_ROOT
  });
  return JSON.parse(stdout.trim().split("\n").at(-1));
}

function pagePayload(payload, name) {
  const page = payload.pages.find(entry => entry.name === name);
  assert.ok(page, `bundle payload is missing the ${name} page`);
  return page;
}

test("desktop ship build rewrites every page to one hashed asset next to its modules", async () => {
  const versionDir = await buildVersionDir();
  const payload = await bundle();

  assert.deepEqual(
    payload.pages.map(page => page.name).sort(),
    PAGES.map(page => page.name)
  );

  for (const page of PAGES) {
    const bundled = pagePayload(payload, page.name);
    assert.equal(bundled.entry, page.entry);
    const assetRe = new RegExp(`^${path.posix.dirname(page.entry)}/${page.name}\\.[0-9a-f]{12}\\.js$`);
    assert.match(bundled.asset, assetRe);
    const asset = await fs.stat(path.join(versionDir, bundled.asset));
    assert.equal(asset.size, bundled.bytes);
    // The build reports its own headroom, and must stay inside the page's
    // checked-in budget on every real build.
    assert.equal(bundled.shellBudgetBytes, PAGE_BUDGETS[page.name].shellMaxBytes);
    assert.ok(bundled.shellBytes >= bundled.bytes);
    assert.ok(
      bundled.shellBytes <= bundled.shellBudgetBytes,
      `${page.name} shell is ${bundled.shellBytes} bytes, over ${bundled.shellBudgetBytes}`
    );

    const html = await fs.readFile(path.join(versionDir, page.html), "utf8");
    assert.deepEqual(
      [...html.matchAll(MODULE_SCRIPT_RE)].map(match => match[1]),
      [bundled.asset],
      `${page.html} should load exactly its bundled asset`
    );
    assert.doesNotMatch(html, new RegExp(`src="${page.entry.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}"`));
  }
});

test("each page's bundle carries the page's own boot module", async () => {
  const versionDir = await buildVersionDir();
  const payload = await bundle();

  for (const page of PAGES) {
    const bundled = pagePayload(payload, page.name);
    const source = await fs.readFile(path.join(versionDir, bundled.asset), "utf8");
    // The entry's first statement is page-specific, so a bundle swapped between
    // pages would fail here instead of silently serving the wrong page.
    const marker = `${page.name}_module_boot_start`;
    assert.ok(source.includes(marker), `${bundled.asset} is missing ${marker}`);
    assert.ok(bundled.bytes > 100_000, `${page.name} bundle looks too small: ${bundled.bytes} bytes`);
  }
});

test("the admin shell defers the ops surface to a lazy chunk the entry only imports on demand", async () => {
  const versionDir = await buildVersionDir();
  const payload = await bundle();

  const admin = pagePayload(payload, "admin");
  assert.ok(admin.chunks.length >= 2, "admin should split into a shared chunk plus a lazy chunk");

  const entry = await fs.readFile(path.join(versionDir, admin.asset), "utf8");
  // Every chunk the entry imports statically is part of the shell; a chunk it
  // only names inside `import()` must stay out of that set, or the "lazy" half
  // of the split is just a second request on the critical path.
  const staticallyImported = new Set(
    [...entry.matchAll(/from"(\.\/chunks\/[^"]+)"/g)].map(match => path.posix.basename(match[1]))
  );
  const lazyChunks = [];
  for (const chunk of admin.chunks) {
    const file = path.join(versionDir, chunk.asset);
    assert.equal((await fs.stat(file)).size, chunk.bytes, `${chunk.asset} size mismatch`);
    const source = await fs.readFile(file, "utf8");
    if (!staticallyImported.has(path.basename(chunk.asset))) lazyChunks.push(source);
  }
  assert.equal(lazyChunks.length, 1, "exactly one admin chunk should load lazily");
  // The shell's statically imported chunk is a serialized round trip after the
  // entry parses unless the page preloads it alongside the entry.
  const html = await fs.readFile(path.join(versionDir, admin.html), "utf8");
  const preloads = [...html.matchAll(/<link\s+rel="modulepreload"\s+href="([^"]+)"/g)].map(
    match => match[1]
  );
  assert.deepEqual(
    preloads,
    [...staticallyImported].map(name => `frontend/admin/chunks/${name}`)
  );
  assert.ok(!preloads.some(href => path.posix.basename(href) === path.basename(admin.chunks.find(chunk => !staticallyImported.has(path.posix.basename(chunk.asset))).asset)));
  // The Ops hydration code owns this route, so its presence is proof the
  // deferred half is the Ops surface rather than an arbitrary shared chunk.
  assert.ok(lazyChunks[0].includes("/ops/task-failure-attempts"));
  assert.ok(!entry.includes("/ops/task-failure-attempts"));

  assert.deepEqual(pagePayload(payload, "saved").chunks, [], "saved should stay a single asset");
});

test("the jobs shell defers the pipeline watch and the update panel to lazy chunks", async () => {
  const versionDir = await buildVersionDir();
  const payload = await bundle();

  const jobs = pagePayload(payload, "jobs");
  const entry = await fs.readFile(path.join(versionDir, jobs.asset), "utf8");
  const staticallyImported = new Set(
    [...entry.matchAll(/from"(\.\/chunks\/[^"]+)"/g)].map(match => path.posix.basename(match[1]))
  );
  const lazySources = [];
  for (const chunk of jobs.chunks) {
    const source = await fs.readFile(path.join(versionDir, chunk.asset), "utf8");
    if (!staticallyImported.has(path.posix.basename(chunk.asset))) lazySources.push(source);
  }

  // Two surfaces are deferred: the update panel (with the release-notes dialog
  // only it opens) and the pipeline controller behind the Update button.
  assert.equal(lazySources.length, 2, "jobs should defer exactly two chunks");
  assert.ok(
    lazySources.some(
      source => source.includes("Baluffo is up to date") && source.includes("release-notes")
    ),
    "one lazy chunk should be the desktop update surface"
  );
  assert.ok(
    lazySources.some(source => source.includes("/ops/dashboard-health?view=summary")),
    "one lazy chunk should be the pipeline controller"
  );
  for (const marker of ["Baluffo is up to date", "/ops/dashboard-health?view=summary"]) {
    assert.ok(!entry.includes(marker), `the shell should not carry ${marker}`);
  }

  // The shell's static chunks are a serialized round trip after the entry parses
  // unless the page preloads them; the lazy ones must stay out of the preloads.
  const html = await fs.readFile(path.join(versionDir, jobs.html), "utf8");
  const preloads = [...html.matchAll(/<link\s+rel="modulepreload"\s+href="([^"]+)"/g)].map(
    match => match[1]
  );
  assert.deepEqual(preloads, [...staticallyImported].map(name => `frontend/jobs/chunks/${name}`));
  assert.deepEqual(
    [...html.matchAll(MODULE_SCRIPT_RE)].map(match => match[1]),
    [jobs.asset],
    "jobs.html should still load exactly one entry asset"
  );
});

test("the jobs page keeps the parse-time early-data hoist, the other pages have none", async () => {
  const versionDir = await buildVersionDir();
  await bundle();

  const jobsHtml = await fs.readFile(path.join(versionDir, "jobs.html"), "utf8");
  // Unlike the container page, the desktop version tree still ships early-data.js,
  // so the parse-time data fetch in front of the asset load stays.
  assert.match(jobsHtml, EARLY_DATA_RE);
  assert.match(jobsHtml, /data-startup-snapshot-url="data\/jobs-unified-startup\.json"/);

  for (const html of ["admin.html", "saved.html"]) {
    assert.doesNotMatch(await fs.readFile(path.join(versionDir, html), "utf8"), /early-data\.js/);
  }
});

test("re-running the desktop bundle is idempotent and prunes every previous asset", async () => {
  const versionDir = await buildVersionDir();
  const first = await bundle();
  const firstHtml = await Promise.all(
    PAGES.map(page => fs.readFile(path.join(versionDir, page.html), "utf8"))
  );

  const second = await bundle();

  assert.deepEqual(second.pages, first.pages);
  for (const [index, page] of PAGES.entries()) {
    const html = await fs.readFile(path.join(versionDir, page.html), "utf8");
    assert.equal(html, firstHtml[index], `${page.html} changed on the second run`);
    const assetDir = path.join(versionDir, path.posix.dirname(page.entry));
    const assets = (await fs.readdir(assetDir)).filter(name =>
      new RegExp(`^${page.name}\\.[0-9a-f]{12}\\.js$`).test(name)
    );
    assert.deepEqual(assets, [path.basename(pagePayload(second, page.name).asset)]);
    const chunksDir = path.join(assetDir, "chunks");
    const chunks = await fs.readdir(chunksDir).catch(() => []);
    assert.deepEqual(
      chunks.sort(),
      pagePayload(second, page.name).chunks.map(chunk => path.posix.basename(chunk.asset)).sort(),
      `${page.name} left stale chunks behind`
    );
  }
});

test("the desktop bundle fails when a shell outgrows its budget, before writing anything", async () => {
  const versionDir = await buildVersionDir();
  const page = JOBS_PAGE;

  await assert.rejects(
    bundlePage(versionDir, page, { shellMaxBytes: 1024, lazySurfaces: [] }),
    /jobs shell is \d+ bytes .*over its 1024-byte budget/
  );

  // A rejected build must leave the packaged page exactly as it was: no new
  // asset, no rewritten HTML.
  const assets = await fs.readdir(path.join(versionDir, "frontend/jobs")).catch(() => []);
  assert.deepEqual(assets.filter(name => /^jobs\.[0-9a-f]{12}\.js$/.test(name)), []);
  assert.match(
    await fs.readFile(path.join(versionDir, "jobs.html"), "utf8"),
    /<script type="module" src="frontend\/jobs\/index\.js"><\/script>/
  );
});

test("the desktop bundle fails when a lazy surface is back on the shell's path", async () => {
  const versionDir = await buildVersionDir();
  const page = JOBS_PAGE;

  await assert.rejects(
    bundlePage(versionDir, page, {
      shellMaxBytes: PAGE_BUDGETS.jobs.shellMaxBytes,
      // `dom.js` is imported by boot.js, so it rides the shell: this is exactly
      // the regression the guard exists for, expressed as a policy mistake.
      lazySurfaces: [{ name: "jobs DOM cache", module: "frontend/jobs/app/dom.js" }]
    }),
    /jobs lazy surface "jobs DOM cache" \(frontend\/jobs\/app\/dom\.js\) is not in any lazy chunk/
  );
});

test("every packaged page boots the desktop composition without the browser client", async () => {
  const versionDir = await buildVersionDir();
  const payload = await bundle();

  for (const page of PAGES) {
    const bundled = pagePayload(payload, page.name);
    const sources = [await fs.readFile(path.join(versionDir, bundled.asset), "utf8")];
    for (const chunk of bundled.chunks) {
      sources.push(await fs.readFile(path.join(versionDir, chunk.asset), "utf8"));
    }
    const source = sources.join("\n");
    assert.doesNotMatch(
      source,
      /IndexedDB open failed\./,
      `${page.name} still bundles the browser local-data client`
    );
    // The other half of the split: the packaged shell must keep the window's
    // heartbeat and close signals, which the hosted composition drops.
    assert.match(
      source,
      /confirmed_active_work_close/,
      `${page.name} lost the desktop session lifecycle`
    );
  }
});

test("the desktop composition guard rejects the browser and hosted compositions", () => {
  assert.deepEqual(
    assertDesktopComposition({ pageName: "jobs", pageInputs: ["frontend/jobs/page-entry.js"] }),
    undefined
  );
  for (const module of [
    "frontend/shared/local-data/browser-client.js",
    "frontend/shared/local-data/app-client.container.js"
  ]) {
    assert.ok(DESKTOP_UNREACHABLE_MODULES.includes(module));
    assert.throws(
      () => assertDesktopComposition({ pageName: "jobs", pageInputs: [module] }),
      /bundles a runtime the packaged app never runs/
    );
  }
});

test("the desktop bundle refuses a version dir missing any page", async () => {
  await fs.rm(OUT_ROOT, { recursive: true, force: true });
  await fs.mkdir(OUT_ROOT, { recursive: true });

  await assert.rejects(
    execFileAsync(process.execPath, [SCRIPT, "--root", OUT_ROOT], { cwd: REPO_ROOT }),
    /No admin\.html to bundle/
  );
});
