#!/usr/bin/env node
import { build } from "esbuild";
import { createHash } from "node:crypto";
import { gzipSync } from "node:zlib";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const PAGES = [
  { name: "admin", html: "admin.html", entry: "frontend/admin/index.js" },
  { name: "jobs", html: "jobs.html", entry: "frontend/jobs/index.js" },
  { name: "saved", html: "saved.html", entry: "frontend/saved/index.js" }
];

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

function replaceModuleScript(html, assetPath) {
  const scriptRe = /<script\s+type="module"\s+src="[^"]+"\s*><\/script>/;
  if (!scriptRe.test(html)) {
    throw new Error("Could not find module script tag in container page HTML.");
  }
  return html.replace(scriptRe, `<script type="module" src="/container-assets/${assetPath}"></script>`);
}

async function buildPage(page, outDir) {
  const tempDir = path.join(outDir, ".tmp", page.name);
  await mkdir(tempDir, { recursive: true });
  const result = await build({
    absWorkingDir: ROOT,
    bundle: true,
    entryPoints: [page.entry],
    external: ["node:*"],
    format: "esm",
    legalComments: "none",
    metafile: true,
    minify: true,
    outdir: tempDir,
    sourcemap: false,
    splitting: false,
    target: ["es2022"],
    write: false,
    plugins: [stripQueryPlugin()]
  });
  const jsOutput = result.outputFiles.find(file => file.path.endsWith(".js"));
  if (!jsOutput) throw new Error(`No JavaScript output produced for ${page.name}.`);
  const hash = hashBytes(jsOutput.contents);
  const assetName = `${page.name}.${hash}.js`;
  const assetRelPath = `assets/${assetName}`;
  const assetPath = path.join(outDir, assetRelPath);
  await mkdir(path.dirname(assetPath), { recursive: true });
  await writeFile(assetPath, jsOutput.contents);
  await writeGzipSidecar(assetPath, jsOutput.contents);

  const sourceHtml = await readFile(path.join(ROOT, page.html), "utf8");
  const generatedHtml = replaceModuleScript(sourceHtml, assetRelPath);
  const htmlPath = path.join(outDir, page.html);
  await writeFile(htmlPath, generatedHtml);

  return {
    html: page.html,
    entry: page.entry,
    asset: assetRelPath,
    bytes: jsOutput.contents.length,
    gzipBytes: gzipSync(jsOutput.contents, { level: 9 }).length,
    sha256: createHash("sha256").update(jsOutput.contents).digest("hex")
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

main().catch(error => {
  console.error(error);
  process.exit(1);
});
