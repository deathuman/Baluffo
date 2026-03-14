import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  buildManifestContent,
  listUnitTestFiles,
  manifestPath,
} from "../../../scripts/sync_frontend_unit_manifest.mjs";

test("frontend unit manifest stays in sync with discovered test files", async () => {
  const discoveredFiles = await listUnitTestFiles();
  const manifestContent = await readFile(manifestPath, "utf8");

  assert.ok(
    discoveredFiles.includes("manifest-contract.test.mjs"),
    "manifest contract test must stay inside the discovered frontend unit suite",
  );
  assert.ok(
    !discoveredFiles.includes("all.test.mjs"),
    "generated manifest must not import itself as a test file",
  );
  assert.equal(
    manifestContent,
    buildManifestContent(discoveredFiles),
    "frontend unit manifest is stale; run `npm run sync:test-manifest`",
  );
});
