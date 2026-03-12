import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..", "..", "..");

test("jobs html pipeline button includes tooltip about long-running pipeline", () => {
  const html = fs.readFileSync(path.join(repoRoot, "jobs.html"), "utf8");
  assert.match(html, /id="jobs-pipeline-run-btn"/);
  assert.match(
    html,
    /title="Runs discovery, fetch, and sync pipeline\. Can take more than 5 minutes\."/
  );
});

