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

test("cleanup structure: legacy wrappers/bootstraps remain removed", () => {
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
