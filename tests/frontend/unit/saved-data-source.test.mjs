import test from "node:test";
import assert from "node:assert/strict";
import { parseBackupInputFile } from "../../../frontend/saved/data-source.js";

test("saved data-source parses json backup directly", async () => {
  const file = {
    name: "backup.json",
    type: "application/json",
    async text() {
      return JSON.stringify({ schemaVersion: 1, attachments: [] });
    }
  };
  const payload = await parseBackupInputFile(file);
  assert.equal(payload.schemaVersion, 1);
});
