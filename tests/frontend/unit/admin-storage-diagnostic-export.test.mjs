import test from "node:test";
import assert from "node:assert/strict";

import {
  downloadStorageDiagnosticJson,
  serializeStorageDiagnosticExport,
  storageDiagnosticExportFilename
} from "../../../frontend/admin/app/storage-diagnostic-export.js";

const DIAGNOSTIC = {
  status: "complete",
  runId: "storage_diag_abc123",
  capturedAt: "2026-11-11T10:00:00.000Z",
  tableSizes: {
    status: "ok",
    method: "dbstat",
    tables: [{ name: "job_sources", tableBytes: 5_000, indexBytes: 1_000, bytes: 6_000, share: 0.75 }]
  },
  jobSources: {
    rowCount: 2,
    payloadBytes: 400,
    contentBytes: 500,
    byGeneration: [
      {
        generation: "gen-current",
        rowCount: 2,
        payloadBytes: 400,
        contentBytes: 500,
        isCurrent: true
      }
    ]
  }
};

test("builds a versioned incident envelope with exact table and generation totals", () => {
  const content = serializeStorageDiagnosticExport(
    DIAGNOSTIC,
    new Date("2026-11-11T10:05:00.000Z")
  );
  const payload = JSON.parse(content);

  assert.equal(payload.kind, "baluffo.storage-diagnostic");
  assert.equal(payload.schemaVersion, 1);
  assert.equal(payload.exportedAt, "2026-11-11T10:05:00.000Z");
  assert.equal(payload.storageDiagnostic.tableSizes.tables[0].bytes, 6_000);
  assert.equal(payload.storageDiagnostic.jobSources.byGeneration[0].generation, "gen-current");
  assert.equal(payload.storageDiagnostic.jobSources.byGeneration[0].contentBytes, 500);
  assert.ok(content.endsWith("\n"));
});

test("names the export with the captured diagnostic timestamp", () => {
  assert.equal(
    storageDiagnosticExportFilename(DIAGNOSTIC),
    "baluffo-storage-diagnostic-2026-11-11T10-00-00-000Z.json"
  );
});

test("downloads the export as a JSON blob through an anchor", () => {
  const link = {
    click() {
      clicked = true;
    },
    remove() {
      removed = true;
    }
  };
  let clicked = false;
  let removed = false;
  let appended = null;
  let revoked = null;
  let blobType = "";
  const documentRef = {
    createElement(tag) {
      assert.equal(tag, "a");
      return link;
    },
    body: {
      appendChild(node) {
        appended = node;
      }
    }
  };
  const urlApi = {
    createObjectURL(blob) {
      blobType = blob.type;
      return "blob:storage-diagnostic";
    },
    revokeObjectURL(url) {
      revoked = url;
    }
  };

  downloadStorageDiagnosticJson("diagnostic.json", "{}", { documentRef, urlApi });

  assert.equal(link.download, "diagnostic.json");
  assert.equal(link.href, "blob:storage-diagnostic");
  assert.equal(link.rel, "noopener");
  assert.equal(appended, link);
  assert.equal(clicked, true);
  assert.equal(removed, true);
  assert.equal(revoked, "blob:storage-diagnostic");
  assert.equal(blobType, "application/json");
});
