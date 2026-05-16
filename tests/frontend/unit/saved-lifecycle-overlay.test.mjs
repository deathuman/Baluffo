import test from "node:test";
import assert from "node:assert/strict";
import {
  buildSavedLifecycleOverlayByJobKey,
  parseLifecycleStatePayload
} from "../../../frontend/saved/app/runtime/lifecycle-overlay.js";
import { generateJobKey } from "../../../frontend/local-data/job-utils.js";

test("saved lifecycle overlay parses lifecycle-state rows from keyed payload", () => {
  const rows = parseLifecycleStatePayload({
    jobs: {
      a: { title: "A" },
      b: { title: "B" }
    }
  });
  assert.equal(rows.length, 2);
});

test("saved lifecycle overlay prefers canonical rows and falls back to lifecycle rows", () => {
  const overlay = buildSavedLifecycleOverlayByJobKey({
    generateJobKeyForRow: row => generateJobKey(row),
    canonicalRows: [
      {
        title: "Gameplay Engineer",
        company: "Studio",
        city: "Rome",
        country: "Italy",
        jobLink: "https://example.com/jobs/1",
        status: "active",
        lifecycleEvent: "reappeared"
      }
    ],
    lifecycleRows: [
      {
        title: "Gameplay Engineer",
        company: "Studio",
        city: "Rome",
        country: "Italy",
        jobLink: "https://example.com/jobs/1",
        status: "likely_removed",
        removedAt: "2026-03-07T00:00:00.000Z"
      },
      {
        title: "Build Engineer",
        company: "Studio",
        city: "Milan",
        country: "Italy",
        jobLink: "https://example.com/jobs/2",
        status: "active",
        lifecycleEvent: "preserved",
        lifecycleReason: "source_failed"
      }
    ]
  });

  assert.equal(overlay.size, 2);
  const first = overlay.get(generateJobKey({
    title: "Gameplay Engineer",
    company: "Studio",
    city: "Rome",
    country: "Italy",
    jobLink: "https://example.com/jobs/1"
  }));
  const second = overlay.get(generateJobKey({
    title: "Build Engineer",
    company: "Studio",
    city: "Milan",
    country: "Italy",
    jobLink: "https://example.com/jobs/2"
  }));
  assert.deepEqual(first, {
    status: "active",
    removedAt: "",
    lastSeenAt: "",
    lifecycleEvent: "reappeared",
    lifecycleReason: ""
  });
  assert.deepEqual(second, {
    status: "active",
    removedAt: "",
    lastSeenAt: "",
    lifecycleEvent: "preserved",
    lifecycleReason: "source_failed"
  });
});
