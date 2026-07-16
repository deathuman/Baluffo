import test from "node:test";
import assert from "node:assert/strict";
import {
  buildSavedLifecycleOverlayByJobKey,
  lifecycleOverlayForSavedJob,
  parseLifecycleStatePayload
} from "../../../frontend/saved/app/runtime/lifecycle-overlay.js";

test("saved lifecycle overlay parses lifecycle-state rows from keyed payload", () => {
  const rows = parseLifecycleStatePayload({
    jobs: {
      a: { title: "A" },
      b: { title: "B" }
    }
  });
  assert.equal(rows.length, 2);
});

test("saved lifecycle overlay resolves exact availability identities", () => {
  const overlay = buildSavedLifecycleOverlayByJobKey({
    canonicalRows: [
      {
        availabilityId: "availability_1",
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
        availabilityId: "availability_1",
        title: "Gameplay Engineer",
        company: "Studio",
        city: "Rome",
        country: "Italy",
        jobLink: "https://example.com/jobs/1",
        status: "likely_removed",
        removedAt: "2026-03-07T00:00:00.000Z"
      },
      {
        availabilityId: "availability_2",
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
  const first = lifecycleOverlayForSavedJob(overlay, { availabilityId: "availability_1" });
  const second = lifecycleOverlayForSavedJob(overlay, { availabilityId: "availability_2" });
  assert.deepEqual(first, {
    status: "active",
    removedAt: "",
    lastSeenAt: "",
    lifecycleEvent: "reappeared",
    lifecycleReason: "",
    availabilityId: "availability_1",
    availabilityStatus: "",
    availabilityCheckedAt: "",
    availabilityVerifiedAt: "",
    availabilityUnavailableAt: "",
    availabilityEvidence: {}
  });
  assert.deepEqual(second, {
    status: "active",
    removedAt: "",
    lastSeenAt: "",
    lifecycleEvent: "preserved",
    lifecycleReason: "source_failed",
    availabilityId: "availability_2",
    availabilityStatus: "",
    availabilityCheckedAt: "",
    availabilityVerifiedAt: "",
    availabilityUnavailableAt: "",
    availabilityEvidence: {}
  });
});

test("saved lifecycle overlay does not regenerate fuzzy job keys", () => {
  const overlay = buildSavedLifecycleOverlayByJobKey({
    canonicalRows: [{ title: "Gameplay Engineer", company: "Studio" }],
    lifecycleRows: [{ title: "Gameplay Engineer", company: "Studio" }]
  });
  assert.equal(overlay.size, 0);
});
