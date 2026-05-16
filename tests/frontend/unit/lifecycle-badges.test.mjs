import test from "node:test";
import assert from "node:assert/strict";

import {
  getLifecycleBadgeMeta,
  renderLifecycleBadgeHtml
} from "../../../frontend/shared/lifecycle-badges.js";

const now = Date.parse("2026-03-08T00:00:00.000Z");

test("lifecycle badges keep Jobs default copy stable", () => {
  const meta = getLifecycleBadgeMeta({
    status: "likely_removed",
    removedAt: "2026-03-07T00:00:00.000Z",
    lastSeenAt: "2026-03-05T00:00:00.000Z"
  }, { now });

  assert.deepEqual(meta, {
    label: "Recently removed",
    cssClass: "likely-removed",
    title: "Recently removed since Mar 7, 2026"
  });

  const html = renderLifecycleBadgeHtml({
    status: "reappeared",
    lifecycleEvent: "reappeared",
    lastSeenAt: "2026-03-07T23:00:00.000Z"
  }, { now });
  assert.match(html, /data-tooltip="Reappeared in the latest fetch"/);
  assert.doesNotMatch(html, /last seen/);
});

test("lifecycle badges add lastSeenAt copy only when requested", () => {
  assert.equal(
    getLifecycleBadgeMeta({
      status: "likely_removed",
      removedAt: "2026-03-07T00:00:00.000Z",
      lastSeenAt: "2026-03-05T00:00:00.000Z"
    }, { includeLastSeenAt: true, now }).title,
    "Recently removed since Mar 7, 2026; last seen 3d ago"
  );

  assert.equal(
    getLifecycleBadgeMeta({
      status: "archived",
      removedAt: "2026-03-01T00:00:00.000Z",
      lastSeenAt: "2026-03-07T12:00:00.000Z"
    }, { includeLastSeenAt: true, now }).title,
    "Archived after removal on Mar 1, 2026; last seen 12h ago"
  );

  assert.equal(
    getLifecycleBadgeMeta({
      status: "active",
      lifecycleEvent: "preserved",
      lifecycleReason: "source_failed",
      lastSeenAt: "2026-03-07T23:45:00.000Z"
    }, { includeLastSeenAt: true, now }).title,
    "Kept visible because the source failed in the latest fetch; last seen 15m ago"
  );
});

test("lifecycle badges ignore missing or invalid lastSeenAt copy", () => {
  assert.equal(
    getLifecycleBadgeMeta({
      status: "archived",
      removedAt: "2026-03-01T00:00:00.000Z",
      lastSeenAt: "invalid"
    }, { includeLastSeenAt: true, now }).title,
    "Archived after removal on Mar 1, 2026"
  );

  assert.equal(
    getLifecycleBadgeMeta({
      status: "likely_removed",
      removedAt: "",
      lastSeenAt: ""
    }, { includeLastSeenAt: true, now }).title,
    "Recently removed"
  );
});
