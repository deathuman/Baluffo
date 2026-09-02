import test from "node:test";
import assert from "node:assert/strict";

import { getJobsLastUpdatedText } from "../../../frontend/jobs/app/startup.js";

function localTime(ts) {
  return new Date(ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function localDate(ts, options) {
  return new Date(ts).toLocaleDateString([], options);
}

test("last updated text returns empty for missing or invalid timestamps", () => {
  const now = new Date(2026, 6, 15, 12, 0).getTime();
  assert.equal(getJobsLastUpdatedText(null, now), "");
  assert.equal(getJobsLastUpdatedText(undefined, now), "");
  assert.equal(getJobsLastUpdatedText("", now), "");
  assert.equal(getJobsLastUpdatedText("not-a-timestamp", now), "");
  assert.equal(getJobsLastUpdatedText(Number.NaN, now), "");
});

test("last updated text renders absolute time for today", () => {
  const now = new Date(2026, 6, 15, 12, 0).getTime();
  const todayTs = new Date(2026, 6, 15, 9, 5).getTime();
  assert.equal(
    getJobsLastUpdatedText(todayTs, now),
    `Last updated: ${localTime(todayTs)}`
  );
});

test("last updated text renders absolute time with Yesterday prefix", () => {
  const now = new Date(2026, 6, 15, 12, 0).getTime();
  const yesterdayTs = new Date(2026, 6, 14, 9, 5).getTime();
  assert.equal(
    getJobsLastUpdatedText(yesterdayTs, now),
    `Last updated: Yesterday ${localTime(yesterdayTs)}`
  );
});

test("last updated text renders month/day for earlier this year", () => {
  const now = new Date(2026, 6, 15, 12, 0).getTime();
  const earlierTs = new Date(2026, 2, 3, 9, 5).getTime();
  assert.equal(
    getJobsLastUpdatedText(earlierTs, now),
    `Last updated: ${localDate(earlierTs, { month: "short", day: "numeric" })} ${localTime(earlierTs)}`
  );
});

test("last updated text renders month/day/year for earlier years", () => {
  const now = new Date(2026, 6, 15, 12, 0).getTime();
  const oldTs = new Date(2025, 10, 20, 9, 5).getTime();
  assert.equal(
    getJobsLastUpdatedText(oldTs, now),
    `Last updated: ${localDate(oldTs, { month: "short", day: "numeric", year: "numeric" })} ${localTime(oldTs)}`
  );
});
