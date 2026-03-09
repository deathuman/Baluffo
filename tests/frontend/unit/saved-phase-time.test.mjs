import test from "node:test";
import assert from "node:assert/strict";
import {
  needsInterviewTimestamp,
  parseScheduledTimestampInput,
  toPromptLocalDateTime
} from "../../../frontend/saved/app.js";

test("saved phase time: interview phases require timestamp", () => {
  assert.equal(needsInterviewTimestamp("interview_1"), true);
  assert.equal(needsInterviewTimestamp("interview_2"), true);
  assert.equal(needsInterviewTimestamp("applied"), false);
});

test("saved phase time: parser accepts supported datetime formats", () => {
  const parsedSpaced = parseScheduledTimestampInput("2026-03-09 14:30");
  const parsedIsoLocal = parseScheduledTimestampInput("2026-03-09T14:30");
  assert.match(parsedSpaced, /^2026-03-09T/);
  assert.match(parsedIsoLocal, /^2026-03-09T/);
});

test("saved phase time: parser rejects invalid input and prompt formatter is stable", () => {
  assert.equal(parseScheduledTimestampInput("not a date"), "");
  assert.equal(parseScheduledTimestampInput(""), "");
  assert.match(toPromptLocalDateTime("2026-03-09T14:30:00.000Z"), /^2026-03-09 \d{2}:\d{2}$/);
});
