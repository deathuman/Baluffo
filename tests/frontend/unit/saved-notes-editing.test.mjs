import test from "node:test";
import assert from "node:assert/strict";
import {
  isEditingNotesFieldFromElement,
  shouldDeferSavedJobsRerender
} from "../../../frontend/saved/app.js";

test("saved notes editing detection recognizes notes textarea only", () => {
  const notesTextarea = {
    tagName: "textarea",
    classList: {
      contains: cls => cls === "job-notes-input"
    }
  };
  const plainTextarea = {
    tagName: "textarea",
    classList: {
      contains: () => false
    }
  };
  const inputEl = {
    tagName: "input",
    classList: {
      contains: cls => cls === "job-notes-input"
    }
  };

  assert.equal(isEditingNotesFieldFromElement(notesTextarea), true);
  assert.equal(isEditingNotesFieldFromElement(plainTextarea), false);
  assert.equal(isEditingNotesFieldFromElement(inputEl), false);
  assert.equal(isEditingNotesFieldFromElement(null), false);
});

test("saved notes editing defer flag is true only while actively editing", () => {
  assert.equal(shouldDeferSavedJobsRerender({ isEditingNotes: true }), true);
  assert.equal(shouldDeferSavedJobsRerender({ isEditingNotes: false }), false);
});

test("saved notes editing defer flag covers in-flight/pending/recent settle window", () => {
  assert.equal(
    shouldDeferSavedJobsRerender({ isEditingNotes: false, inFlightCount: 1 }),
    true
  );
  assert.equal(
    shouldDeferSavedJobsRerender({ isEditingNotes: false, pendingCount: 2 }),
    true
  );
  assert.equal(
    shouldDeferSavedJobsRerender({
      isEditingNotes: false,
      lastInteractionAt: 10_000,
      nowMs: 10_900
    }),
    true
  );
  assert.equal(
    shouldDeferSavedJobsRerender({
      isEditingNotes: false,
      lastInteractionAt: 10_000,
      nowMs: 12_500
    }),
    false
  );
});
