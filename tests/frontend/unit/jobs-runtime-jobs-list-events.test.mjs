import test from "node:test";
import assert from "node:assert/strict";

import { setupJobsListDelegation } from "../../../frontend/jobs/app/runtime/jobs-list-events.js";

class FakeElement {}

function createTarget({ row, saveButton }) {
  const target = Object.create(FakeElement.prototype);
  target.closest = selector => {
    if (selector === ".save-btn") return saveButton ? saveButton : null;
    if (selector === ".job-row[data-job-link]") return row ? row : null;
    return null;
  };
  return target;
}

function createJobsList() {
  const handlers = new Map();
  return {
    handlers,
    addEventListener(type, handler) {
      handlers.set(type, handler);
    }
  };
}

test("jobs list delegation opens rows once and protects save clicks", () => {
  const previousElement = global.Element;
  const previousWindow = global.window;
  global.Element = FakeElement;
  const opens = [];
  const marks = [];
  const saves = [];
  global.window = {
    open(url) {
      opens.push(url);
    }
  };

  try {
    const jobsList = createJobsList();
    const row = { dataset: { jobLink: "https://example.com/job", jobKey: "job-1" } };
    const saveButton = { dataset: { jobId: "job-1" } };

    setupJobsListDelegation({
      jobsList,
      jobRowSelector: ".job-row[data-job-link]",
      saveJobBtnSelector: ".save-btn",
      sanitizeUrl: value => value,
      getJobById: id => (id === "job-1" ? { id: "job-1" } : null),
      onToggleSaveJob: async job => {
        saves.push(job.id);
      },
      onMarkJobSeen: async jobKey => {
        marks.push(jobKey);
      }
    });

    jobsList.handlers.get("click")({
      target: createTarget({ row, saveButton: null }),
      preventDefault() {},
      stopPropagation() {}
    });
    jobsList.handlers.get("click")({
      target: createTarget({ row, saveButton }),
      preventDefault() {},
      stopPropagation() {}
    });
    jobsList.handlers.get("keydown")({
      key: "Enter",
      target: createTarget({ row, saveButton: null })
    });

    assert.deepEqual(opens, ["https://example.com/job", "https://example.com/job"]);
    assert.deepEqual(marks, ["job-1", "job-1"]);
    assert.deepEqual(saves, ["job-1"]);
  } finally {
    global.Element = previousElement;
    global.window = previousWindow;
  }
});
