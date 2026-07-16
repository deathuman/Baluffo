import test from "node:test";
import assert from "node:assert/strict";

import { setupJobsListDelegation as setupJobsListDelegationEvents } from "../../../frontend/jobs/app/runtime/jobs-list-events.js";

global.window = {
  location: {
    href: "http://localhost/jobs.html"
  }
};

const { openJobLinkInDefaultBrowser } = await import("../../../frontend/jobs/app/runtime.js");

class FakeElement {}

function createTarget({ row, saveButton, originalLinkButton = null }) {
  const target = Object.create(FakeElement.prototype);
  target.closest = selector => {
    if (selector === ".save-btn") return saveButton ? saveButton : null;
    if (selector === "[data-ui='job-original-link-btn']") return originalLinkButton;
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

    setupJobsListDelegationEvents({
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

test("unavailable rows only open through the explicit original-link button", () => {
  const previousElement = global.Element;
  global.Element = FakeElement;
  const external = [];
  const marks = [];

  try {
    const jobsList = createJobsList();
    const row = { dataset: { jobLink: "", jobKey: "job-closed" } };
    const originalLinkButton = {
      dataset: { jobLink: "https://example.com/jobs/closed" },
      closest: selector => selector === ".job-row[data-job-link]" ? row : null
    };

    setupJobsListDelegationEvents({
      jobsList,
      jobRowSelector: ".job-row[data-job-link]",
      saveJobBtnSelector: ".save-btn",
      sanitizeUrl: value => value,
      getJobById: () => null,
      onToggleSaveJob: async () => {},
      onOpenJobLink: async url => external.push(url),
      onMarkJobSeen: jobKey => marks.push(jobKey)
    });

    jobsList.handlers.get("click")({
      target: createTarget({ row, saveButton: null }),
      preventDefault() {},
      stopPropagation() {}
    });
    jobsList.handlers.get("click")({
      target: createTarget({ row, saveButton: null, originalLinkButton }),
      preventDefault() {},
      stopPropagation() {}
    });

    assert.deepEqual(external, ["https://example.com/jobs/closed"]);
    assert.deepEqual(marks, ["job-closed"]);
  } finally {
    global.Element = previousElement;
  }
});

test("jobs list delegation can hand off job links to a browser callback", () => {
  const previousElement = global.Element;
  const previousWindow = global.window;
  global.Element = FakeElement;
  const opens = [];
  const external = [];
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

    setupJobsListDelegationEvents({
      jobsList,
      jobRowSelector: ".job-row[data-job-link]",
      saveJobBtnSelector: ".save-btn",
      sanitizeUrl: value => value,
      getJobById: id => (id === "job-1" ? { id: "job-1" } : null),
      onToggleSaveJob: async job => {
        saves.push(job.id);
      },
      onOpenJobLink: async url => {
        external.push(url);
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

    assert.deepEqual(external, ["https://example.com/job"]);
    assert.deepEqual(opens, []);
    assert.deepEqual(marks, ["job-1"]);
    assert.deepEqual(saves, []);
  } finally {
    global.Element = previousElement;
    global.window = previousWindow;
  }
});

test("desktop job link open does not fall back to the shell on bridge failure", async () => {
  const opens = [];
  const logs = [];

  await openJobLinkInDefaultBrowser("https://example.com/job", {
    isDesktopRuntimeMode: () => true,
    callJobsBridge: async () => {
      throw new Error("bridge unavailable");
    },
    openWindow: url => {
      opens.push(url);
    },
    logJobsError: (message, err) => {
      logs.push([message, err?.message]);
    },
    bridgeBaseUrl: "http://127.0.0.1:8877"
  });

  assert.deepEqual(opens, []);
  assert.deepEqual(logs, [["Failed to open job link in the default browser", "bridge unavailable"]]);
});

test("desktop job link open posts the URL through the jobs bridge", async () => {
  const opens = [];
  const bridgeCalls = [];

  await openJobLinkInDefaultBrowser("https://example.com/job", {
    isDesktopRuntimeMode: () => true,
    callJobsBridge: async (...args) => {
      bridgeCalls.push(args);
    },
    openWindow: url => {
      opens.push(url);
    }
  });

  assert.deepEqual(opens, []);
  assert.deepEqual(bridgeCalls, [[
    "/desktop-local-data/open-url",
    {
      method: "POST",
      body: { url: "https://example.com/job" }
    }
  ]]);
});
