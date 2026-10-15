import test from "node:test";
import assert from "node:assert/strict";

import { createSavedAttachmentsController } from "../../../frontend/saved/app/runtime/attachments-controller.js";
import { createElement } from "./helpers/saved-runtime-helpers.mjs";

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function withAttachmentGlobals(fn) {
  const originalDocument = globalThis.document;
  const originalWindow = globalThis.window;
  const originalURL = globalThis.URL;
  const originalRequestAnimationFrame = globalThis.requestAnimationFrame;
  const originalSetTimeout = globalThis.setTimeout;
  const originalConsoleError = console.error;
  const appended = [];
  const opened = [];
  const objectUrls = [];

  globalThis.document = {
    createElement(tagName) {
      return {
        tagName,
        className: "",
        textContent: "",
        children: [],
        classList: {
          add() {},
          remove() {}
        },
        appendChild(child) {
          this.children.push(child);
        },
        addEventListener() {},
        click() {
          this.didClick = true;
        },
        remove() {
          this.didRemove = true;
        }
      };
    },
    body: {
      appendChild(element) {
        appended.push(element);
      }
    }
  };
  globalThis.window = {
    open(...args) {
      opened.push(args);
    }
  };
  globalThis.URL = {
    createObjectURL(blob) {
      const url = `blob:test-${objectUrls.length + 1}`;
      objectUrls.push({ blob, url });
      return url;
    },
    revokeObjectURL() {}
  };
  globalThis.requestAnimationFrame = callback => {
    callback();
    return 1;
  };
  globalThis.setTimeout = () => 1;
  console.error = () => {};

  try {
    return await fn({ appended, opened, objectUrls });
  } finally {
    globalThis.document = originalDocument;
    globalThis.window = originalWindow;
    globalThis.URL = originalURL;
    globalThis.requestAnimationFrame = originalRequestAnimationFrame;
    globalThis.setTimeout = originalSetTimeout;
    console.error = originalConsoleError;
  }
}

function createHarness(getAttachmentBlob) {
  const openButton = createElement({
    dataset: { jobKey: "job-1", attachmentId: "att-1" }
  });
  const downloadButton = createElement({
    dataset: { jobKey: "job-1", attachmentId: "att-1", fileName: "notes.txt" }
  });
  const savedJobsListEl = {
    querySelectorAll(selector) {
      if (selector === ".att-open-btn") return [openButton];
      if (selector === ".att-download-btn") return [downloadButton];
      return [];
    }
  };
  const viewState = { currentUser: { uid: "u1" } };
  const controller = createSavedAttachmentsController({
    dom: { savedJobsListEl },
    viewState,
    savedPageService: {
      getAttachmentOpenUrl: () => null,
      getAttachmentDownloadUrl: () => null,
      getAttachmentBlob
    },
    savedDispatch: { dispatch() {} },
    savedActions: { ATTACHMENT_MUTATED: "saved/attachmentMutated" },
    queueActivityPulse() {},
    timelineScopeAttachments: "attachments",
    maxAttachmentsPerJob: 3,
    maxAttachmentBytes: 1024,
    attachmentPreviewUrls: new Map(),
    cssEscape: value => String(value || ""),
    setSelectedJobKey() {}
  });
  controller.bindAttachmentActionButtons();
  return { controller, viewState, openButton, downloadButton };
}

const successfulBlobRead = {
  ok: true,
  data: { blob: { type: "text/plain" }, filename: "notes.txt" }
};

test("attachment open and download discard blob reads after an account switch", async () => {
  for (const action of ["open", "download"]) {
    for (const outcome of ["success", "failure"]) {
      await withAttachmentGlobals(async ({ appended, opened, objectUrls }) => {
        const started = createDeferred();
        const finishRead = createDeferred();
        const harness = createHarness((uid, jobKey, attachmentId) => {
          assert.deepEqual([uid, jobKey, attachmentId], ["u1", "job-1", "att-1"]);
          started.resolve();
          return finishRead.promise;
        });
        const clickPromise = harness[`${action}Button`].onclick();
        await started.promise;
        harness.viewState.currentUser = { uid: "u2" };
        if (outcome === "success") finishRead.resolve(successfulBlobRead);
        else finishRead.reject(new Error("read failed"));
        await clickPromise;

        assert.deepEqual(opened, [], `${action} must not open content from the previous account`);
        assert.deepEqual(objectUrls, [], `${action} must not create a URL for a stale result`);
        assert.deepEqual(appended, [], `${action} must not start a download or show a toast`);
      });
    }
  }
});

test("attachment open and download still consume blob reads for the current account", async () => {
  for (const action of ["open", "download"]) {
    await withAttachmentGlobals(async ({ appended, opened, objectUrls }) => {
      const harness = createHarness(async () => successfulBlobRead);
      await harness[`${action}Button`].onclick();

      assert.equal(objectUrls.length, 1);
      if (action === "open") {
        assert.deepEqual(opened, [["blob:test-1", "_blank", "noopener,noreferrer"]]);
        assert.deepEqual(appended, []);
      } else {
        assert.deepEqual(opened, []);
        const anchor = appended.find(element => element.tagName === "a");
        assert.ok(anchor, "download should append an anchor");
        assert.equal(anchor.href, "blob:test-1");
        assert.equal(anchor.download, "notes.txt");
        assert.equal(anchor.didClick, true);
        assert.equal(anchor.didRemove, true);
      }
    });
  }
});
