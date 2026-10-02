import test from "node:test";
import assert from "node:assert/strict";

import {
  hydrateAttachmentList,
  renderAttachmentError,
  renderAttachmentList
} from "../../../frontend/saved/app/attachments.js";
import { escapeHtml } from "../../../frontend/shared/ui/index.js";
import { renderSavedJobBlockHtml } from "../../../frontend/saved/render.js";

test("saved attachments omit redundant visible action tooltips and native titles", () => {
  const listEl = { innerHTML: "" };
  const savedJobsListEl = {
    querySelector(selector) {
      assert.equal(selector, '.attachments-list[data-job-key="job-1"]');
      return listEl;
    }
  };

  renderAttachmentList("job-1", [
    { id: "att-1", name: "portfolio.pdf", size: 1024 }
  ], {
    savedJobsListEl,
    cssEscape: value => value,
    clearAttachmentPreviewUrls() {},
    getAttachmentPreviewUrl() {
      return "";
    },
    escapeHtml,
    bindAttachmentActionButtons() {}
  });

  assert.doesNotMatch(listEl.innerHTML, /att-open-btn[\s\S]*data-tooltip=/);
  assert.doesNotMatch(listEl.innerHTML, /att-download-btn[\s\S]*data-tooltip=/);
  assert.doesNotMatch(listEl.innerHTML, /att-delete-btn[\s\S]*data-tooltip=/);
  assert.doesNotMatch(listEl.innerHTML, /\stitle=/);
});


test("attachment load failure renders a retry state instead of an empty list", async () => {
  const el = { innerHTML: "" };
  const calls = [];
  const savedJobsListEl = { querySelector: () => el };

  await hydrateAttachmentList("job-1", {
    currentUser: { uid: "u1" },
    listAttachmentsForJob: async () => {
      throw new Error("bridge offline");
    },
    renderAttachmentList: (jobKey, rows) => {
      calls.push(["list", rows]);
      renderAttachmentList(jobKey, rows, {
        savedJobsListEl,
        cssEscape: v => v,
        clearAttachmentPreviewUrls() {},
        getAttachmentPreviewUrl: () => "",
        escapeHtml,
        bindAttachmentActionButtons() {}
      });
    },
    renderAttachmentError: jobKey => {
      calls.push(["error", null]);
      renderAttachmentError(jobKey, { savedJobsListEl, cssEscape: v => v });
    }
  });

  assert.deepEqual(calls, [["error", null]], "a failed load must take the error path only");
  assert.match(el.innerHTML, /Could not load attachments/);
  assert.doesNotMatch(el.innerHTML, /No attachments yet/);
});

test("an ok:false attachment response is a failure, not an empty list", async () => {
  const el = { innerHTML: "" };
  const calls = [];
  const savedJobsListEl = { querySelector: () => el };

  await hydrateAttachmentList("job-1", {
    currentUser: { uid: "u1" },
    listAttachmentsForJob: async () => ({ ok: false, error: "bridge offline" }),
    renderAttachmentList: () => calls.push(["list"]),
    renderAttachmentError: jobKey => {
      calls.push(["error"]);
      renderAttachmentError(jobKey, { savedJobsListEl, cssEscape: v => v });
    }
  });

  assert.deepEqual(calls, [["error"]]);
  assert.match(el.innerHTML, /Could not load attachments/);
});

test("a genuinely empty attachment list still renders as empty", async () => {
  const el = { innerHTML: "" };
  const calls = [];
  const savedJobsListEl = { querySelector: () => el };

  await hydrateAttachmentList("job-1", {
    currentUser: { uid: "u1" },
    listAttachmentsForJob: async () => ({ ok: true, data: [] }),
    renderAttachmentList: (jobKey, rows) => {
      calls.push(["list", rows]);
      renderAttachmentList(jobKey, rows, {
        savedJobsListEl,
        cssEscape: v => v,
        clearAttachmentPreviewUrls() {},
        getAttachmentPreviewUrl: () => "",
        escapeHtml,
        bindAttachmentActionButtons() {}
      });
    },
    renderAttachmentError: () => calls.push(["error"])
  });

  assert.deepEqual(calls, [["list", []]], "a real empty list must not become an error state");
  assert.match(el.innerHTML, /No attachments yet/);
});

test("saved attachment panel exposes a manual refresh and does not claim to be empty before loading", () => {
  const html = renderSavedJobBlockHtml(
    {
      jobKey: "job_1",
      title: "Gameplay Engineer",
      company: "Studio",
      city: "Rome",
      country: "Italy",
      jobLink: "https://example.com/jobs/1",
      applicationStatus: "bookmark",
      phaseTimestamps: {},
      savedAt: "2026-03-08T09:00:00.000Z",
      notes: ""
    },
    {
      isCustomJob: () => false,
      customSourceLabel: "Custom",
      normalizeSavedSector: () => "Game",
      fullCountryName: value => value,
      sanitizeUrl: value => value,
      toContractClass: () => "full-time",
      normalizePhase: value => value || "bookmark",
      expandedJobKey: "",
      selectedJobKey: "",
      getJobDetailsTab: () => "notes",
      renderDetailsSummary: () => "",
      getReminderMeta: () => ({ isSoon: false, label: "" }),
      renderMissingInfoChips: () => "",
      renderUpdatedHint: () => "",
      getJobHistoryEntries: () => "",
      renderWebIcon: () => "",
      renderPhaseBar: () => "",
      lifecycleOverlay: { status: "active", lastSeenAt: "" },
      currentUser: { uid: "u1" },
      maxAttachmentsPerJob: 10,
      maxAttachmentBytes: 1024
    }
  );

  // The manual refresh control, mirroring the existing history-tab refresh button.
  assert.match(html, /data-ui="att-refresh-btn"/);
  assert.match(html, /class="btn back-btn att-refresh-btn"[^>]*data-job-key="job_1"/);

  // Before hydration the list is unknown, so the panel must not claim to be
  // empty. This is the first-paint state, and "No attachments yet." there is a
  // claim the app has not made yet.
  assert.match(html, /Attachments have not been loaded yet/);
  assert.doesNotMatch(
    html,
    /attachments-list[^>]*>\s*<div class="muted">No attachments yet\.<\/div>/
  );
});
