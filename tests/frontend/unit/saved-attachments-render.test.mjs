import test from "node:test";
import assert from "node:assert/strict";

import { renderAttachmentList } from "../../../frontend/saved/app/attachments.js";
import { escapeHtml } from "../../../frontend/shared/ui/index.js";

test("saved attachments render meaningful action tooltips without native titles", () => {
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

  assert.match(listEl.innerHTML, /att-open-btn[\s\S]*data-tooltip="Open this attachment\."/);
  assert.match(listEl.innerHTML, /att-download-btn[\s\S]*data-tooltip="Download this attachment\."/);
  assert.match(listEl.innerHTML, /att-delete-btn[\s\S]*data-tooltip="Delete this attachment from the saved job\."/);
  assert.doesNotMatch(listEl.innerHTML, /\stitle=/);
});
