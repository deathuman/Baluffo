import test from "node:test";
import assert from "node:assert/strict";

import { renderAttachmentList } from "../../../frontend/saved/app/attachments.js";
import { escapeHtml } from "../../../frontend/shared/ui/index.js";

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
