import test from "node:test";
import assert from "node:assert/strict";

import {
  renderAdminOpsAlerts,
  renderUsersTableHtml
} from "../../../frontend/admin/render.js";

function makeEl() {
  return {
    innerHTML: "",
    querySelectorAll: () => []
  };
}

test("admin render: dismiss alerts and wipe account buttons expose tooltips", () => {
  const alertsEl = makeEl();
  renderAdminOpsAlerts(alertsEl, [
    { id: "sync_lag", severity: "warning", message: "Sync lag detected.", dismissible: true }
  ]);
  assert.match(alertsEl.innerHTML, /admin-alert-ack-btn[\s\S]*data-tooltip="Dismiss this operations alert\."/);
  assert.doesNotMatch(alertsEl.innerHTML, /\stitle=/);

  const usersHtml = renderUsersTableHtml([
    { uid: "u1", name: "Ada", savedJobsCount: 1, notesBytes: 2, attachmentsCount: 3, attachmentsBytes: 4, totalBytes: 6 }
  ], value => `${value} B`);
  assert.match(usersHtml, /admin-wipe-btn[\s\S]*data-tooltip="Wipe this local account&#039;s saved jobs, notes, and attachments\."/);
  assert.doesNotMatch(usersHtml, /\stitle=/);
});
