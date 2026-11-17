import test from "node:test";
import assert from "node:assert/strict";

import {
  deriveDesktopUpdateView,
  shouldExposeJobsDesktopUpdateStatus
} from "../../../frontend/jobs/app/desktop-update.js";

test("deriveDesktopUpdateView shows a terminal rollback failure instead of up to date", () => {
  const view = deriveDesktopUpdateView({
    currentVersion: "0.3.065",
    availability: "up_to_date",
    installState: "failed",
    installStage: "rollback_failed",
    installStageLabel: "Update repair required",
    lastError: "install_restore_failed: install is locked"
  }, { panelOpen: true });

  assert.equal(view.buttonLabel, "Update needs repair");
  assert.equal(view.title, "Desktop update needs repair");
  assert.equal(view.body, "install_restore_failed: install is locked");
  assert.equal(view.primaryVisible, false);
  assert.equal(shouldExposeJobsDesktopUpdateStatus({ installStage: "rollback_failed" }), true);
});
