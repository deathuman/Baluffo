import test from "node:test";
import assert from "node:assert/strict";

import {
  deriveDesktopUpdateView,
  shouldExposeJobsDesktopUpdateStatus,
  shouldPollDesktopUpdateStatus
} from "../../../frontend/jobs/app/desktop-update.js";

test("deriveDesktopUpdateView shows a terminal rollback failure instead of up to date", () => {
  const view = deriveDesktopUpdateView({
    currentVersion: "0.3.065",
    availability: "up_to_date",
    installState: "failed",
    installStage: "rollback_failed",
    installStageLabel: "Update repair required",
    lastError: "install_restore_failed: install is locked",
    repairAvailable: true
  }, { panelOpen: true });

  assert.equal(view.buttonLabel, "Update needs repair");
  assert.equal(view.title, "Desktop update needs repair");
  assert.equal(view.body, "install_restore_failed: install is locked");
  assert.equal(view.primaryAction, "repair");
  assert.equal(view.primaryLabel, "Repair install");
  assert.equal(view.primaryVisible, true);
  assert.equal(shouldExposeJobsDesktopUpdateStatus({ installStage: "rollback_failed" }), true);
});

test("deriveDesktopUpdateView hides repair when no portable snapshot remains", () => {
  const view = deriveDesktopUpdateView({
    installState: "failed",
    installStage: "rollback_failed",
    lastError: "install_restore_failed: install is locked",
    repairAvailable: false
  }, { panelOpen: true });

  assert.equal(view.primaryVisible, false);
  assert.match(view.progress, /troubleshooting guide/);
});

test("deriveDesktopUpdateView surfaces repairing, repaired, and failed repair states", () => {
  const repairing = deriveDesktopUpdateView({ repairState: "repairing" }, { panelOpen: true });
  assert.equal(repairing.buttonLabel, "Repairing update");
  assert.equal(repairing.primaryVisible, false);
  assert.equal(shouldPollDesktopUpdateStatus({ repairState: "repairing" }), true);

  const repaired = deriveDesktopUpdateView({
    repairState: "repaired",
    repairVersion: "0.3.064"
  }, { panelOpen: true });
  assert.equal(repaired.buttonLabel, "Repair complete");
  assert.equal(repaired.title, "Desktop install repaired");
  assert.equal(repaired.body, "Restored Baluffo 0.3.064 from the retained rollback snapshot.");
  assert.equal(repaired.primaryAction, "check");

  const failed = deriveDesktopUpdateView({
    repairState: "repair_failed",
    repairAvailable: true,
    repairError: "The previous AppImage could not be restored."
  }, { panelOpen: true });
  assert.equal(failed.buttonLabel, "Repair failed");
  assert.equal(failed.primaryAction, "repair");
  assert.equal(failed.primaryLabel, "Try repair again");
  assert.equal(failed.body, "The previous AppImage could not be restored.");
  assert.equal(shouldExposeJobsDesktopUpdateStatus({ repairState: "repair_failed" }), true);
});
