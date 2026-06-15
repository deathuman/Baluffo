import test from "node:test";
import assert from "node:assert/strict";
import { deriveSourceApprovalStatus } from "../../../frontend/admin/domain.js";

test("admin source approval labels prefer backend pending classification", () => {
  assert.equal(
    deriveSourceApprovalStatus({
      jobsFound: 3,
      autoApprovalEligible: false,
      reviewBucket: "conflict_demoted",
      primaryBlocker: "conflict_demoted",
      approvalBlockerLabels: ["Previously demoted by registry-conflict automation"]
    }, "pending").label,
    "Blocked: conflict-demoted"
  );
  assert.equal(
    deriveSourceApprovalStatus({
      jobsFound: 3,
      autoApprovalEligible: false,
      reviewBucket: "weak_signal",
      primaryBlocker: "weak_signal",
      approvalBlockerLabels: ["Weak discovery signal"]
    }, "pending").label,
    "Blocked: weak signal"
  );
  assert.equal(
    deriveSourceApprovalStatus({
      jobsFound: 0,
      autoApprovalEligible: true,
      reviewBucket: "auto_approvable"
    }, "pending").label,
    "Auto-approvable"
  );
});
