import test from "node:test";
import assert from "node:assert/strict";
import {
  deriveFetcherProgressModel,
  deriveSourceApprovalStatus
} from "../../../frontend/admin/domain.js";

test("admin domain includes ok-with-warning fetcher counts in progress model", () => {
  const view = deriveFetcherProgressModel({
    summary: {
      successfulSources: 8,
      okWithWarningSources: 2,
      failedSources: 0,
      excludedSources: 1,
      outputCount: 30,
      sourceCount: 10
    },
    runtime: {
      selectedSourceCount: 10
    }
  }, { running: true });

  assert.match(view.label, /ok warnings 2/i);
});

test("admin domain labels hidden pending sources with their reason", () => {
  const status = deriveSourceApprovalStatus({
    registryState: "pending",
    candidateState: "hidden",
    hiddenFromDefault: true,
    pendingReason: "repeated_zero_jobs"
  }, "pending");

  assert.equal(status.label, "Hidden: repeated_zero_jobs");
  assert.equal(status.tone, "warning");
});
