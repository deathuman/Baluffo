import test from "node:test";
import assert from "node:assert/strict";

import { createOpsActions } from "../../../frontend/admin/app/ops/health-actions.js";
import {
  createDeferred,
  createDeferredWithReject
} from "./helpers/async-test-helpers.mjs";

function createActionsFixture() {
  const posts = [];
  const actions = createOpsActions({
    state: {},
    refs: {},
    postBridge(path, payload) {
      const request = createDeferredWithReject();
      posts.push({ path, payload, request });
      return request.promise;
    },
    showToast() {},
    getErrorMessage: error => String(error?.message || error || "unknown"),
    escapeHtml: value => String(value || ""),
    getObjectValue: value => value && typeof value === "object" ? value : {},
    loadOpsHealthData: async () => {},
    loadSourcePolicyDetail: async () => {},
    loadRegistryConflictsMore: async () => {},
    loadOpsOverviewDetailData: async () => {},
    loadActiveOpsSummaryData: async () => {},
    applyOptimisticAbortRow() {},
    setPendingOpsAbort() {},
    clearPendingOpsAbort() {},
    hasPendingOpsAbort: () => false,
    isAbortAcceptedResult: () => true,
    hasActivePipelineOrFetchRows: () => false,
    renderAdminRegistryConflictsImpl() {},
    renderDiscoveryCandidateReviewHtml: () => "",
    toDiscoveryBadgeState: () => ({ count: 0 }),
    renderAdminSourcePolicyReviewImpl() {},
    currentRenderToken: () => 0
  });
  return { actions, posts };
}

async function assertPerTargetAdmission({
  posts,
  invoke,
  target,
  otherTarget,
  path,
  firstAction = "first-action",
  competingAction = "competing-action",
  retryActionName = "retry-action",
  assertPayload
}) {
  const first = invoke(target, firstAction);
  const duplicate = invoke({ ...target }, competingAction);
  const unrelated = invoke(otherTarget, firstAction);

  assert.equal(posts.length, 2, "same-target mutation is rejected while unrelated target proceeds");
  assert.equal(posts.filter(post => post.path === path).length, 2);
  if (typeof assertPayload === "function") assertPayload(posts);

  posts[0].request.resolve({});
  posts[1].request.resolve({});
  await Promise.all([first, duplicate, unrelated]);

  const retry = invoke(target, retryActionName);
  assert.equal(posts.length, 3, "target can be acted on again after the first mutation settles");
  posts[2].request.resolve({});
  await retry;
}

test("source-policy review actions admit only one in-flight mutation per pair", async () => {
  const fixture = createActionsFixture();
  await assertPerTargetAdmission({
    ...fixture,
    invoke: fixture.actions.handleSourcePolicyAction,
    target: { staticSourceId: "static:one", providerSourceId: "provider:one" },
    otherTarget: { staticSourceId: "static:two", providerSourceId: "provider:two" },
    path: "/source-policy/review-action",
    firstAction: "clear_override",
    competingAction: "snooze",
    retryActionName: "acknowledge",
    assertPayload(posts) {
      assert.equal(posts[0].payload.action, "clear_override");
      assert.equal(posts[0].payload.staticSourceId, "static:one");
      assert.equal(posts[1].payload.staticSourceId, "static:two");
    }
  });
});

test("migration-link actions admit only one in-flight mutation per identity pair", async () => {
  const fixture = createActionsFixture();
  await assertPerTargetAdmission({
    ...fixture,
    invoke: fixture.actions.handleMigrationLinkAction,
    target: { recommendedApiPayload: { action: "apply_migration_identity_link", providerSourceId: "greenhouse:one", staticSourceId: "static:one" } },
    otherTarget: { recommendedApiPayload: { action: "apply_migration_identity_link", providerSourceId: "greenhouse:two", staticSourceId: "static:two" } },
    path: "/source-policy/migration-link-action",
    firstAction: "apply_migration_identity_link",
    competingAction: "clear_migration_identity_link",
    retryActionName: "clear_migration_identity_link",
    assertPayload(posts) {
      assert.equal(posts[0].payload.action, "apply_migration_identity_link");
      assert.equal(posts[0].payload.providerSourceId, "greenhouse:one");
      assert.equal(posts[1].payload.providerSourceId, "greenhouse:two");
    }
  });
});

test("dedup review actions admit only one in-flight mutation per dedup key", async () => {
  const fixture = createActionsFixture();
  await assertPerTargetAdmission({
    ...fixture,
    invoke: fixture.actions.handleDedupReviewAction,
    target: { dedupKey: "job:one", title: "First job" },
    otherTarget: { dedupKey: "job:two", title: "Second job" },
    path: "/dedup/review-action",
    firstAction: "reviewed_safe",
    competingAction: "confirmed_blocking",
    retryActionName: "clear_review",
    assertPayload(posts) {
      assert.equal(posts[0].payload.action, "reviewed_safe");
      assert.equal(posts[0].payload.dedupKey, "job:one");
      assert.equal(posts[1].payload.dedupKey, "job:two");
    }
  });
});

test("failed review mutations release their per-target admission guard", async () => {
  const failedPost = createDeferredWithReject();
  const posts = [];
  const actions = createOpsActions({
    state: {},
    refs: {},
    postBridge(path, payload) {
      const request = posts.length === 0 ? failedPost : createDeferred();
      posts.push({ path, payload, request });
      return request.promise;
    },
    showToast() {},
    getErrorMessage: error => String(error?.message || error || "unknown"),
    escapeHtml: value => String(value || ""),
    getObjectValue: value => value && typeof value === "object" ? value : {},
    loadOpsHealthData: async () => {},
    loadSourcePolicyDetail: async () => {},
    loadRegistryConflictsMore: async () => {},
    loadOpsOverviewDetailData: async () => {},
    loadActiveOpsSummaryData: async () => {},
    applyOptimisticAbortRow() {},
    setPendingOpsAbort() {},
    clearPendingOpsAbort() {},
    hasPendingOpsAbort: () => false,
    isAbortAcceptedResult: () => true,
    hasActivePipelineOrFetchRows: () => false,
    renderAdminRegistryConflictsImpl() {},
    renderDiscoveryCandidateReviewHtml: () => "",
    toDiscoveryBadgeState: () => ({ count: 0 }),
    renderAdminSourcePolicyReviewImpl() {},
    currentRenderToken: () => 0
  });
  const row = { dedupKey: "job:retry", title: "Retry job" };

  const failedAction = actions.handleDedupReviewAction(row, "reviewed_safe");
  assert.equal(posts.length, 1);
  failedPost.reject(new Error("temporary failure"));
  await failedAction;

  const retry = actions.handleDedupReviewAction(row, "reviewed_safe");
  assert.equal(posts.length, 2, "failed mutation no longer blocks retry");
  posts[1].request.resolve({});
  await retry;
});
