import test from "node:test";
import assert from "node:assert/strict";

import {
  getMigrationLinkLinkedActions,
  renderAdminSourcePolicyReview
} from "../../../frontend/admin/render/source-policy-review.js";
import { UI_TOKENS, ui } from "../../../frontend/shared/ui/selectors.js";

function makeMigrationLinkCandidate(overrides = {}) {
  return {
    providerSourceId: "greenhouse:slug:studio",
    providerSourceName: "Studio Greenhouse",
    selectedStaticSourceId: "static:listing_url:https://studio.example/jobs",
    selectedStaticSourceName: "Studio Static",
    apiEligible: true,
    recommendedApiPayload: {
      action: "apply_migration_identity_link",
      providerSourceId: "greenhouse:slug:studio",
      staticSourceId: "static:listing_url:https://studio.example/jobs",
      staticSourceName: "Studio Static",
      confidence: 0.8,
      reasons: ["source_state_disambiguation"],
      recommendationSource: "provider_coverage_link_backfill",
      recommendedAction: "needs_review"
    },
    ...overrides
  };
}

function makeLinkedMigrationCandidate(overrides = {}) {
  return {
    providerBucket: "active",
    providerSourceId: "greenhouse:slug:studio",
    providerSourceName: "Studio Greenhouse",
    providerAdapter: "greenhouse",
    staticSourceId: "static:listing_url:https://studio.example/jobs",
    staticSourceName: "Studio Static",
    migrationSourceIdentity: "static:listing_url:https://studio.example/jobs",
    migrationSourceName: "Studio Static",
    migrationLinkedBy: "admin_provider_link_backfill",
    adminBackfillOwned: true,
    providerCoverageStatus: "validated_provider",
    providerCoverageConsecutiveSuccesses: 1,
    providerCoverageLatestKeptCount: 4,
    providerReplacementReadiness: "candidate",
    recommendedAction: "already_linked",
    ...overrides
  };
}

function makeEl(buttonsBySelector = {}) {
  return {
    innerHTML: "",
    dataset: {},
    querySelectorAll(selector) {
      return buttonsBySelector[selector] || [];
    }
  };
}

function makeButton(dataset) {
  return {
    dataset,
    addEventListener(_event, handler) {
      this.click = handler;
    }
  };
}

test("admin source policy review renders linked migration identities", () => {
  const reviewEl = makeEl();
  renderAdminSourcePolicyReview(reviewEl, {
    providerCoverageLinkBackfill: {
      linkedCandidates: [makeLinkedMigrationCandidate()]
    }
  });

  assert.match(reviewEl.innerHTML, /Linked Migration Identities/);
  assert.match(reviewEl.innerHTML, /Studio Greenhouse/);
  assert.match(reviewEl.innerHTML, /Studio Static/);
  assert.match(reviewEl.innerHTML, /Linked migration identity/);
  assert.match(reviewEl.innerHTML, /repeated successful provider fetches/);
  assert.match(reviewEl.innerHTML, /validated provider/);
  assert.match(reviewEl.innerHTML, /Success streak/);
  assert.match(reviewEl.innerHTML, /Latest kept/);
  assert.match(reviewEl.innerHTML, /candidate/);
  assert.match(reviewEl.innerHTML, />Clear link</);
  assert.doesNotMatch(reviewEl.innerHTML, />Apply link</);
});

test("linked migration identity action visibility allows only admin-owned clear", () => {
  assert.deepEqual(
    getMigrationLinkLinkedActions(makeLinkedMigrationCandidate()).map(action => action.key),
    ["clear_migration_identity_link"]
  );
  assert.deepEqual(
    getMigrationLinkLinkedActions(makeLinkedMigrationCandidate({
      migrationLinkedBy: "manual_import",
      adminBackfillOwned: false
    })).map(action => action.key),
    []
  );
  assert.deepEqual(
    getMigrationLinkLinkedActions(makeLinkedMigrationCandidate({
      migrationSourceIdentity: "static:listing_url:https://other.example/jobs"
    })).map(action => action.key),
    []
  );
});

test("provider-shaped migration link candidates render not applicable copy", () => {
  const reviewEl = makeEl();
  renderAdminSourcePolicyReview(reviewEl, {
    providerCoverageLinkBackfill: {
      reviewCandidates: [
        makeMigrationLinkCandidate({
          selectedStaticSourceId: "greenhouse:slug:studio",
          recommendedApiPayload: {
            ...makeMigrationLinkCandidate().recommendedApiPayload,
            staticSourceId: "greenhouse:slug:studio"
          }
        })
      ]
    }
  });

  assert.match(reviewEl.innerHTML, /Not applicable: selected static identity is provider-shaped/);
  assert.doesNotMatch(reviewEl.innerHTML, />Apply link</);
});

test("linked migration identity buttons call action handler", () => {
  const calls = [];
  const actionButton = makeButton({
    sourcePolicyMigrationLinkAction: "clear_migration_identity_link",
    sourcePolicyMigrationLinkKind: "linked",
    sourcePolicyMigrationLinkIndex: "0"
  });
  const reviewEl = makeEl({
    [ui(UI_TOKENS.admin.sourcePolicyMigrationLinkActionBtn)]: [actionButton]
  });
  renderAdminSourcePolicyReview(reviewEl, {
    providerCoverageLinkBackfill: {
      linkedCandidates: [makeLinkedMigrationCandidate()]
    }
  }, {
    onMigrationLinkAction(candidate, action) {
      calls.push({ action, providerSourceId: candidate.providerSourceId });
    }
  });

  actionButton.click();

  assert.deepEqual(calls, [
    { action: "clear_migration_identity_link", providerSourceId: "greenhouse:slug:studio" }
  ]);
});

test("renders suppression eligibility diagnostics without actions", () => {
  const reviewEl = makeEl();
  renderAdminSourcePolicyReview(reviewEl, {
    suppressionEligibility: {
      missingLinkedStaticRows: [
        {
          providerSourceId: "greenhouse:slug:studio",
          providerSourceName: "Studio Greenhouse",
          migrationSourceIdentity: "static:listing_url:https://studio.example/jobs",
          migrationSourceName: "Studio Static",
          providerCoverageStatus: "validated_provider",
          providerCoverageConsecutiveSuccesses: 2,
          providerCoverageLatestKeptCount: 4,
          providerReplacementReadiness: "ready_later",
          linkedStaticRegistryState: "active",
          reason: "linked_static_not_in_default_loader_set",
          selectionReason: "linked_static_not_in_default_loader_set",
          registryBucket: "active",
          registryState: "active",
          adapter: "static",
          expectedLoaderName: "static_source::static:listing_url:https://studio.example/jobs",
          foundInActiveRegistry: true,
          foundInDefaultLoaders: false,
          foundInSourceRows: false,
          excludedByCadenceOrCache: false,
          onlySourcesMode: false
        }
      ]
    }
  });

  assert.match(reviewEl.innerHTML, /Suppression Eligibility Visibility/);
  assert.match(reviewEl.innerHTML, /Provider ready, static not selected/);
  assert.match(reviewEl.innerHTML, /Studio Greenhouse/);
  assert.match(reviewEl.innerHTML, /Studio Static/);
  assert.match(reviewEl.innerHTML, /linked static not in default loader set/);
  assert.match(reviewEl.innerHTML, /static_source::static:listing_url:https:\/\/studio.example\/jobs/);
  assert.match(reviewEl.innerHTML, /Selected<\/strong> no/);
  assert.match(reviewEl.innerHTML, /Only sources<\/strong> no/);
  assert.match(reviewEl.innerHTML, /Success streak<\/strong> 2/);
  assert.match(reviewEl.innerHTML, /Latest kept<\/strong> 4/);
  assert.doesNotMatch(reviewEl.innerHTML, />Apply link</);
  assert.doesNotMatch(reviewEl.innerHTML, />Clear link</);
});
