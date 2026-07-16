import test from "node:test";
import assert from "node:assert/strict";

import { createSavedBoot } from "../../../frontend/saved/app/runtime/boot.js";
import { createButton, createElement } from "./helpers/saved-runtime-helpers.mjs";

test("Saved renders the current availability-attention summary", async () => {
  const banner = createElement();
  const count = createElement();
  const filter = createButton({ dataset: { savedFilter: "availability_attention" } });
  const boot = createSavedBoot({
    startupMetrics: null,
    dom: {
      availabilityAttentionBannerEl: banner,
      availabilityAttentionCountEl: count,
      savedCustomFilterBtnEls: [filter]
    },
    viewState: { currentUser: { uid: "user-1" } },
    savedPageService: {
      async getAvailabilityAttention(uid) {
        assert.equal(uid, "user-1");
        return { ok: true, data: { count: 2, events: [{ transitionId: "t-1" }] } };
      }
    }
  });

  await boot.refreshAvailabilityAttention();

  assert.equal(banner.classList.contains("hidden"), false);
  assert.equal(count.textContent, "2 availability updates need attention.");
  assert.equal(filter.textContent, "Availability attention (2)");
});
