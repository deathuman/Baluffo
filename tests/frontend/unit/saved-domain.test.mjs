import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizeCustomJobInput,
  activityTypeLabel,
  formatActivityDetail
} from "../../../frontend/saved/domain.js";

test("saved domain normalizes custom job payload", () => {
  const row = normalizeCustomJobInput({ title: "Dev", company: "Studio", country: "usa" }, { customSourceLabel: "Custom" });
  assert.equal(row.title, "Dev");
  assert.equal(row.company, "Studio");
  assert.equal(row.country, "US");
  assert.equal(row.customSourceLabel, "Custom");
});

test("saved domain maps activity labels/details", () => {
  assert.equal(activityTypeLabel("job_saved"), "Saved");
  const detail = formatActivityDetail(
    { type: "phase_changed", details: { previousStatus: "bookmark", nextStatus: "applied" } },
    {
      normalizePhase: value => value,
      phaseLabels: { bookmark: "Saved", applied: "Applied" },
      formatPhaseTimestamp: () => ""
    }
  );
  assert.equal(detail, "Saved -> Applied");
});
