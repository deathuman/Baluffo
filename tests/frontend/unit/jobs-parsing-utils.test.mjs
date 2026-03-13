import assert from "node:assert/strict";
import { parseCSVLarge } from "../../../jobs-parsing-utils.js";

test("jobs parsing utils prefers alternate employer over untrustworthy label", () => {
  const csv = [
    "Company,Company Name,City,Country,Title,Job Link",
    "giant enemy crab,Actual Studio,Amsterdam,NL,Gameplay Engineer,https://example.com/jobs/77"
  ].join("\n");

  const rows = parseCSVLarge(csv, {});
  assert.equal(rows.length, 1);
  assert.equal(rows[0].company, "Actual Studio");
});

test("jobs parsing utils preserves untrustworthy employer as unknown company", () => {
  const csv = [
    "Company,City,Country,Title,Job Link",
    "FarBridge,Amsterdam,NL,Gameplay Engineer,https://example.com/jobs/88"
  ].join("\n");

  const rows = parseCSVLarge(csv, {});
  assert.equal(rows.length, 1);
  assert.equal(rows[0].company, "Unknown company");
});
