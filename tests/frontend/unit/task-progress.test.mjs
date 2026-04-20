import test from "node:test";
import assert from "node:assert/strict";

import { formatScrapyStaticSourcesTailBadge } from "../../../frontend/shared/task-progress.js";

test("formatScrapyStaticSourcesTailBadge returns empty when queue item is absent", () => {
  assert.equal(formatScrapyStaticSourcesTailBadge([]), "");
  assert.equal(
    formatScrapyStaticSourcesTailBadge([{ id: "studio_a", status: "running" }]),
    ""
  );
});

test("formatScrapyStaticSourcesTailBadge returns fallback progress for active scrapy queue", () => {
  assert.equal(
    formatScrapyStaticSourcesTailBadge([
      {
        id: "scrapy_static_sources",
        status: "running",
        progress: {
          active: true,
          phaseKey: "loading_source",
          phaseLabel: "Processing browser fallback queue",
          counts: {
            completedSources: 19,
            totalSources: 26
          }
        }
      }
    ]),
    "Browser fallback 19/26"
  );
});

test("formatScrapyStaticSourcesTailBadge ignores malformed or inactive queue counts", () => {
  assert.equal(
    formatScrapyStaticSourcesTailBadge([
      {
        id: "scrapy_static_sources",
        status: "ok",
        progress: {
          counts: {
            completedSources: 19,
            totalSources: 26
          }
        }
      }
    ]),
    ""
  );
  assert.equal(
    formatScrapyStaticSourcesTailBadge([
      {
        id: "scrapy_static_sources",
        status: "running",
        progress: {
          counts: {
            completedSources: "x",
            totalSources: 26
          }
        }
      }
    ]),
    ""
  );
});
