import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";
import {
  createAttrButton as makeAttrButton,
  createButtonMapEl as makeEl
} from "./helpers/dom-test-helpers.mjs";

test("admin render: browser fallback queue health shows duplicates, kept sources, and parser rows", () => {
  const refreshButton = makeAttrButton({});
  const metricsEl = makeEl({
    '[data-action="refresh-browser-fallback-queue"]': [refreshButton]
  });
  let refreshed = 0;

  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {},
    history: {},
    browserFallbackQueue: {
      ok: true,
      exists: true,
      readable: true,
      queueFile: "jobs-browser-fallback-queue.json",
      pathDisplay: "data/jobs-browser-fallback-queue.json",
      modifiedAt: "2026-11-08T12:00:00Z",
      rowCount: 4,
      duplicatePageCount: 1,
      duplicateRowCount: 2,
      readerCollapsedRowCount: 1,
      parseErrorRowCount: 1,
      issueCount: 2,
      clean: false,
      duplicatePages: [
        {
          page: "https://example.test/careers",
          rowCount: 2,
          eligibleRowCount: 2,
          readerCollapsedRowCount: 1,
          sourceIds: ["source-a", "source-b"],
          classifications: ["timeout", "anti_bot_or_challenge"],
          readerKeptSourceId: "source-a"
        }
      ],
      parseErrorRows: [
        {
          page: "https://parser.test/careers",
          sourceId: "parser-source",
          name: "Parser Board",
          classification: "parse_error",
          reason: "no jobs extracted from listing"
        }
      ],
      truncated: { duplicatePages: false, parseErrorRows: false },
      warnings: []
    }
  }, null, {
    onRefreshBrowserFallbackQueue: () => {
      refreshed += 1;
    }
  });

  const html = metricsEl.innerHTML;
  assert.match(html, /Browser fallback queue/i);
  assert.match(html, /1 duplicate page/i);
  assert.match(html, /1 reader-collapsed row/i);
  assert.match(html, /1 non-allowlisted parser row/i);
  assert.match(html, /https:\/\/example\.test\/careers/i);
  assert.match(html, /source-a/);
  assert.match(html, /source-b/);
  assert.match(html, /https:\/\/parser\.test\/careers/i);
  assert.match(html, /parser-source/);
  assert.match(html, /no jobs extracted from listing/i);
  assert.match(html, /Refresh queue/i);

  refreshButton.click();
  assert.equal(refreshed, 1);
});

test("admin render: browser fallback queue health exposes the not-loaded state", () => {
  const metricsEl = makeEl();

  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {},
    history: {},
    browserFallbackQueue: {}
  });

  assert.match(metricsEl.innerHTML, /Browser fallback queue/i);
  assert.match(metricsEl.innerHTML, /diagnostics not loaded/i);
  assert.match(metricsEl.innerHTML, /Refresh queue/i);
});
