import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { renderSourcesTableHtml } from "../../../frontend/admin/render/sources.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
const css = fs.readFileSync(path.join(repoRoot, "styles", "admin.css"), "utf8");

function renderSource() {
  return renderSourcesTableHtml(
    [{ id: "source-1", name: "Example Studio", adapter: "static", listing_url: "https://example.test/jobs" }],
    "pending",
    () => "0",
    () => "warning",
    () => ({ label: "Pending", tone: "warning", title: "Pending review" }),
  );
}

test("source ID affordance uses a full-size theme-aware glyph and focus target", () => {
  const html = renderSource();
  assert.match(html, /class="admin-source-id-glyph" viewBox="0 0 24 24" width="16" height="16"/);
  assert.match(html, /class="admin-source-id-inline"[^>]*tabindex="0" role="img"/);
  assert.match(html, /class="admin-source-id-inline"[^>]*data-tooltip="source-1"/);

  const rule = css.match(/\.admin-source-id-inline\s*\{[\s\S]*?\n\}/)?.[0] || "";
  assert.match(rule, /width:\s*32px/);
  assert.match(rule, /height:\s*32px/);
  assert.match(rule, /margin:\s*-8px/);
  assert.match(rule, /flex-shrink:\s*0/);
  assert.match(rule, /color:\s*var\(--text-secondary\)/);
  assert.doesNotMatch(rule, /opacity\s*:/);
  assert.doesNotMatch(rule, /border:/);
});

test("advanced bulk disclosure has one authored indicator", () => {
  const summary = css.match(/\.admin-advanced-bulk-summary\s*\{[\s\S]*?\n\}/)?.[0] || "";
  assert.match(summary, /display:\s*flex/);
  assert.match(css, /\.admin-advanced-bulk-summary::marker\s*\{[\s\S]*?content:\s*["']?["']?/);
  assert.match(css, /\.admin-advanced-bulk-summary::after\s*\{[\s\S]*?content:\s*["']?>["']/);
  assert.match(css, /\.admin-advanced-bulk-summary::-webkit-details-marker\s*\{[\s\S]*?display:\s*none/);
});

test("run trends disclosure separates its following tabs", () => {
  const rules = [...css.matchAll(/\.admin-ops-trends-details\s*\{[\s\S]*?\n\}/g)].map(match => match[0]).join("\n");
  assert.match(rules, /margin-bottom:\s*0\.65rem/);
  assert.match(css, /\.admin-ops-trends-details \.results-summary\s*\{[\s\S]*?margin:\s*0/);
});
