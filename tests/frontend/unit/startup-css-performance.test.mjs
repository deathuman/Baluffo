import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

const adminCss = readFileSync(new URL("../../../styles/admin.css", import.meta.url), "utf8");
const baseCss = readFileSync(new URL("../../../styles/base.css", import.meta.url), "utf8");
const jobsCss = readFileSync(new URL("../../../styles/jobs.css", import.meta.url), "utf8");
const componentsCss = readFileSync(new URL("../../../styles/components.css", import.meta.url), "utf8");

test("jobs startup CSS does not animate header or filters into place", () => {
  assert.doesNotMatch(jobsCss, /jobsIntro/);
  assert.doesNotMatch(jobsCss, /\.jobs-page\s+\.jobs-header\s*\{[^}]*animation:/s);
  assert.doesNotMatch(jobsCss, /\.jobs-page\s+\.filters\s*\{[^}]*animation:/s);
});

test("admin bridge startup pulse avoids box-shadow animation", () => {
  assert.doesNotMatch(adminCss, /adminBridgePulseBg/);
  assert.doesNotMatch(adminCss, /@keyframes\s+adminBridgePulse[^}]+box-shadow/s);
});

test("admin running task rows use neutral active styling", () => {
  const runningRule = adminCss.match(/\.admin-ops-history-row-running\s*\{[\s\S]*?\}/)?.[0] || "";
  assert.match(runningRule, /var\(--accent-soft-bg\)/);
  assert.match(runningRule, /var\(--accent\)/);
  assert.doesNotMatch(runningRule, /contract-temporary|danger/i);
});

test("shared source-list hover avoids transition-all on startup-visible lists", () => {
  assert.doesNotMatch(componentsCss, /\.sources-list\s+li\s*\{[^}]*transition:\s*all/s);
});

test("admin source tables reserve a fixed virtual viewport height", () => {
  assert.match(adminCss, /#admin-pending-sources[\s\S]*min-height:\s*calc\(var\(--admin-source-row-height\)/);
  assert.match(adminCss, /#admin-pending-sources \.jobs-table-body[\s\S]*height:\s*calc\(var\(--admin-source-visible-rows\)/);
  assert.match(adminCss, /\.admin-source-row\s*\{[\s\S]*height:\s*var\(--admin-source-row-height/);
});

test("admin checkboxes use one polished source-list style", () => {
  const checkboxRule = adminCss.match(/input\[type="checkbox"\]\s*\{[\s\S]*?\}/)?.[0] || "";
  assert.match(checkboxRule, /appearance:\s*none/);
  assert.match(checkboxRule, /width:\s*14px/);
  assert.match(checkboxRule, /height:\s*14px/);
  assert.match(checkboxRule, /border-radius:\s*3px/);
  assert.match(checkboxRule, /color-mix\(in srgb,\s*#d6d6d6 72%,\s*var\(--surface-11\)\)/);
  assert.match(adminCss, /input\[type="checkbox"\]:checked::after\s*\{[\s\S]*transform:\s*rotate\(45deg\)/);
  assert.match(adminCss, /input\[type="checkbox"\]:indeterminate::after\s*\{[\s\S]*border-radius:\s*999px/);
  assert.doesNotMatch(adminCss, /\.pending-source-checkbox,[\s\S]*?\.rejected-source-checkbox\s*\{/);
});

test("admin scrollbars use the source-list style everywhere", () => {
  assert.match(adminCss, /#admin-pending-sources \.jobs-table-body/);
  assert.match(adminCss, /\.admin-fetcher-log/);
  assert.match(adminCss, /\.admin-registry-conflicts-list/);
  assert.match(adminCss, /\.admin-ops-history-older \.admin-ops-history-older-scroll/);
  assert.match(adminCss, /\.inspector-content/);
  assert.match(adminCss, /scrollbar-color:\s*var\(--surface-18\)\s+var\(--surface-1\)/);
  assert.match(adminCss, /::-webkit-scrollbar-thumb\s*\{[\s\S]*background:\s*var\(--surface-18\)/);
  assert.match(adminCss, /::-webkit-scrollbar-thumb:hover\s*\{[\s\S]*background:\s*var\(--surface-20\)/);
  assert.doesNotMatch(adminCss, /\.admin-fetcher-log::-webkit-scrollbar-thumb\s*\{/);
});

test("admin first-paint dynamic panels reserve stable height before data arrives", () => {
  assert.match(adminCss, /#admin-source-status\.source-status\s*\{[\s\S]*min-height:/);
  assert.match(adminCss, /\.action-center-items\s*\{[\s\S]*min-height:/);
  assert.match(adminCss, /\.admin-totals\s*\{[\s\S]*min-height:/);
  assert.match(adminCss, /#admin-users-list\.jobs-list\s*\{[\s\S]*min-height:/);
  assert.match(adminCss, /#admin-ops-history\.jobs-list\s*\{[\s\S]*min-height:/);
  assert.match(adminCss, /\.admin-ops-alerts\s*\{[\s\S]*min-height:/);
  assert.match(adminCss, /\.admin-ops-kpis\s*\{[\s\S]*min-height:/);
  assert.match(adminCss, /\.admin-ops-schedule\s*\{[\s\S]*min-height:/);
});

test("admin optional ops slots collapse when empty", () => {
  assert.match(adminCss, /\.admin-ops-alerts:empty,[\s\S]*#admin-ops-fetcher-metrics:empty\s*\{[\s\S]*min-height:\s*0/);
  assert.match(adminCss, /\.admin-ops-alerts:empty,[\s\S]*#admin-ops-fetcher-metrics:empty\s*\{[\s\S]*margin-bottom:\s*0/);
});

test("admin older runs use a bounded scroll area", () => {
  const rule = adminCss.match(/\.admin-ops-history-older \.admin-ops-history-older-scroll\s*\{[\s\S]*?\}/)?.[0] || "";
  assert.match(rule, /max-height:\s*clamp\(18rem,\s*52vh,\s*34rem\)/);
  assert.match(rule, /overflow-y:\s*auto/);
  assert.match(rule, /scrollbar-gutter:\s*stable/);
});

test("jobs first-paint layout reserves scroll gutter and late content space", () => {
  assert.match(baseCss, /html\s*\{[\s\S]*scrollbar-gutter:\s*stable/);
  assert.match(jobsCss, /\.jobs-page \.source-status\s*\{[\s\S]*min-height:/);
  assert.match(jobsCss, /\.jobs-page \.results-summary\s*\{[\s\S]*min-height:/);
  assert.match(jobsCss, /\.jobs-page \.jobs-list\s*\{[\s\S]*min-height:\s*calc\(2\.95rem \+ \(10 \* 3\.35rem\)\)/);
  assert.match(jobsCss, /\.jobs-page \.pagination\s*\{[\s\S]*min-height:/);
});

test("jobs startup-visible controls avoid border and color transitions", () => {
  assert.match(jobsCss, /\.jobs-page \.page-nav-btn,[\s\S]*\.jobs-page \.country-picker-btn\s*\{[\s\S]*transition-property:\s*opacity,\s*transform,\s*background-color;/);
});
