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

test("shared source-list hover avoids transition-all on startup-visible lists", () => {
  assert.doesNotMatch(componentsCss, /\.sources-list\s+li\s*\{[^}]*transition:\s*all/s);
});

test("admin source tables reserve a fixed virtual viewport height", () => {
  assert.match(adminCss, /#admin-pending-sources[\s\S]*min-height:\s*calc\(var\(--admin-source-row-height\)/);
  assert.match(adminCss, /#admin-pending-sources \.jobs-table-body[\s\S]*height:\s*calc\(var\(--admin-source-visible-rows\)/);
  assert.match(adminCss, /\.admin-source-row\s*\{[\s\S]*height:\s*var\(--admin-source-row-height/);
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
