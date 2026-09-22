// The Action Center panel used to render all five states through one function, so
// "All systems operational" and "Operational signals unavailable" produced identical
// markup apart from the text. It also used emoji and ASCII glyphs where the rest of
// the page uses monochrome inline SVG.
//
// These tests pin the two properties that make the panel legible: every state is
// distinguishable by its tone and chip, and every icon is an SVG path rather than a
// text glyph. They exercise the renderer directly — it is a pure string builder, so
// no DOM or controller is needed.

import test from "node:test";
import assert from "node:assert/strict";

import {
  ACTION_CENTER_ICONS,
  actionCenterIcon,
  formatCheckedAt,
  renderActionCenterBody,
  renderCheckedAt,
  renderSignalRow,
  renderStatusChip,
  resolveSeverity,
  severityChipClass
} from "../../../frontend/admin/render/action-center.js";

// Codepoints the old panel used: `i`, ⚠, ⏰, ↔️, ❌, ✓, ▶, 🔄, 📋, ✕.
// Any of them appearing in output means a glyph slipped back in.
const LEGACY_GLYPHS = /[\u26A0\u23F0\u2194\u274C\u2713\u25B6\u2715\uD83D\uDD04\uD83D\uDCCB\uFE0F]/u;

function signal(overrides = {}) {
  return {
    id: "stale_fetch",
    severity: "warning",
    label: "Jobs fetch is stale",
    summary: "Last successful fetch was 13h ago",
    actions: ["review", "retry_fetch", "dismiss"],
    ...overrides
  };
}

test("every state renders a distinct tone and chip", () => {
  const states = ["healthy", "partial", "checking", "active-work-delayed", "unavailable"];
  const chips = new Map();
  const tones = new Map();

  for (const state of states) {
    const chipHtml = renderStatusChip({ state });
    const bodyHtml = renderActionCenterBody({ state, summary: `line for ${state}` });
    chips.set(state, chipHtml);
    tones.set(state, bodyHtml);
    assert.match(bodyHtml, new RegExp(`action-center-status-`), `${state} must carry a tone class`);
    assert.match(bodyHtml, new RegExp(`data-state="${state}"`), `${state} must be tagged`);
  }

  // The regression this guards: one shared shape for every state.
  assert.equal(new Set(tones.values()).size, states.length, "each state must render distinct markup");
});

test("a passing check is visually distinguishable from a failure", () => {
  const healthy = renderActionCenterBody({ state: "healthy" });
  const unavailable = renderActionCenterBody({ state: "unavailable" });

  assert.match(healthy, /action-center-status-ok/);
  assert.match(healthy, /All systems operational/);
  assert.match(unavailable, /action-center-status-warning/);
  assert.notEqual(healthy, unavailable);
  // The healthy chip is the affirmative tone, not the warning tone.
  assert.match(renderStatusChip({ state: "healthy" }), /healthy/);
  assert.doesNotMatch(renderStatusChip({ state: "healthy" }), /warning/);
});

test("signal rows carry severity, message and delegated action attributes", () => {
  const html = renderSignalRow(signal());

  assert.match(html, /action-center-signal-warning/);
  assert.match(html, /data-signal="stale_fetch"/);
  assert.match(html, /Jobs fetch is stale/);
  assert.match(html, /Last successful fetch was 13h ago/);
  // `bindEvents` delegates on these; losing them breaks every action button.
  assert.match(html, /data-action="review"/);
  assert.match(html, /data-action="retry"[^>]*data-preset="default"/);
  assert.match(html, /data-action="dismiss"/);
});

test("a critical signal renders the critical tone and chip", () => {
  const html = renderSignalRow(signal({ id: "storage_health", severity: "critical" }));
  assert.match(html, /action-center-signal-critical/);
  assert.match(html, />critical</);
});

test("no legacy text glyph survives in any rendered output", () => {
  const outputs = [
    ...["healthy", "partial", "checking", "active-work-delayed", "unavailable"]
      .map(state => renderActionCenterBody({ state, summary: "x" })),
    renderActionCenterBody({ state: "healthy", signals: [signal()] }),
    renderActionCenterBody({ state: "healthy", signals: [signal({ severity: "critical" })] }),
    renderStatusChip({ state: "healthy" }),
    renderStatusChip({ state: "healthy", signals: [signal()] }),
    renderCheckedAt(Date.now(), Date.now()),
    actionCenterIcon("refresh")
  ];

  for (const html of outputs) {
    assert.doesNotMatch(html, LEGACY_GLYPHS, `legacy glyph found in: ${html.slice(0, 120)}`);
  }
});

test("icons are monochrome inline SVG that inherit currentColor", () => {
  for (const [name, path] of Object.entries(ACTION_CENTER_ICONS)) {
    const html = actionCenterIcon(name);
    assert.match(html, /<svg/, `${name} must render an svg`);
    assert.match(html, /viewBox="0 0 24 24"/, `${name} must use the shared viewBox`);
    assert.match(html, /fill="currentColor"/, `${name} must inherit the theme colour`);
    assert.match(html, /aria-hidden="true"/, `${name} must stay decorative`);
    assert.equal(path.length > 10, true, `${name} must be a real path`);
  }
  // An unknown name degrades to a real icon rather than emitting `undefined`.
  assert.match(actionCenterIcon("does-not-exist"), /<path fill="currentColor" d="M/);
  assert.doesNotMatch(actionCenterIcon("does-not-exist"), /undefined/);
});

test("the chip reports issue count and worst severity", () => {
  assert.equal(resolveSeverity([]), "healthy");
  assert.equal(resolveSeverity([signal()]), "warning");
  assert.equal(resolveSeverity([signal(), signal({ severity: "critical" })]), "critical");
  assert.equal(severityChipClass("critical"), "critical");

  const two = renderStatusChip({ state: "healthy", signals: [signal(), signal({ id: "sync_status" })] });
  assert.match(two, /2 issues/);
  assert.match(two, /critical|warning/);

  const one = renderStatusChip({ state: "healthy", signals: [signal()] });
  assert.match(one, /1 issue</, "a single issue must not be pluralised");
});

test("the checked-at stamp ages and stays empty before the first poll", () => {
  const now = 1_700_000_000_000;
  assert.equal(formatCheckedAt(0, now), "");
  assert.equal(formatCheckedAt(undefined, now), "");
  assert.equal(formatCheckedAt(Number.NaN, now), "");
  assert.equal(formatCheckedAt(now, now), "checked just now");
  assert.equal(formatCheckedAt(now - 30_000, now), "checked 30s ago");
  assert.equal(formatCheckedAt(now - 5 * 60_000, now), "checked 5m ago");
  assert.equal(formatCheckedAt(now - 3 * 3_600_000, now), "checked 3h ago");
  // A clock skew must not produce a negative age.
  assert.equal(formatCheckedAt(now + 10_000, now), "checked just now");

  assert.equal(renderCheckedAt(0, now), "", "no stamp before the first poll");
  assert.match(renderCheckedAt(now - 30_000, now), /checked 30s ago/);
});

test("every active signal renders, with no unreachable overflow row", () => {
  // `stale_fetch` (age > 12h) and `failed_sources` (age <= 12h) are mutually
  // exclusive, so at most three signals can be active at once and the display cap is
  // three. The "View all" overflow row that used to sit behind that cap could never
  // render and was removed rather than left as dead UI.
  const three = [signal(), signal({ id: "sync_status" }), signal({ id: "failed_sources" })];
  const html = renderActionCenterBody({ state: "healthy", signals: three });

  assert.equal((html.match(/class="action-center-signal /g) || []).length, 3);
  assert.doesNotMatch(html, /view-all/);
  assert.doesNotMatch(html, /View all/);
});

test("a signal with no actions renders no buttons and no empty container", () => {
  const html = renderSignalRow(signal({ actions: [] }));
  assert.doesNotMatch(html, /data-action=/);
  assert.match(html, /action-center-signal-actions"><\/div>/);
});

test("an unknown action name is skipped rather than emitting a broken button", () => {
  const html = renderSignalRow(signal({ actions: ["review", "not-a-real-action"] }));
  assert.match(html, /data-action="review"/);
  assert.doesNotMatch(html, /not-a-real-action/);
  assert.equal((html.match(/<button/g) || []).length, 1);
});

test("a signal summary is never dropped, even when it resembles the label", () => {
  // A text-similarity filter was tried here and rejected: it scored the useful sync
  // summary as a restatement and hid it. Both fields must always render.
  const html = renderSignalRow(signal({
    id: "sync_status",
    label: "Sync needs attention",
    summary: "Sync conflict needs review; data refresh can continue"
  }));
  assert.match(html, /Sync needs attention/);
  assert.match(html, /Sync conflict needs review; data refresh can continue/);
});
