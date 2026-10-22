// The pages are set in Segoe UI, but Segoe UI's cmap has no entry for a handful of
// symbols the UI used to draw as text: `＋` (save), `✓` (saved / step complete),
// `▸`/`▾` (discovery-log disclosure). Chromium resolved each one from another
// family — Microsoft YaHei, Segoe UI Symbol, Cambria Math — paying for a whole
// typeface to paint a single character, and drawing an arrow in a serif math face.
//
// The guard has three parts: no page markup carries a glyph the font lacks, no
// CSS `content:` string does either, and the tick is a drawn path instead of a
// typed character. `…` is in the font, so it never tripped the codepoint scan;
// it is covered instead by the per-state drawing assertions below.

import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { renderJobRow } from "../../../frontend/shared/components/JobRow.js";
import { renderPhaseBar } from "../../../frontend/saved/render.js";
import { renderCheckIcon } from "../../../frontend/shared/ui-helpers.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

// Codepoints verified absent from every Segoe UI face (segoeui*.ttf / seguibl*.ttf)
// on Windows 11, i.e. guaranteed to be resolved from a fallback font. Astral
// emoji are written as full code points so the unicode-flagged class cannot
// combine halves with neighbouring characters; U+FE0F is omitted because it only
// varies how a glyph is drawn.
const FONT_MISSING_GLYPHS = /[\u2713\u2714\u2715\u2717\u25B6\u25B8\u25BE\u25C6\u2605\u26A0\u23F0\u274C\uFF0B\u2611\u{1F4CB}\u{1F504}]/u;

function read(file) {
  return fs.readFileSync(path.join(repoRoot, file), "utf8");
}

// `content: "\25B8"` looks like ASCII in the source and is a fallback at render
// time, so escapes are decoded before the scan. The old arrow was written exactly
// that way, which is why the scan cannot look at raw source text alone.
function decodeCssEscapes(text) {
  return text
    .replace(/\\([0-9a-fA-F]{1,6})\s?/g, (_, hex) => String.fromCodePoint(parseInt(hex, 16)))
    .replace(/\\(.)/g, "$1");
}

const CONTENT_STRING = /content\s*:\s*("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')/g;

function contentGlyphs(css) {
  const findings = [];
  for (const match of css.matchAll(CONTENT_STRING)) {
    for (const char of decodeCssEscapes(match[1].slice(1, -1))) {
      if (char.codePointAt(0) > 127) findings.push({ char, declaration: match[0] });
    }
  }
  return findings;
}

test("page markup renders no glyph the UI font lacks", () => {
  for (const page of ["jobs.html", "admin.html", "saved.html"]) {
    const match = FONT_MISSING_GLYPHS.exec(read(page));
    assert.equal(match, null, `${page} contains U+${match?.[0].codePointAt(0).toString(16).toUpperCase()}`);
  }
});test("CSS content strings use only characters Segoe UI carries", () => {
  // Segoe UI covers these typographic marks; anything else in `content:` is a
  // fallback waiting to happen. Keep the list to punctuation the font is known to
  // have rather than growing it to make a test pass.
  const covered = new Set(["·", "…", "—", "–"]);
  // The scan must recognise both the literal glyph and its CSS escape.
  assert.equal(decodeCssEscapes('"\\2713"').slice(1, -1), "\u2713");
  const findings = [];
  for (const file of fs.readdirSync(path.join(repoRoot, "styles")).filter(name => name.endsWith(".css"))) {
    // Comments are stripped first: prose about a glyph is not a declaration.
    const css = read(path.join("styles", file)).replace(/\/\*[\s\S]*?\*\//g, "");
    for (const { char, declaration } of contentGlyphs(css)) {
      if (!covered.has(char)) {
        findings.push(`${file}: ${declaration} (U+${char.codePointAt(0).toString(16).toUpperCase()})`);
      }
    }
  }
  assert.deepEqual(findings, []);
});

test("the discovery-log disclosure arrow is drawn, not typed", () => {
  const css = read("styles/admin.css").replace(/\/\*[\s\S]*?\*\//g, "");
  assert.doesNotMatch(css, /content\s*:\s*"[^"]*\\25B[8E]/);
  const closed = css.match(/\.admin-discovery-log-summary::after\s*\{([^}]*)\}/)?.[1] || "";
  assert.match(closed, /border-top:[^;]*currentColor/, "the closed marker must be a border triangle");
  assert.match(closed, /transform:\s*rotate\(-90deg\)/, "the closed marker must point right");
  const open = css.match(/\.admin-discovery-log-card\[open\]\s+\.admin-discovery-log-summary::after\s*\{([^}]*)\}/)?.[1] || "";
  assert.match(open, /transform:\s*rotate\(0deg\)/, "the open marker must point down");
});

test("the tick is a drawn path, not a typed character", () => {
  const icon = renderCheckIcon();
  assert.match(icon, /<svg viewBox="0 0 24 24"[^>]*aria-hidden="true"/);
  assert.match(icon, /d="M9 16\.17/);
  assert.match(icon, /fill="currentColor"/);
  assert.doesNotMatch(icon, FONT_MISSING_GLYPHS);

  const css = read("styles/jobs.css") + read("styles/saved.css");
  assert.doesNotMatch(
    css,
    /--icon-check-mask|linear-gradient\(transparent, transparent\)/,
    "the tick must not depend on a mask token that can fail open"
  );
});

function jobRow(overrides = {}) {
  return {
    id: "1",
    title: "Gameplay Engineer",
    company: "Studio",
    sector: "Game",
    city: "Boston",
    country: "United States",
    workType: "Remote",
    contractType: "Full-time",
    jobLink: "https://example.com/job",
    ...overrides
  };
}

const rowOptions = {
  fullCountryName: value => value,
  sanitizeUrl: value => value,
  getJobKeyForJob: job => `key_${job.id}`,
  savedJobKeys: new Set(["key_1"]),
  pendingSavedJobActions: new Map(),
  isJobsApiReady: () => true,
  canManageAvailability: false,
  toContractClass: () => "contract-fulltime",
  capitalizeFirst: value => String(value).charAt(0).toUpperCase() + String(value).slice(1)
};

test("the save button's states carry no text glyph Segoe UI lacks", () => {
  const unsaved = renderJobRow(jobRow(), { ...rowOptions, savedJobKeys: new Set() });
  const saved = renderJobRow(jobRow(), rowOptions);
  const pending = renderJobRow(jobRow(), {
    ...rowOptions,
    savedJobKeys: new Set(),
    pendingSavedJobActions: new Map([["key_1", "saving"]])
  });
  assert.doesNotMatch(unsaved, FONT_MISSING_GLYPHS);
  assert.doesNotMatch(saved, FONT_MISSING_GLYPHS);
  assert.doesNotMatch(pending, FONT_MISSING_GLYPHS);
  // The unsaved state draws its plus. A typed "+" carries the same font-metric
  // problem in milder form: its ink rode 1.5px below the button's centre.
  assert.match(unsaved, /<span aria-hidden="true">\s*<svg viewBox="0 0 24 24"/);
  assert.match(unsaved, /<path d="M10\.75 5h2\.5v5\.75h5\.75v2\.5h-5\.75v5\.75h-2\.5v-5\.75H5v-2\.5h5\.75z"/);
  assert.doesNotMatch(unsaved, /<span aria-hidden="true">\s*\+\s*<\/span>/);
  // The saved state draws the shared path; it never types the tick character.
  assert.match(saved, /class="save-job-btn job-inline-save-btn saved"/);
  assert.match(saved, /<svg viewBox="0 0 24 24"/);
  assert.match(saved, /<path d="M9 16\.17/);
  // So does the pending state: its mark is a ring on the box centre, the third
  // of the button's states to stop being a typed character.
  assert.match(pending, /<span aria-hidden="true">\s*<svg viewBox="0 0 24 24"/);
  assert.match(pending, /<circle cx="12" cy="12" r="8\.6"/);
  assert.doesNotMatch(pending, /<span aria-hidden="true">\s*…\s*<\/span>/);
});

test("the completed phase step draws its tick instead of typing one", () => {
  const html = renderPhaseBar(
    "key_1",
    "applied",
    { applied: "2026-03-08T10:00:00.000Z" },
    "2026-03-08T09:00:00.000Z",
    {
      phaseOptions: ["bookmark", "applied", "offer"],
      phaseLabels: { bookmark: "Saved", applied: "Applied", offer: "Offer" },
      canTransition: () => false,
      currentUser: { uid: "u1" }
    }
  );
  assert.match(html, /class="phase-step-check">\s*<svg viewBox="0 0 24 24"/);
  assert.match(html, /<path d="M9 16\.17/);
  assert.doesNotMatch(html, FONT_MISSING_GLYPHS);
});
