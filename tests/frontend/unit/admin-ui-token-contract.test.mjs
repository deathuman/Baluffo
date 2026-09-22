// `admin.html` marks its DOM handles with `data-ui="<token>"`, and `UI_TOKENS.admin`
// maps a ref name to the token value that markup must carry. Nothing validated the
// two against each other, so five inspector handles shipped as
// `data-ui="inspectorOverlay"` while the registry expected
// `data-ui="admin-inspector-overlay"`.
//
// The failure mode is what makes this worth a guard: `cacheAdminDom` resolves each
// ref lazily and simply returns `null` for a selector that matches nothing, and
// `bindOverlayClose` skips silently when the overlay is missing. So the whole
// drawer was inert — the click delegate never bound, no error was thrown, and the
// console stayed clean. Only a real click on a real row revealed it.
//
// This walks the registry against the shipped markup and fails on any handle the
// page declares with a name the registry does not use.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { UI_TOKENS } from "../../../frontend/shared/ui/selectors.js";

const REPO_ROOT = fileURLToPath(new URL("../../../", import.meta.url));
const read = (relative) => readFileSync(`${REPO_ROOT}${relative}`, "utf8");

// Every `data-ui="..."` value the page actually declares.
function declaredTokens(html) {
  const tokens = new Set();
  for (const match of html.matchAll(/data-ui="([^"]+)"/g)) tokens.add(match[1]);
  return tokens;
}

// The token *values* the registry can resolve, for one registry bucket.
function registryValues(bucket) {
  return new Set(Object.values(UI_TOKENS[bucket] || {}));
}

test("no admin handle is declared with a camelCase name the registry spells differently", () => {
  const html = read("admin.html");
  const declared = declaredTokens(html);

  // The exact bug: the page carried `data-ui="inspectorOverlay"` while the registry
  // expected `data-ui="admin-inspector-overlay"`. Every other handle in the file is
  // kebab-case, so a camelCase value is the signature of this mistake.
  const camelCase = [...declared].filter((token) => /[a-z][A-Z]/.test(token));
  assert.deepEqual(
    camelCase,
    [],
    `these data-ui handles are camelCase while the registry uses kebab-case: ${camelCase.join(", ")}. `
    + "A mismatch resolves to null and disables the feature with no error."
  );
});

test("admin markup declares the data-ui tokens the registry resolves", () => {
  const html = read("admin.html");
  const declared = declaredTokens(html);

  // Tokens may legitimately come from any registry bucket, because the page shares
  // handles like `theme-toggle-btn` with the global registry. A token in *no*
  // bucket is the failure this catches — unless the page's own code selects it by
  // literal string, which `bulk-actions.js` does for two of its own handles.
  const known = new Set();
  for (const bucket of Object.values(UI_TOKENS)) {
    for (const token of Object.values(bucket)) known.add(token);
  }
  const selectedByLiteral = ["admin-bulk-busy-message", "admin-advanced-bulk-actions"];

  const unknown = [...declared].filter(
    (token) => !known.has(token) && !selectedByLiteral.includes(token)
  );
  assert.deepEqual(
    unknown,
    [],
    `admin.html declares data-ui handles that exist in no UI_TOKENS bucket: ${unknown.join(", ")}. `
    + "Either the markup is misspelled or the registry is missing the token."
  );
});

test("every admin registry token that the page uses is spelled exactly as declared", () => {
  const html = read("admin.html");
  const declared = declaredTokens(html);

  // The reverse direction, which is the one that broke: a registry token whose
  // value never appears in the markup resolves to `null` forever. Only tokens the
  // page is expected to carry are checked, so optional handles do not false-fail.
  const inspectorTokens = [
    "inspectorOverlay",
    "inspectorPanel",
    "inspectorTitle",
    "inspectorContent",
    "inspectorCloseBtn"
  ];
  for (const name of inspectorTokens) {
    const token = UI_TOKENS.admin[name];
    assert.ok(token, `UI_TOKENS.admin.${name} must exist`);
    assert.ok(
      declared.has(token),
      `admin.html must carry data-ui="${token}" for UI_TOKENS.admin.${name}; `
      + "without it the ref resolves to null and the inspector silently does nothing."
    );
  }
});

test("the inspector refs the controller depends on resolve to real elements", () => {
  const html = read("admin.html");

  // `init()` binds the overlay, the close button and the document click delegate.
  // With a null overlay, `bindOverlayClose` returns early and the drawer can never
  // open, so the elements must be present in the shipped markup.
  for (const token of ["admin-inspector-overlay", "admin-inspector-panel", "admin-inspector-content"]) {
    assert.match(
      html,
      new RegExp(`data-ui="${token}"`),
      `the inspector needs a [data-ui="${token}"] element or its ref is null`
    );
  }

  // The overlay must still ship hidden (it is revealed by removing the class), and
  // `hidden` must be the class the controller removes.
  assert.match(
    html,
    /class="inspector-overlay hidden"/,
    "the overlay must ship with the `hidden` class the controller removes on open"
  );
});
