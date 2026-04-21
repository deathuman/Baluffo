import test from "node:test";
import assert from "node:assert/strict";

import {
  openReleaseNotesDialog,
  renderReleaseNotesMarkdown
} from "../../../frontend/shared/ui/release-notes-dialog.js";
import { createFakeDocument } from "./helpers/browser-test-helpers.mjs";

test("renderReleaseNotesMarkdown renders headings, lists, inline code, and links", () => {
  const doc = createFakeDocument();
  const container = doc.createElement("div");
  const openedUrls = [];
  doc.body.appendChild(container);

  renderReleaseNotesMarkdown(
    container,
    "# Added\n\n- Read the [guide](https://example.com/guide)\n- Use `baluffo update`\n\nParagraph text.",
    {
      documentTarget: doc,
      openExternalUrl: url => openedUrls.push(url),
      windowTarget: doc.defaultView,
    }
  );

  assert.equal(container.children[0].tagName, "H3");
  assert.equal(container.children[0].textContent, "Added");

  const list = container.children[1];
  assert.equal(list.tagName, "UL");
  assert.equal(list.children.length, 2);

  const guideLink = doc.find(node => node.tagName === "A" && node.href === "https://example.com/guide");
  assert.ok(guideLink);
  guideLink.dispatch("click");
  assert.deepEqual(openedUrls, ["https://example.com/guide"]);

  const inlineCode = doc.find(node => node.tagName === "CODE");
  assert.ok(inlineCode);
  assert.equal(inlineCode.textContent, "baluffo update");
  assert.equal(container.children[2].tagName, "P");
  assert.equal(container.children[2].textContent, "Paragraph text.");
});

test("openReleaseNotesDialog renders fallback state, supports escape close, and restores focus", () => {
  const doc = createFakeDocument();
  const trigger = doc.createElement("button");
  const openedUrls = [];
  doc.body.appendChild(trigger);
  trigger.focus();

  const dialog = openReleaseNotesDialog({
    title: "Baluffo v0.1.2",
    markdown: "",
    publishedAt: "2026-04-15T10:00:00Z",
    releaseNotesUrl: "https://example.com/releases/v0.1.2",
    openExternalUrl: url => openedUrls.push(url),
    fallbackMessage: "Release notes are unavailable in-app for this build.",
    documentTarget: doc,
    windowTarget: doc.defaultView,
  });

  assert.ok(dialog);
  assert.equal(dialog.overlay.parentNode, doc.body);
  assert.match(dialog.overlay.className, /\bpopup-overlay-visible\b/);
  assert.match(dialog.panel.className, /\bpopup-visible\b/);
  assert.equal(
    doc.activeElement,
    doc.find(node => typeof node.className === "string" && node.className.includes("release-notes-dialog-close"))
  );
  assert.ok(doc.find(node => node.className === "release-notes-dialog-empty"));

  const openButton = doc.find(
    node => typeof node.className === "string" && node.className.includes("release-notes-dialog-open")
  );
  const closeButton = doc.find(
    node => typeof node.className === "string" && node.className.includes("release-notes-dialog-close")
  );
  assert.ok(openButton);
  assert.ok(closeButton);
  assert.match(openButton.className, /\bpopup-btn-primary\b/);
  assert.match(closeButton.className, /\bpopup-btn-secondary\b/);
  openButton.dispatch("click");
  assert.deepEqual(openedUrls, ["https://example.com/releases/v0.1.2"]);

  let prevented = false;
  doc.dispatch("keydown", {
    key: "Escape",
    preventDefault() {
      prevented = true;
    }
  });

  assert.equal(prevented, true);
  assert.equal(dialog.overlay.parentNode, null);
  assert.equal(doc.activeElement, trigger);
});

test("openReleaseNotesDialog closes when clicking the dimmed overlay", () => {
  const doc = createFakeDocument();
  const dialog = openReleaseNotesDialog({
    title: "Baluffo v0.1.2",
    markdown: "### Fixed\n- Modal works",
    documentTarget: doc,
    windowTarget: doc.defaultView,
  });

  assert.ok(dialog);
  dialog.overlay.dispatch("click", { target: dialog.overlay });
  assert.equal(dialog.overlay.parentNode, null);
});
