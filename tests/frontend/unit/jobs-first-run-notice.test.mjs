import test from "node:test";
import assert from "node:assert/strict";

import { openFirstRunJobsNotice } from "../../../frontend/jobs/app/first-run-notice.js";
import { createFakeDocument } from "./helpers/browser-test-helpers.mjs";

function noticeOverlay(doc) {
  return doc.find(node => node?.dataset?.jobsFirstRunNotice === "true");
}

test("openFirstRunJobsNotice renders the first-run jobs popup", () => {
  const doc = createFakeDocument();
  const trigger = doc.createElement("button");
  doc.body.appendChild(trigger);
  trigger.focus();

  openFirstRunJobsNotice({
    title: "Preparing first-run jobs",
    body: "This can take several minutes.",
    primaryLabel: "Got it",
    documentTarget: doc,
    windowTarget: doc.defaultView
  });

  const overlay = noticeOverlay(doc);
  const panel = doc.find(node =>
    typeof node.className === "string"
    && node.className.includes("jobs-first-run-notice")
    && node.attributes?.role === "dialog"
  );
  const button = doc.find(node => typeof node.className === "string" && node.className.includes("local-auth-dialog-submit"));

  assert.ok(overlay);
  assert.ok(panel);
  assert.match(overlay.className, /\bpopup-overlay-visible\b/);
  assert.match(panel.className, /\bpopup-visible\b/);
  assert.equal(panel.attributes.role, "dialog");
  assert.equal(panel.attributes["aria-modal"], "true");
  assert.equal(doc.find(node => node.id === "jobs-first-run-notice-title").textContent, "Preparing first-run jobs");
  assert.match(
    doc.find(node => node.id === "jobs-first-run-notice-body").textContent,
    /several minutes/
  );
  assert.equal(button.textContent, "Got it");
  assert.equal(doc.activeElement, button);

  button.dispatch("click");

  assert.equal(overlay.parentNode, null);
  assert.equal(doc.activeElement, trigger);
});

test("openFirstRunJobsNotice dismisses on Escape and outside click", () => {
  const doc = createFakeDocument();
  openFirstRunJobsNotice({
    body: "Bootstrap is running.",
    documentTarget: doc,
    windowTarget: doc.defaultView
  });
  const firstOverlay = noticeOverlay(doc);
  let prevented = false;

  doc.dispatch("keydown", {
    key: "Escape",
    preventDefault() {
      prevented = true;
    }
  });

  assert.equal(prevented, true);
  assert.equal(firstOverlay.parentNode, null);

  openFirstRunJobsNotice({
    body: "Bootstrap is running.",
    documentTarget: doc,
    windowTarget: doc.defaultView
  });
  const secondOverlay = noticeOverlay(doc);
  secondOverlay.dispatch("click", { target: secondOverlay });

  assert.equal(secondOverlay.parentNode, null);
});
