import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  DEFERRED_SECTION_SELECTOR,
  revealDeferredSections
} from "../../../frontend/admin/app/runtime/deferred-sections.js";

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..");

function createSection(id, { deferred = true } = {}) {
  const classes = new Set(deferred ? ["admin-deferred"] : []);
  return {
    id,
    classes,
    classList: {
      remove(name) {
        classes.delete(name);
      }
    }
  };
}

function createDocument(nodes) {
  return {
    querySelectorAll(selector) {
      return selector === DEFERRED_SECTION_SELECTOR ? nodes : [];
    }
  };
}

function createWindow() {
  const frames = [];
  return {
    frames,
    requestAnimationFrame(callback) {
      frames.push(callback);
      return frames.length;
    }
  };
}

test("nothing is scheduled when no section is deferred", () => {
  const windowObject = createWindow();
  const result = revealDeferredSections({
    documentObject: createDocument([]),
    windowObject
  });
  assert.equal(result.scheduled, false);
  assert.equal(windowObject.frames.length, 0);
});

test("the reveal waits for the frame after the first paint, not the first frame", () => {
  // The shell's first layout is the thing being protected: revealing in the
  // first rAF would put the sections back before that frame paints.
  const nodes = [createSection("admin-discovery-section"), createSection("admin-sync-section")];
  const windowObject = createWindow();
  const revealed = [];
  revealDeferredSections({
    documentObject: createDocument(nodes),
    windowObject,
    onReveal: sections => revealed.push(sections)
  });
  assert.equal(windowObject.frames.length, 1);
  windowObject.frames.shift()();
  assert.equal(windowObject.frames.length, 1, "the second frame must be requested, not run");
  assert.ok(nodes.every(node => node.classes.has("admin-deferred")), "still deferred before the second frame");

  windowObject.frames.shift()();
  assert.ok(nodes.every(node => !node.classes.has("admin-deferred")), "revealed on the second frame");
  assert.deepEqual(revealed, [nodes], "onReveal sees the revealed nodes");
});

test("revealing twice is a no-op", () => {
  const nodes = [createSection("admin-discovery-section")];
  const windowObject = createWindow();
  let calls = 0;
  const { reveal } = revealDeferredSections({
    documentObject: createDocument(nodes),
    windowObject,
    onReveal: () => {
      calls += 1;
    }
  });
  reveal();
  reveal();
  assert.equal(calls, 1);
});

test("falls back to a task when requestAnimationFrame is unavailable", () => {
  const nodes = [createSection("admin-sync-section")];
  const timeouts = [];
  revealDeferredSections({
    documentObject: createDocument(nodes),
    windowObject: { setTimeout: (callback, delay) => timeouts.push({ callback, delay }) }
  });
  assert.equal(timeouts.length, 1);
  timeouts[0].callback();
  assert.ok(!nodes[0].classes.has("admin-deferred"));
});

test("admin.html defers exactly the sections that start below the fold", () => {
  const html = fs.readFileSync(path.join(repoRoot, "admin.html"), "utf8");
  const deferred = [...html.matchAll(/<section id="([^"]+)"[^>]*class="[^"]*admin-deferred/g)].map(
    match => match[1]
  );
  // Ops starts inside the first viewport and must stay in the first layout.
  assert.deepEqual(deferred, ["admin-discovery-section", "admin-sync-section"]);
  const css = fs.readFileSync(path.join(repoRoot, "styles", "admin.css"), "utf8");
  assert.match(css, /\.admin-page \.admin-deferred \{\s*content-visibility: hidden;/);
});
