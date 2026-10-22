import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

// Guards the centring of the hand-drawn inline icons. Every glyph here shares
// the same 24x24 box, so ink whose bounding box centres more than 0.75 units
// off (12, 12) reads as a nudge at the 14-16px sizes the rows and buttons use.
// The measurement is the centre-line of straight-edged paths only: curved art
// (the theme moon, the availability arrow, the saved clock) needs a rasterizer
// and is pinned separately below.

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const SOURCES = [
  "admin.html",
  "saved.html",
  "theme.js",
  "frontend/saved/render.js",
  "frontend/shared/ui-helpers.js"
];
const TOLERANCE = 0.75;

const ARITY = new Map([
  ["M", 2],
  ["L", 2],
  ["H", 1],
  ["V", 1],
  ["C", 6],
  ["S", 4],
  ["Q", 4],
  ["T", 2],
  ["A", 7],
  ["Z", 0]
]);

function pathPoints(d) {
  const tokens = d.match(/[a-zA-Z]|-?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?/g) || [];
  const points = [];
  let index = 0;
  let command = "";
  let x = 0;
  let y = 0;
  let startX = 0;
  let startY = 0;
  while (index < tokens.length) {
    if (/[a-zA-Z]/.test(tokens[index])) {
      command = tokens[index];
      index += 1;
    } else if (command === "M") {
      command = "L";
    } else if (command === "m") {
      command = "l";
    }
    const upper = command.toUpperCase();
    assert.ok(ARITY.has(upper), `unsupported path command '${command}' in ${d}`);
    if (upper === "Z") {
      x = startX;
      y = startY;
      points.push([x, y]);
      command = "";
      continue;
    }
    const count = ARITY.get(upper);
    const args = tokens.slice(index, index + count).map(Number);
    assert.ok(
      args.length === count && args.every(Number.isFinite),
      `truncated '${command}' in ${d}`
    );
    index += count;
    const relative = command === command.toLowerCase();
    if (upper === "H") {
      x = relative ? x + args[0] : args[0];
    } else if (upper === "V") {
      y = relative ? y + args[0] : args[0];
    } else {
      x = relative ? x + args[0] : args[0];
      y = relative ? y + args[1] : args[1];
      if (upper === "M") {
        startX = x;
        startY = y;
      }
    }
    points.push([x, y]);
  }
  return points;
}

// Returns the ink box of a straight-edged 24x24 icon, or null when the block is
// curved art (or a chart) that this check cannot measure.
function straightIconBox(svg) {
  // Only `<path>` art is measurable here; circles, lines and charts are not.
  if (/<(circle|ellipse|rect|line|polyline|polygon|text|use)\b/.test(svg)) return null;
  const viewBox = (svg.match(/viewBox="([^"]+)"/) || ["", "0 0 24 24"])[1].trim().split(/\s+/).map(Number);
  if (viewBox.join(" ") !== "0 0 24 24") return null;
  const paths = [...svg.matchAll(/\sd="([^"]*)"/g)].map(match => match[1]);
  if (!paths.length) return null;
  if (paths.some(d => /[AaCcSsQqTt]/.test(d))) return null;
  const [tx, ty] = (svg.match(/translate\(([-\d.]+)[ ,]([-\d.]+)\)/) || ["", "0", "0"])
    .slice(1)
    .map(Number);
  const box = [Infinity, Infinity, -Infinity, -Infinity];
  for (const d of paths) {
    if (!/^[\dMmLlHhVvZz\s.,-]+$/.test(d)) return null;
    const points = pathPoints(d);
    if (!points.length) return null;
    for (const [px, py] of points) {
      box[0] = Math.min(box[0], px);
      box[1] = Math.min(box[1], py);
      box[2] = Math.max(box[2], px);
      box[3] = Math.max(box[3], py);
    }
  }
  return [box[0] + tx, box[1] + ty, box[2] + tx, box[3] + ty];
}

function iconBlocks(text) {
  return [...text.matchAll(/<svg[\s\S]*?<\/svg>/g)].map(match => match[0]);
}

test("hand-drawn straight-edged icons sit on their viewBox centre", () => {
  const offenders = [];
  let measured = 0;
  for (const file of SOURCES) {
    const text = fs.readFileSync(path.join(ROOT, file), "utf8");
    for (const svg of iconBlocks(text)) {
      const box = straightIconBox(svg);
      if (!box) continue;
      measured += 1;
      const dx = (box[0] + box[2]) / 2 - 12;
      const dy = (box[1] + box[3]) / 2 - 12;
      if (Math.abs(dx) > TOLERANCE || Math.abs(dy) > TOLERANCE) {
        const d = [...svg.matchAll(/\sd="([^"]*)"/g)].map(match => match[1]).join(" | ");
        offenders.push(`${file}: ${d} centres at (${(12 + dx).toFixed(2)}, ${(12 + dy).toFixed(2)})`);
      }
    }
  }
  assert.deepEqual(offenders, []);
  assert.ok(measured >= 6, `expected at least 6 measurable hand-drawn icons, found ${measured}`);
});

test("the theme moon keeps the centring group its curved art needs", () => {
  const theme = fs.readFileSync(path.join(ROOT, "theme.js"), "utf8");
  assert.match(
    theme,
    /<g transform="translate\(0\.34 -1\.24\)"><path fill="currentColor" d="M14\.53 3\.53/
  );
});

test("the saved-row link and notes icons keep their centring groups", () => {
  const render = fs.readFileSync(path.join(ROOT, "frontend/saved/render.js"), "utf8");
  assert.match(render, /<g transform="translate\(-1 1\)">[\s\S]*?d="M14 3h7v7h-2V6\.41/);
  assert.match(render, /<g transform="translate\(-1 0\)">[\s\S]*?d="M7 3h7l4 4v14H7z"/);
});
