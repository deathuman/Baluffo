// Shared CSS parsing for the admin stylesheet guards.
//
// The derived views of styles/admin.css are module-local: `css` and `mediaBlocks`
// back the two exported entry points, and the file read plus `repoRoot` exist only
// to produce them. The sibling CSS guard tests still carry their own copies.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// helpers/ -> unit/ -> frontend/ -> tests/ -> repo root
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "..");

const adminCss = fs.readFileSync(path.join(repoRoot, "styles", "admin.css"), "utf8");

// Comments are stripped before any structural matching: a comment mentioning a
// selector is not a declaration, and without stripping, a `[^{}]*\{` pattern
// happily bridges prose into the next real rule and reports a phantom hit.
export const css = adminCss.replace(/\/\*[\s\S]*?\*\//g, "");

// Every @media block, keyed by its condition.
const mediaBlocks = [...css.matchAll(/@media([^{]*)\{/g)].map((match) => {
  const condition = match[1].trim();
  const start = match.index + match[0].length;
  let depth = 1;
  let index = start;
  while (index < css.length && depth > 0) {
    if (css[index] === "{") depth += 1;
    else if (css[index] === "}") depth -= 1;
    index += 1;
  }
  return { condition, body: css.slice(start, index - 1) };
});

/** The concatenated bodies of every @media block with this exact condition. */
export function mediaBlock(condition) {
  return mediaBlocks
    .filter((block) => block.condition === condition)
    .map((block) => block.body)
    .join("\n");
}
