// Shared CSS parsing for the admin stylesheet guards.
//
// Two test files assert against styles/admin.css and both need the same derived
// views of it (comment-stripped source, unguarded rules, @media blocks). Parsing it
// twice would risk the two copies drifting, so it lives here.

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// helpers/ -> unit/ -> frontend/ -> tests/ -> repo root
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "..");

export const adminCss = fs.readFileSync(path.join(repoRoot, "styles", "admin.css"), "utf8");

// Comments are stripped before any structural matching: a comment mentioning a
// selector is not a declaration, and without stripping, a `[^{}]*\{` pattern
// happily bridges prose into the next real rule and reports a phantom hit.
export const css = adminCss.replace(/\/\*[\s\S]*?\*\//g, "");

// Everything outside any @media block.
export const unguarded = css.replace(/@media[^{]*\{[\s\S]*?\n\}\n?/g, "");

// Every @media block, keyed by its condition.
export const mediaBlocks = [...css.matchAll(/@media([^{]*)\{/g)].map((match) => {
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

export { repoRoot };
