const DEFERRED_SECTION_SELECTOR = ".admin-page .admin-deferred";

function deferredNodes(documentObject) {
  const nodes = documentObject?.querySelectorAll?.(DEFERRED_SECTION_SELECTOR);
  return nodes ? Array.from(nodes) : [];
}

/**
 * Reveal the sections `admin.html` marks `admin-deferred`.
 *
 * Those sections start below the fold and are `content-visibility: hidden` in
 * admin.css, so the shell's first layout sizes only what is visible. Flipping
 * the class in the frame *after* the first paint keeps that layout off the
 * first frame (190.9 -> 108.2 ms of `Layout` at 4x, one pass either way) while
 * leaving the DOM untouched: every ref cached at boot and every listener bound
 * to a node inside a section keeps working, before and after the reveal.
 *
 * `onReveal` runs once the sections are visible again, so callers can re-sync
 * anything that depends on layout (log scroll positions, for instance, which
 * read 0 while the subtree is hidden).
 */
export function revealDeferredSections({
  documentObject = globalThis.document,
  windowObject = globalThis.window,
  onReveal
} = {}) {
  const nodes = deferredNodes(documentObject);
  if (!nodes.length) return { scheduled: false, reveal: () => {} };

  let revealed = false;
  function reveal() {
    if (revealed) return;
    revealed = true;
    for (const node of nodes) node.classList?.remove("admin-deferred");
    onReveal?.(nodes);
  }

  const requestFrame = windowObject?.requestAnimationFrame;
  if (typeof requestFrame === "function") {
    // Two frames: the first runs before this frame paints, the second after it.
    requestFrame(() => requestFrame(reveal));
  } else {
    windowObject?.setTimeout?.(reveal, 0);
  }
  return { scheduled: true, reveal };
}

export { DEFERRED_SECTION_SELECTOR };
