/**
 * @fileoverview Shared small UI/browser helpers used by more than one page slice.
 */

function getFileExtension(name) {
  const idx = String(name || "").lastIndexOf(".");
  if (idx === -1) return "";
  return String(name).slice(idx + 1).toLowerCase();
}

/**
 * Returns the inline SVG tick used by saved rows and completed phase steps.
 * @returns {string}
 */
export function renderCheckIcon() {
  return `
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M9 16.17 4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z" fill="currentColor"/>
    </svg>
  `;
}

/**
 * Returns the inline SVG plus used by the unsaved job-row save button.
 * @returns {string}
 */
export function renderPlusIcon() {
  // A typed "+" is a text glyph: the line box is centred in the round button,
  // but the ink rides the font's math axis, which measured 1.5px below the
  // 1.95rem circle's centre. This path's ink box is 5-19 on both axes, so the
  // mark is centred by construction, and its 2.5-unit bars keep the weight the
  // bold text glyph had.
  return `
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M10.75 5h2.5v5.75h5.75v2.5h-5.75v5.75h-2.5v-5.75H5v-2.5h5.75z" fill="currentColor"/>
    </svg>
  `;
}

/**
 * Returns the inline SVG ring used by the job-row save button while a save
 * request is in flight.
 * @returns {string}
 */
export function renderPendingIcon() {
  // A typed "…" was the last of the button's three states still drawn by the
  // font, and it rode the same math axis the typed "+" did. The ring is a
  // circle on the 24x24 centre, so it is concentric with the round button at
  // every size; the 38.9/15.1 dash (72% of the 2π·8.6 circumference) leaves the
  // quarter gap that reads as motion once CSS rotates it.
  return `
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <circle cx="12" cy="12" r="8.6" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-dasharray="38.9 15.1"/>
    </svg>
  `;
}

/**
 * Returns the inline SVG used by the "check availability now" buttons.
 * @returns {string}
 */
export function renderAvailabilityCheckIcon() {
  // The art is drawn around (13.4, 14) rather than the 24x24 centre, so in a
  // 1rem button it read as a sag to the lower right and its bottom stroke sat
  // on the viewBox edge. The group shifts the mark back onto the centre.
  return `
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <g transform="translate(-1.4 -2.25)">
        <path d="M20 11a8 8 0 1 0 1.4 4.6" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
        <path d="M20 5v6h-6" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
      </g>
    </svg>
  `;
}

/**
 * True when an attachment row looks like a PNG/JPEG by MIME type or file name.
 * @param {{ type?: string, name?: string }} attachment
 * @returns {boolean}
 */
export function isImageAttachment(attachment) {
  const type = String(attachment?.type || "").toLowerCase();
  if (type === "image/png" || type === "image/jpeg") return true;
  const ext = getFileExtension(attachment?.name || "");
  return ext === "png" || ext === "jpg" || ext === "jpeg";
}

/**
 * Writes status text through a caller-supplied setter, ignoring missing parts.
 * @param {Function} setText
 * @param {*} element
 * @param {*} text
 */
export function setStatusText(setText, element, text) {
  if (setText && element) setText(element, text);
}

/**
 * Allows a Node timer to be garbage collected, then returns it unchanged.
 * @template T
 * @param {T} timer
 * @returns {T}
 */
export function maybeUnrefTimer(timer) {
  timer?.unref?.();
  return timer;
}

/**
 * Builds a capture-phase keydown handler that turns Escape into a dismissal.
 * @param {Function} cleanup
 * @returns {(event: KeyboardEvent) => void}
 */
export function escapeKeyDismissHandler(cleanup) {
  return event => {
    if (event.key !== "Escape") return;
    event.preventDefault();
    cleanup();
  };
}

/**
 * Focuses an element, preferring a non-scrolling focus when supported.
 * @param {*} element
 */
export function focusWithPreventScroll(element) {
  try {
    element.focus({ preventScroll: true });
  } catch {
    element.focus();
  }
}

/**
 * Returns focus to the element that was active before a dialog opened.
 * @param {Document} doc
 * @param {*} previousActiveElement
 */
function restorePreviousFocus(doc, previousActiveElement) {
  if (previousActiveElement && doc.contains(previousActiveElement)) {
    focusWithPreventScroll(previousActiveElement);
  }
}

/**
 * Builds a one-shot dialog teardown: detaches the key handler, removes the
 * overlay, restores focus, then reports the result exactly once.
 * @param {{ doc: Document, overlay: *, previousActiveElement: *, getKeyHandler: Function, onDone: Function }} options
 * @returns {(result?: *) => void}
 */
export function createModalCleanup({ doc, overlay, previousActiveElement, getKeyHandler, onDone }) {
  let finished = false;
  return function cleanup(result) {
    if (finished) return;
    finished = true;
    doc.removeEventListener("keydown", getKeyHandler(), true);
    overlay.remove();
    restorePreviousFocus(doc, previousActiveElement);
    onDone(result);
  };
}
