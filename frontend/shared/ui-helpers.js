/**
 * @fileoverview Shared small UI/browser helpers used by more than one page slice.
 */

function getFileExtension(name) {
  const idx = String(name || "").lastIndexOf(".");
  if (idx === -1) return "";
  return String(name).slice(idx + 1).toLowerCase();
}

/**
 * Returns the inline SVG used by the "check availability now" buttons.
 * @returns {string}
 */
export function renderAvailabilityCheckIcon() {
  return `
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M20 11a8 8 0 1 0 1.4 4.6" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
      <path d="M20 5v6h-6" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
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
