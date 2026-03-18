export function setStatusText(setText, element, text) {
  // Matches the shared helper contract but keeps this module slice-local.
  if (setText && element) setText(element, text);
}

/** Set element text content; no-op if element is null/undefined. */
export function setElementText(element, text) {
  if (element) element.textContent = text;
}
