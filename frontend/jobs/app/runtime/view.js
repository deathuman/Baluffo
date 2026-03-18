export function setProgressVisibility(_setText, element, visible) {
  if (!element) return;
  element.classList.toggle("hidden", !visible);
}

export function setStatusText(setText, element, text) {
  // Matches the shared helper contract but keeps this module slice-local.
  if (setText && element) setText(element, text);
}
