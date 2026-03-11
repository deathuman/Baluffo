export function setProgressVisibility(_setText, element, visible) {
  if (!element) return;
  element.classList.toggle("hidden", !visible);
}

export function setStatusText(setText, element, text) {
  setText(element, text);
}
