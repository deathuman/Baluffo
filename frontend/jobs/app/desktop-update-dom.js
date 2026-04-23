export function toggleHidden(element, hidden) {
  if (!element?.classList?.toggle) return;
  element.classList.toggle("hidden", Boolean(hidden));
}

export function setText(element, value) {
  if (element) {
    element.textContent = String(value || "");
  }
}

export function setDisabled(element, disabled) {
  if (!element) return;
  element.disabled = Boolean(disabled);
  element.setAttribute?.("aria-disabled", disabled ? "true" : "false");
}
