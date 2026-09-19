export function setProgressVisibility(_setText, element, visible) {
  if (!element) return;
  element.classList.toggle("hidden", !visible);
}

export { setStatusText } from "../../../shared/ui-helpers.js";
