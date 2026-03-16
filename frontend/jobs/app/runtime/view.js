import { setStatusText as setStatusTextFromShared } from "../../../shared/ui/index.js";

export function setProgressVisibility(_setText, element, visible) {
  if (!element) return;
  element.classList.toggle("hidden", !visible);
}

export const setStatusText = setStatusTextFromShared;
