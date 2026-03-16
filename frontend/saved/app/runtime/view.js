import { setStatusText as setStatusTextFromShared } from "../../../shared/ui/index.js";

export const setStatusText = setStatusTextFromShared;

/** Set element text content; no-op if element is null/undefined. */
export function setElementText(element, text) {
  if (element) element.textContent = text;
}
