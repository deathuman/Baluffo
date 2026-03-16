import { setStatusText as setStatusTextFromShared } from "../../../shared/ui/index.js";

export function toLocalTime(value) {
  try {
    return value.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    });
  } catch {
    return "--:--:--";
  }
}

export const setStatusText = setStatusTextFromShared;
