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

export function setStatusText(setText, element, text) {
  // Matches the shared helper contract but keeps this module slice-local.
  if (setText && element) setText(element, text);
}
