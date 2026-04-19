function appendClassName(element, className) {
  if (!element || !className) return;
  const current = String(element.className || "").trim();
  const nextNames = current ? current.split(/\s+/) : [];
  if (nextNames.includes(className)) return;
  nextNames.push(className);
  element.className = nextNames.join(" ");
}

export function presentPopup(overlay, panel, options = {}) {
  if (!overlay || !panel) return;
  const windowTarget = options.windowTarget || overlay?.ownerDocument?.defaultView || globalThis?.window;
  const show = () => {
    appendClassName(overlay, "popup-overlay-visible");
    appendClassName(panel, "popup-visible");
  };
  if (typeof windowTarget?.requestAnimationFrame === "function") {
    windowTarget.requestAnimationFrame(show);
    return;
  }
  setTimeout(show, 0);
}
