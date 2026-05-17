import { presentPopup } from "../../shared/ui/popup-presentation.js";

function isFocusableElement(value) {
  return Boolean(value && typeof value === "object" && typeof value.focus === "function");
}

export function openFirstRunJobsNotice({
  documentTarget,
  windowTarget,
  title = "Preparing first-run jobs",
  body = "",
  primaryLabel = "Got it"
} = {}) {
  const doc = documentTarget || (typeof document !== "undefined" ? document : null);
  if (!doc?.body) return null;
  if (
    typeof doc.querySelector === "function"
    && doc.querySelector("[data-jobs-first-run-notice='true']")
  ) {
    return null;
  }

  const win = windowTarget || doc.defaultView || globalThis?.window;
  const previousFocus = doc.activeElement;
  const titleId = "jobs-first-run-notice-title";
  const bodyId = "jobs-first-run-notice-body";

  const overlay = doc.createElement("div");
  overlay.className = "popup-overlay local-auth-dialog-overlay jobs-first-run-notice-overlay";
  overlay.dataset.jobsFirstRunNotice = "true";

  const panel = doc.createElement("div");
  panel.className = "popup local-auth-dialog jobs-first-run-notice";
  panel.setAttribute("role", "dialog");
  panel.setAttribute("aria-modal", "true");
  panel.setAttribute("aria-labelledby", titleId);
  panel.setAttribute("aria-describedby", bodyId);

  const heading = doc.createElement("h2");
  heading.id = titleId;
  heading.className = "local-auth-dialog-title";
  heading.textContent = String(title || "Preparing first-run jobs");

  const description = doc.createElement("p");
  description.id = bodyId;
  description.className = "local-auth-dialog-description";
  description.textContent = String(body || "");

  const actions = doc.createElement("div");
  actions.className = "local-auth-dialog-actions";

  const dismissBtn = doc.createElement("button");
  dismissBtn.type = "button";
  dismissBtn.className = "btn back-btn popup-btn-primary local-auth-dialog-submit";
  dismissBtn.textContent = String(primaryLabel || "Got it");

  let closed = false;
  function cleanup() {
    if (closed) return;
    closed = true;
    doc.removeEventListener("keydown", onKeyDown, true);
    overlay.remove();
    if (isFocusableElement(previousFocus)) {
      try {
        previousFocus.focus({ preventScroll: true });
      } catch {
        previousFocus.focus();
      }
    }
  }

  function onKeyDown(event) {
    if (event.key !== "Escape") return;
    event.preventDefault();
    cleanup();
  }

  dismissBtn.addEventListener("click", cleanup);
  overlay.addEventListener("click", event => {
    if (event.target === overlay) cleanup();
  });
  doc.addEventListener("keydown", onKeyDown, true);

  actions.append(dismissBtn);
  panel.append(heading, description, actions);
  overlay.appendChild(panel);
  doc.body.appendChild(overlay);
  presentPopup(overlay, panel, { windowTarget: win });

  const focusDismiss = () => {
    try {
      dismissBtn.focus({ preventScroll: true });
    } catch {
      dismissBtn.focus();
    }
  };
  if (typeof win?.requestAnimationFrame === "function") {
    win.requestAnimationFrame(focusDismiss);
  } else {
    setTimeout(focusDismiss, 0);
  }

  return cleanup;
}
