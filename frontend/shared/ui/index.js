export function escapeHtml(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

export function tooltipAttrs(text) {
  const value = String(text || "").trim();
  return value ? ` data-tooltip="${escapeHtml(value)}"` : "";
}

export function setTooltip(el, text) {
  if (!el) return;
  const value = String(text || "").trim();
  el.removeAttribute?.("title");
  if ("title" in el) {
    try {
      el.title = "";
    } catch {
      // Ignore read-only title implementations in tests or unusual hosts.
    }
  }
  if (value) {
    el.setAttribute?.("data-tooltip", value);
    if (el.dataset) el.dataset.tooltip = value;
    return;
  }
  el.removeAttribute?.("data-tooltip");
  if (el.dataset) delete el.dataset.tooltip;
}

export function clearTooltip(el) {
  setTooltip(el, "");
}

export function showToast(message, type = "info", options = {}) {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;

  const messageSpan = document.createElement("span");
  messageSpan.textContent = String(message || "");
  toast.appendChild(messageSpan);

  const hasAction = typeof options?.onAction === "function" && options?.actionLabel;
  if (hasAction) {
    const actionBtn = document.createElement("button");
    actionBtn.type = "button";
    actionBtn.className = "toast-action-btn";
    actionBtn.textContent = String(options.actionLabel);
    actionBtn.addEventListener("click", async () => {
      try {
        await options.onAction();
      } finally {
        toast.classList.remove("visible");
        setTimeout(() => toast.remove(), 220);
      }
    });
    toast.appendChild(actionBtn);
  }

  document.body.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add("visible"));

  const durationMs = Number(options?.durationMs) > 0 ? Number(options.durationMs) : 2600;
  setTimeout(() => {
    toast.classList.remove("visible");
    setTimeout(() => toast.remove(), 220);
  }, durationMs);
}

export function setText(el, text) {
  if (!el) return;
  el.textContent = String(text || "");
}


export function bindUi(el, eventName, handler) {
  if (!el) return;
  el.addEventListener(eventName, handler);
}

export function bindAsyncClick(el, handler) {
  if (!el) return;
  el.addEventListener("click", () => {
    Promise.resolve(handler()).catch(err => {
      console.error("[async click]", err);
    });
  });
}

export function bindHandlersMap(clickHandlers) {
  clickHandlers.forEach((handler, el) => bindUi(el, "click", handler));
}
