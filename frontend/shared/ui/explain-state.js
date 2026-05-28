let _overlay = null;
let _panel = null;

function resolveDoc() {
  return typeof document !== "undefined" ? document : null;
}

function getOrCreateOverlay() {
  const doc = resolveDoc();
  if (!doc) return { overlay: null, panel: null };
  if (_overlay && doc.contains(_overlay)) return { overlay: _overlay, panel: _panel };

  _overlay = doc.createElement("div");
  _overlay.className = "explain-state-overlay hidden";
  _overlay.setAttribute("role", "dialog");
  _overlay.setAttribute("aria-modal", "true");
  _overlay.setAttribute("aria-label", "State explanation");

  _panel = doc.createElement("div");
  _panel.className = "explain-state-panel";

  const closeBtn = doc.createElement("button");
  closeBtn.className = "btn explain-state-close-btn";
  closeBtn.setAttribute("aria-label", "Close explanation");
  closeBtn.innerHTML = "&times;";
  closeBtn.addEventListener("click", hideExplain);
  _panel.appendChild(closeBtn);

  const content = doc.createElement("div");
  content.className = "explain-state-content";
  _panel.appendChild(content);

  _overlay.appendChild(_panel);
  doc.body.appendChild(_overlay);

  _overlay.addEventListener("click", function (event) {
    if (event.target === _overlay) hideExplain();
  });

  return { overlay: _overlay, panel: _panel };
}

function hideExplain() {
  const { overlay } = getOrCreateOverlay();
  if (overlay) {
    overlay.classList.add("hidden");
    overlay.classList.remove("explain-state-overlay-visible");
  }
}

function showExplain(title, body, actions) {
  const { overlay, panel } = getOrCreateOverlay();
  if (!overlay || !panel) return;

  const content = panel.querySelector(".explain-state-content");
  if (!content) return;

  let html = `<div class="explain-state-title">${escapeHtml(String(title || ""))}</div>`;
  html += `<div class="explain-state-body">${escapeHtml(String(body || ""))}</div>`;

  if (actions && Array.isArray(actions) && actions.length > 0) {
    html += `<div class="explain-state-actions">`;
    for (const action of actions) {
      html += `<button class="btn clear-filters-btn explain-state-action-btn" data-explain-action="${escapeHtml(String(action?.id || ""))}">${escapeHtml(String(action?.label || ""))}</button>`;
    }
    html += `</div>`;
  }

  content.innerHTML = html;

  overlay.classList.remove("hidden");
  overlay.classList.add("explain-state-overlay-visible");

  content.querySelectorAll("[data-explain-action]").forEach(btn => {
    btn.addEventListener("click", function (event) {
      event.stopPropagation();
      const id = btn.dataset.explainAction;
      const match = (actions || []).find(a => String(a?.id || "") === id);
      if (match && typeof match.onAction === "function") {
        match.onAction();
        hideExplain();
      }
    });
  });
}

function escapeHtml(text) {
  const div = typeof document !== "undefined" ? document.createElement("div") : null;
  if (div) {
    div.textContent = String(text || "");
    return div.innerHTML;
  }
  return String(text || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

export function installExplainStateHandler() {
  if (typeof document === "undefined") return;

  document.addEventListener("click", function handler(event) {
    const target = event.target.closest("[data-explain-state]");
    if (!target) return;
    const title = target.dataset.explainState || "";
    const body = target.dataset.explainBody || "";
    if (!title && !body) return;
    event.preventDefault();

    const actions = [];
    const actionIds = target.dataset.explainActions || "";
    if (actionIds) {
      const ids = actionIds.split(",").map(s => s.trim()).filter(Boolean);
      for (const id of ids) {
        actions.push({
          id,
          label: id.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase()),
          onAction: () => {
            const btn = document.querySelector(`[data-explain-target="${id}"]`);
            if (btn) btn.click();
          }
        });
      }
    }

    showExplain(title, body, actions);
  });

  document.addEventListener("keydown", function escHandler(event) {
    if (event.key === "Escape") hideExplain();
  });
}

export { showExplain, hideExplain };
