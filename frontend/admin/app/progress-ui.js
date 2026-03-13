function clampRatio(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(1, numeric));
}

export function applyAdminTaskProgress(rootEl, barEl, labelEl, view = {}) {
  if (!rootEl || !barEl || !labelEl) return;
  const active = Boolean(view?.active);
  const determinate = active && Boolean(view?.determinate);
  const ratio = clampRatio(view?.ratio);

  rootEl.classList.toggle("hidden", !active);
  rootEl.classList.toggle("indeterminate", active && !determinate);
  rootEl.classList.toggle("determinate", determinate);
  rootEl.classList.toggle("complete", active && ratio >= 1);

  if (!active) {
    labelEl.textContent = "";
    barEl.style.width = "0%";
    rootEl.setAttribute("aria-hidden", "true");
    rootEl.removeAttribute("aria-valuenow");
    rootEl.removeAttribute("aria-valuetext");
    return;
  }

  rootEl.setAttribute("aria-hidden", "false");
  labelEl.textContent = String(view?.label || "");
  if (determinate) {
    const percent = Math.round(ratio * 100);
    barEl.style.width = `${percent}%`;
    rootEl.setAttribute("aria-valuenow", String(percent));
    rootEl.setAttribute("aria-valuetext", String(view?.label || `${percent}%`));
    return;
  }

  barEl.style.width = "36%";
  rootEl.removeAttribute("aria-valuenow");
  rootEl.setAttribute("aria-valuetext", String(view?.label || "In progress"));
}
