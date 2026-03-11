export function updateCustomJobWarning(linkEl, warningEl) {
  if (!warningEl) return;
  const hasLink = Boolean(String(linkEl?.value || "").trim());
  warningEl.classList.toggle("hidden", hasLink);
}
