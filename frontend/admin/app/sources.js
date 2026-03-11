export function isValidSourceFilter(value) {
  return ["all", "error", "excluded", "zero", "healthy"].includes(String(value || "").trim().toLowerCase());
}

export function normalizeSourceFilter(value) {
  const raw = String(value || "").trim().toLowerCase();
  return isValidSourceFilter(raw) ? raw : "all";
}

export function setSourceFilterValue(value, {
  normalizeSourceFilter,
  writeSourceFilter,
  sourceFilterKey,
  buttons
}) {
  const next = normalizeSourceFilter(value);
  writeSourceFilter(sourceFilterKey, next);
  buttons.forEach(btn => {
    const key = String(btn.dataset.sourceFilter || "").toLowerCase();
    btn.classList.toggle("active", key === next);
  });
  return next;
}
