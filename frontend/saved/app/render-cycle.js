export function computeAnchorScrollDelta(beforeTop, afterTop) {
  const start = Number(beforeTop);
  const end = Number(afterTop);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return 0;
  return end - start;
}
