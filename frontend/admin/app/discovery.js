export function isDiscoveryMobileViewport(width = window.innerWidth) {
  return Number(width) < 900;
}

export function setDiscoveryLogOpen(detailsEl, nextOpen, {
  onSyncStart,
  onSyncEnd,
  schedule = callback => window.setTimeout(callback, 0)
} = {}) {
  if (!detailsEl) return;
  const desired = Boolean(nextOpen);
  if (detailsEl.open === desired) return;
  onSyncStart?.();
  detailsEl.open = desired;
  schedule(() => {
    onSyncEnd?.();
  });
}

export function syncDiscoveryLogDisclosure(detailsEl, {
  isMobileViewport,
  hasLiveDiscovery,
  discoveryLogUserToggled,
  discoveryLogPreferredOpen,
  setDiscoveryLogOpen
}) {
  if (!detailsEl) return;
  if (hasLiveDiscovery) {
    setDiscoveryLogOpen(true);
    return;
  }
  if (discoveryLogUserToggled) {
    setDiscoveryLogOpen(discoveryLogPreferredOpen);
    return;
  }
  setDiscoveryLogOpen(!isMobileViewport());
}
