export function createSavedPageState() {
  return {
    noteSaveState: {
      timers: new Map(),
      inFlight: new Map(),
      pendingValues: new Map(),
      lastInteractionAt: 0
    },
    attachmentPreviewUrls: new Map()
  };
}
