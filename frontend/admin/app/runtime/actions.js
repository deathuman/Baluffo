export function createBridgeCaller({
  setBridgeOnline,
  setBridgeOffline
}) {
  // Reachability is a claim about the bridge, so a caller has to opt in to
  // making it. Diagnostic reads (a slow /ops/storage-health, say) can fail while
  // every real route answers, and reporting that failure as the bridge being
  // offline is what flipped the whole Admin page into the reconnect state.
  return function callBridge(request, { affectsReachability = true } = {}) {
    return request()
      .then(data => {
        setBridgeOnline();
        return data;
      })
      .catch(error => {
        if (affectsReachability) setBridgeOffline();
        throw error;
      });
  };
}
