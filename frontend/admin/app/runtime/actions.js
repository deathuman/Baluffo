export function createBridgeCaller({
  setBridgeOnline,
  setBridgeOffline
}) {
  return function callBridge(request) {
    return request()
      .then(data => {
        setBridgeOnline();
        return data;
      })
      .catch(error => {
        setBridgeOffline();
        throw error;
      });
  };
}
