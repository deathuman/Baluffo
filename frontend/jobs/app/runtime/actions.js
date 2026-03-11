export function createJobsBridgeRequest({
  baseUrl,
  timeoutMs,
  request
}) {
  return function callBridge(path, options = {}) {
    return request(baseUrl, path, {
      timeoutMs,
      ...options
    });
  };
}
