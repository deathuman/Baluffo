export async function fetchJobsFetchReportJson(jobsFetchReportUrl) {
  try {
    const response = await fetch(`${jobsFetchReportUrl}?t=${Date.now()}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.json();
  } catch {
    return null;
  }
}

export async function getBridge(adminBridgeBase, path) {
  const response = await fetch(`${adminBridgeBase}${path}?t=${Date.now()}`, {
    method: "GET",
    cache: "no-store"
  });
  if (!response.ok) {
    throw new Error(`Bridge GET ${path} failed with HTTP ${response.status}`);
  }
  return await response.json();
}

export async function postBridge(adminBridgeBase, path, payload) {
  const response = await fetch(`${adminBridgeBase}${path}`, {
    method: "POST",
    cache: "no-store",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {})
  });
  if (!response.ok) {
    throw new Error(`Bridge POST ${path} failed with HTTP ${response.status}`);
  }
  return await response.json();
}
