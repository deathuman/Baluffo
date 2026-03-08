export function safeReadLocal(key, fallback = "") {
  try {
    const value = localStorage.getItem(String(key || ""));
    return value == null ? fallback : value;
  } catch {
    return fallback;
  }
}

export function safeWriteLocal(key, value) {
  try {
    localStorage.setItem(String(key || ""), String(value || ""));
    return true;
  } catch {
    return false;
  }
}

export function safeReadSession(key, fallback = "") {
  try {
    const value = sessionStorage.getItem(String(key || ""));
    return value == null ? fallback : value;
  } catch {
    return fallback;
  }
}

export function safeWriteSession(key, value) {
  try {
    sessionStorage.setItem(String(key || ""), String(value || ""));
    return true;
  } catch {
    return false;
  }
}

export function safeReadJsonLocal(key, fallback) {
  try {
    const raw = localStorage.getItem(String(key || ""));
    if (!raw) return fallback;
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

export function safeWriteJsonLocal(key, value) {
  try {
    localStorage.setItem(String(key || ""), JSON.stringify(value));
    return true;
  } catch {
    return false;
  }
}
