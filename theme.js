(function initThemeModule() {
  const THEME_KEY = "baluffo_theme";
  const THEMES = {
    DARK: "dark",
    LIGHT: "light"
  };

  function getSystemTheme() {
    try {
      return window.matchMedia("(prefers-color-scheme: light)").matches ? THEMES.LIGHT : THEMES.DARK;
    } catch (_) {
      return THEMES.DARK;
    }
  }

  function getStoredTheme() {
    try {
      const value = localStorage.getItem(THEME_KEY);
      if (value === THEMES.DARK || value === THEMES.LIGHT) return value;
    } catch (_) {
      // Ignore storage errors and use system fallback.
    }
    return "";
  }

  function getInitialTheme() {
    return getStoredTheme() || getSystemTheme();
  }

  function applyTheme(theme) {
    const next = theme === THEMES.LIGHT ? THEMES.LIGHT : THEMES.DARK;
    document.documentElement.setAttribute("data-theme", next);
    return next;
  }

  function persistTheme(theme) {
    try {
      localStorage.setItem(THEME_KEY, theme);
    } catch (_) {
      // Ignore storage errors.
    }
  }

  function updateToggleLabels(theme) {
    const toggles = document.querySelectorAll(".theme-toggle-btn");
    toggles.forEach(btn => {
      const onLight = theme === THEMES.LIGHT;
      const current = onLight ? "light" : "dark";
      const next = onLight ? "dark" : "light";
      btn.innerHTML = onLight
        ? '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true" focusable="false"><path fill="currentColor" d="M12 3.75a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0V4.5a.75.75 0 0 1 .75-.75Zm0 14.25a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0v-1.5A.75.75 0 0 1 12 18Zm8.25-6a.75.75 0 0 1-.75.75H18a.75.75 0 0 1 0-1.5h1.5a.75.75 0 0 1 .75.75ZM6 12a.75.75 0 0 1-.75.75h-1.5a.75.75 0 0 1 0-1.5h1.5A.75.75 0 0 1 6 12Zm11.303-5.303a.75.75 0 0 1 0 1.06l-1.06 1.061a.75.75 0 1 1-1.06-1.06l1.06-1.061a.75.75 0 0 1 1.06 0ZM8.818 15.182a.75.75 0 0 1 0 1.06l-1.06 1.061a.75.75 0 1 1-1.06-1.06l1.06-1.061a.75.75 0 0 1 1.06 0Zm8.485 2.121a.75.75 0 0 1-1.06 0l-1.061-1.06a.75.75 0 1 1 1.06-1.06l1.061 1.06a.75.75 0 0 1 0 1.06ZM8.818 8.818a.75.75 0 0 1-1.06 0L6.697 7.757a.75.75 0 0 1 1.06-1.06l1.061 1.06a.75.75 0 0 1 0 1.06ZM12 8.25a3.75 3.75 0 1 1 0 7.5a3.75 3.75 0 0 1 0-7.5Z"/></svg>'
        : '<svg viewBox="0 0 24 24" width="18" height="18" aria-hidden="true" focusable="false"><path fill="currentColor" d="M14.53 3.53a.75.75 0 0 1 .84-.15a9 9 0 1 0 5.24 11.1a.75.75 0 0 1 1.32-.2c.19.24.23.56.11.84A10.5 10.5 0 1 1 13.9 2.96c.25-.04.51.05.63.24Z"/></svg>';
      btn.setAttribute("aria-label", `Current theme: ${current}. Switch to ${next} theme`);
      btn.setAttribute("aria-pressed", onLight ? "true" : "false");
      btn.setAttribute("title", `Switch to ${next} theme`);
    });
  }

  function setTheme(theme, { persist = false } = {}) {
    const applied = applyTheme(theme);
    updateToggleLabels(applied);
    if (persist) persistTheme(applied);
    return applied;
  }

  function toggleTheme() {
    const current = document.documentElement.getAttribute("data-theme") === THEMES.LIGHT ? THEMES.LIGHT : THEMES.DARK;
    const next = current === THEMES.DARK ? THEMES.LIGHT : THEMES.DARK;
    return setTheme(next, { persist: true });
  }

  function bindThemeToggles() {
    document.querySelectorAll(".theme-toggle-btn").forEach(btn => {
      btn.addEventListener("click", () => {
        toggleTheme();
      });
    });
  }

  const initialTheme = getInitialTheme();
  applyTheme(initialTheme);

  document.addEventListener("DOMContentLoaded", () => {
    updateToggleLabels(initialTheme);
    bindThemeToggles();
  });
})();
