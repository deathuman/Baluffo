## [0.3.034] - 2026-11-04

### Fixed

- The Jobs page now opens the full feed directly and shows a loading state while it loads. The
  last saved startup snapshot is used only if the full feed cannot be loaded, so a new session
  no longer briefly shows a partial list.
- The Admin action center now explains failed-source counts by separating sources that kept zero
  jobs from sources that need browser fallback.
- Source sync now retries GitHub rate-limit and server errors instead of failing the push on the
  first transient response.
- Workable board detection no longer mistakes a site's machine-readable help file for its account
  name.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.033:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
