## [0.3.066] - 2026-11-18

### Notes

- Release preflight now runs the JavaScript dead-code scan, so an unused export fails the local preflight instead of passing it and then blocking the `main` or release-tag push.
- This release changes repository tooling only. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility are unchanged, and no app behavior, job feed, saved job, or note is affected.
