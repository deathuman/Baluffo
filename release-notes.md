## [0.2.146] - 2026-09-03
### Changed

- The Jobs and Admin footer now shows the running app version in container mode too (previously desktop-only): container pages hydrate the same-origin `/app/ready` payload, while desktop keeps reading `/app/update-status`. The version visible in the Umbrel app comes from the running container itself, which makes update verification unambiguous.

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
