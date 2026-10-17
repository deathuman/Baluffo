## [0.3.021] - 2026-10-17
> **Desktop rollup.** This is the first desktop package since 0.3.017, so it also carries the
> user-facing work released on the container channel in 0.3.018 through 0.3.020.

### Changed

- **The private Umbrel app-store packaging is retired.** Baluffo still publishes the same-origin
  container image on GHCR, but the private store metadata, the Umbrel Compose deployment files, and
  the release steps that maintained them are gone. A version bump no longer looks for them, and the
  container workflow no longer treats them as a publish trigger.

### Notes

- **First desktop package since 0.3.017.** It also carries the user-facing work from 0.3.018 through
  0.3.020: provider boards are read directly instead of through a guessed careers page, saved-job
  state stays with the session that asked for it, Discovery stops re-asking boards it has already
  ruled out or that never answer, and the runtime database reclaims space instead of growing.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.020:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
