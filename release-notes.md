## [0.3.029] - 2026-10-29

### Changed

- A source that has stopped making progress is now given up on after 60 seconds of failed
  attempts, instead of holding the run for up to ten minutes. Runs finish sooner in the slow case,
  and no board that was still producing jobs is affected.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.028:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
