## [0.3.031] - 2026-11-02

### Fixed

- A run can no longer sit silently for hours while another part of the app holds the job-feed update lock.
  While it waits, the log names the process holding the lock, and the run now ends with a recorded error
  after 20 minutes instead of waiting indefinitely; the limit can be adjusted in the container environment.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.030:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
