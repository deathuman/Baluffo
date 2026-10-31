## [0.3.029] - 2026-10-31

### Added

- **Baluffo can be installed from its Umbrel community app store.** Add the Baluffo GitHub repository in Umbrel, then install the app; it uses the published `0.3.029` image and keeps app data in Umbrel's persistent storage. Store-only metadata changes do not trigger normal container builds.

### Security

- **The Python dependency audit is clear.** Updated Twisted to 26.10.0 and pyasn1 to 0.6.5 to address the reported vulnerabilities.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.028:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
