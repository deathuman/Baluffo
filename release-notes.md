## [0.2.145] - 2026-09-03
### Changed

- Upgraded Scrapy to 2.17.0 in the container image and packaged runtime, fixing CVE-2026-84366 (S3DownloadHandler sending signed S3 requests over plaintext HTTP; the codebase has no s3:// usage, but the advisory is now resolved at the source instead of allowlisted).
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
