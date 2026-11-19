## [0.3.068] - 2026-11-19

### Fixed

- Source sync now prunes the shard files a push replaces. The cleanup step reads the repository's manifest history, and that read discarded any response that was a list, so every push reported the retention window as unavailable and deleted nothing; replaced shards accumulated on the remote indefinitely. The history now arrives intact, so superseded shards are removed while the generations a pull may still be reading are kept.
### Notes

- Release preflight's packaged sync rehearsal now drives the atomic Git commit path a real push uses — branch head, blobs, tree, commit and ref update — instead of only the older single-file writes, so a rehearsal can no longer pass while the shipped push path goes untested.
- This release changes how source sync cleans up replaced shard files. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all stay as they are, and no feed, saved job, note, or synced row changes.
