## [0.3.062] - 2026-11-15

### Changed

- Deleting the retained pre-rebuild database now requires explicit confirmation and a fresh database check. The completed rebuild's generation must still match its counts and hashes, or, if a newer feed has superseded and removed it, the current published feed must match its own stored counts and hashes. A mismatch keeps the rollback copy in place, and Admin refreshes the freed-space summary after a successful delete.

### Notes

- The guarded delete keeps the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved; the rollback copy is removed only after the operator confirms deletion.
