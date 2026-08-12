## [0.2.130] - 2026-08-12

> Desktop rollup: the public desktop line moves from v0.2.119 straight to
> v0.2.130, folding in the container/Umbrel 0.2.120-0.2.129 patch cycle plus
> the jobs-pipeline memory/stability batch below. Shared fixes (pipeline
> memory, browser-fallback pool, lifecycle preservation, fetch transport)
> apply to desktop and container alike.

### Performance

- Jobs pipeline peak-RSS reduction (pi4-tight seat, 1.5 GiB cap): the finalize pass now runs end-to-end at ~916 MiB peak instead of OOM-killing the fetch child. Three stacked peaks were removed: (1) `read_existing_output` streams rows from a `.rows.jsonl.gz` sidecar instead of `json.loads`-ing the 60+ MB blob (560 → 154 MB parse peak); (2) finalize drops the duplicate `to_dict()` snapshot from tombstone reconciliation and skips the lifecycle-state re-read when the on-disk fingerprint is unchanged; (3) `writing_outputs` streams the unified/light/lifecycle-state/tombstone JSON writes one row at a time (the equivalent `json.dumps` paths peaked 355 + 255 + 178 + ~745 MiB at 40k rows / 111k lifecycle entries), frees ~500-700 MiB of dead identity-preparation references after the lifecycle phase (`gc` + `malloc_trim`), and drops the duplicate pydantic re-validation of every tombstone at write time.
- Browser fallback now pools a single Chromium per fetch stage (`BrowserFallbackPool`) instead of launching a fresh browser per call: measured 43 fallback acquisitions on one browser (559 ms startup, 0 relaunches) vs 43 launches in the subset-50 bench. Fresh `BrowserContext` per call preserves session isolation; `BALUFFO_BROWSER_POOL=0` restores the legacy launch-per-call path; crash recovery stays with the existing circuit-breaker cooldown.
- Fixed an O(N·K) alias-index rebuild in carried lifecycle initialization: `_initialize_carried_lifecycle_rows` rebuilt the full ~71k-entry index after every initialized row (~2 s each), stalling `applying_lifecycle` for ~36 min on ~1,000 fresh-identity rows; incremental index updates make it ~12 s end-to-end with identical output.
- Fetch stage bounds the live future set during source execution (windowed submission instead of all 2k+ loaders at once) and adds stall detection with task-state throttling for long-running child stages.
- `BALUFFO_PROFILE_ALLOC=1` gates per-source tracemalloc capture (`run_profiled_alloc`, `scripts/perf_alloc_top.py`) for allocation-profile diagnostics; findings recorded in `docs/plans/jobs-pipeline-memory-reduction-plan.md` (H1: fetch concurrency count, not per-source body size, drives pi4-tight pressure; mw=10 OOMs at 293/500 sources, mw=4 holds peak).
- Fetch response bodies are now capped at `BALUFFO_FETCH_MAX_BYTES` (default 20 MiB, 1 MiB floor) on both transport paths — urllib read and the httpx async stream — instead of fully materializing unbounded pages. Bounds the H2-class amplification measured for the ~37 MiB playsimple-class peak (`httpx/_models.py` 119.7 MiB cumulative in the allocation profile); a truncated page simply parses fewer rows and is retried on the next run.

### Fixed

- `source_skipped` lifecycle preservation no longer accrues availability failures: the skipped-preserve path called `_apply_unverified_availability_entry`, which incremented `consecutiveAvailabilityFailures` and, after `AVAILABILITY_OVERDUE_FAILURE_COUNT=2` + 7 days, marked jobs `verification_overdue` and hid them from the output — even though their sources were simply not run that cycle (cadence, subset filter, or exclusion). A skipped source provides no availability evidence; only failed sources decay, and eligible-missing retirement is unchanged. Previously collapsed re-run outputs (41k → ~1.2k rows) now project 100%.
- Per-stage RSS logging (`[jobs_fetcher] INFO rssMiB=... phase_enter/exit ...`) in finalize phases plus a `finalizeInputs` size line aid future bench diagnostics.
- `BrowserFallbackPool` close now cancels lingering asyncio tasks (playwright driver `Connection.run`) before stopping its event loop, removing the `Task was destroyed but it is pending!` teardown warning while staying idempotent and join-bounded.
- Atomic writers in `src/pipeline_io.py` sweep same-target `*.tmp` siblings older than one hour before writing, so SIGKILL-interrupted writes (70 MB leftover in the bench seed, 1.3 MB on the live Umbrel volume) no longer accumulate on disk.
- `read_existing_output` is sidecar-only: the legacy `json.loads` fallback on the 60+ MB feed blob is removed (it existed to cover a missing `.rows.jsonl.gz` but re-introduced the ~3x parse peak it was meant to avoid). A missing sidecar cold-seeds the run; the feed rebuilds from the lifecycle carry (the source of truth). Deleting a sidecar is safe but cold-seeds the next run.

### Changed

- Bench harness `scripts/perf_pipeline_stages.py` gains `--only-sources-file` (env-file staged `BALUFFO_CONTAINER_PIPELINE_ONLY_SOURCES` to avoid the Windows 32k command-line cap), `--fetch-max-workers-env` (`BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS`), `--browser-fallback-max-workers-env` (`BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS`, service-capped at 6), and `--profile-alloc`; the pipeline completion timeout default rises to one hour for full-seed runs. Bench evidence and root-cause write-ups live in `docs/plans/jobs-pipeline-memory-reduction-plan.md` and `docs/plans/browser-fallback-pool-plan.md`.

### Tooling

- `scripts/perf_alloc_top.py` aggregates the per-source tracemalloc JSONL (`<data>/perf-profiles/allocations.jsonl`) by cumulative MiB and per-source peak.

### Notes

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
