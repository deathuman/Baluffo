# Repo Guardrails

Always-loaded rules only. Keep detailed workflow in the owning docs.

## Hard Stops

- Do not use destructive Git or file operations unless the user explicitly asks and the target is understood.
- Do not submit changes with `--no-verify`, under any circumstance, on any git command (`commit`, `push`, `merge`, `rebase`, `cherry-pick`, `am`). This is an absolute rule with no exceptions: not to land an amend, not to fix a message, not to get past a gate that seems wrong. A bypass is undetectable afterwards — the fixer hooks are idempotent, so a bypassed tree is byte-identical to a clean one — which means the only real control is CI re-running the same gate on push.
  - When a gate blocks a commit, the gate is telling you the change does not belong in that commit. Fix the cause or split the commit; never silence the gate.
  - Frequent cause worth recognising: editing a path that counts as *shipped container code* (`build-container.yml` `paths-ignore` omits `scripts/`, so a `scripts/` edit republishes the image and trips `container_version_policy`). The correct response is to move dev-only tooling under `tools/`, or to put the change in its own commit — not `--no-verify`, and not a decorative `Release-tag:` line declaring a release nobody intends to ship.
- Do not add Python or Node dependencies without explicit user approval.
- Repo source, tests, docs, and `AGENTS.md` are canonical; external memory is continuity only.
- Never store secrets, tokens, credentials, private keys, or sensitive data in repo docs or memory.
- Before diagnosing a runtime, console, port, or URL error, identify the process that actually emitted it (for example `Get-NetTCPConnection -LocalPort <port>` plus its command line) and confirm the failing URL is served by the code you intend to change. Do not fix a serving path the error never came from.
- Before reporting a tool, capability, or feature as broken or unavailable, check the ground truth directly (the file, the store, the response body) rather than a status string or summary message.
- Do not conclude a board is dead from one endpoint's 404. Probe the **board root** as well as the adapter endpoint, and run a **known-good control** first: a wrong endpoint 404s on healthy rows too (BambooHR `/api/listed_jobs`, Teamtailor `embed`). A control that fails means the probe is wrong, not the board. Prefer certifi-anchored TLS; bare `urllib` fails on hosts the pipeline fetches fine.
- Do not infer that a source yielded nothing from `lastStatus: excluded` or `lastKeptCount: 0`. In `static_listing_flow._handle_skip_and_revalidation` `excluded` means *not fetched this run* — cache `skip_fresh`/`cooldown_skip`, HTTP 304, or `structured_migration_promoted`; corroborating tells are `lastDurationMs: 0` and a `lastFingerprint` equal to the SHA-1 of the empty string. Only a real fetch settles what a source collects: `python src/jobs_fetcher.py --only-sources static_source::<id> --ignore-circuit-breaker --force-refresh-all --output-dir <dir>`. **Omitting `--output-dir` writes stub state into live `data/`** — always isolate.
- Do not treat "no active row and at least one pending row" as a defect. That is the normal state for a parked source (repeated zero jobs, fetch failure, unpromoted candidate). Bucket by `pendingReason` first; only `registry_conflict_*_auto_demote` means reconciliation emptied a board. `tools/repo_health/source_registry_preflight.py` reports both buckets, and its counts are advisory upper bounds, not verdicts.
- Before any bulk registry or state mutation: print the plan, assert its size against an expected value, and require an explicit apply flag. Match rows by **host**, not by studio label — one board is registered as both `Lost Boys Interactive` and `Lost Boys Interactive (Embracer Group)`, and a label-keyed test calls a covered board stranded. Back up to `_out/` and read back after writing.

## Code Boundaries

- Prefer leaf modules and direct config/data reads; do not import composition-root modules from narrow helpers, build scripts, or packaging code.
- Do not expand root-injection or root monkeypatch seams; existing seams are compatibility-only.
- Treat bridge/route signature changes as compatibility work: check route call sites, frontend payload builders, task-start, busy-state, and log-polling behavior together.
- Treat packaging, installer, release, and tag work as high risk; verify the release-critical path and never move/recreate release tags unless explicitly asked.
- Preserve public job text, locations, and persisted/user-facing data contracts when changing normalization, adapters, or report payloads.
- Validate dead-code or boundary-cleanup analyzer findings against actual imports. Before manual dead-code hunting, check the pre-push Vulture hook and `whitelist.py`.
- Keep `src/ship/desktop_app/_linux.py` and `_windows.py` helpers in sync. When touching desktop_app internals, test both `npm run test:py:linux` and `npm run test:py:extended`.

## Routing

- Start docs discovery at `docs/INDEX.md`; load the smallest authoritative doc set only.
- For AI coding workflow, use `docs/AI_ASSISTANT_GUIDE.md`; load `docs/architecture-ai-map.md` only when file routing or compatibility-surface detail is needed.
- For doc ownership or maintenance changes, follow `docs/DOCS_WORKFLOW.md`.
- Do not load `docs/archive/` by default.

## Memory And Tools

- Serena MCP is required for code-intelligence work; use Basic Memory only for continuity, handoffs, recurring gotchas, current focus, and stale-memory corrections.
- For non-trivial Baluffo tasks, check relevant Basic Memory notes, then validate useful claims against repo source/tests/docs before acting.
- At closeout, update Basic Memory only when the task created durable continuity value. Detailed policy lives in `tools/mcp/BASIC_MEMORY.md`.
- For environment/toolbelt triage, run `python scripts/ai_env_check.py --smoke`; run `python scripts/toolbelt_check.py --install` only when missing tools matter. Toolbelt tools are conveniences, not build/CI requirements.
- Avoid broad repo packers or context generators by default; use targeted search, structured filters, and symbol tools first.

## Testing

- The repo `.venv` is a Linux/WSL venv whose `bin/python` symlink is broken on Windows; run Python tests with the global `python -m pytest`, not `.venv/bin/python`. Verification commands, fixture layout, and test routing live in `docs/testing.md`.
- Test runs must never leave discovery audit artifacts (`gameprog-*`/`gamesmap-*`/`*-discovery-audit.json`) in repo `data/`; if they do, an unpinned caller ran the real stages — see `docs/testing.md` (Discovery audit artifact hygiene).
