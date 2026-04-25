# Contributing to Baluffo

## Quick start for contributors

1. **Start here:** Read [`docs/AI_ASSISTANT_GUIDE.md`](docs/AI_ASSISTANT_GUIDE.md) - the primary entry point for all code work
2. **Run setup:** For AI-assisted repo work, install `uv`, install Serena, and configure Codex CLI or OpenCode using [`tools/mcp/SERENA.md`](tools/mcp/SERENA.md). Then run `npm install && npm run setup:hooks`
3. **Make changes:** Create a branch, edit code, add tests
4. **Verify:** Run the smallest relevant check for your change:
   - `npm run test:py` - Python/backend tests
   - `npm run test:unit` - Frontend unit tests
   - `npm run verify` - Full verification for risky or broad changes
   - `npm run security:python` - Python dependency vulnerability audit after dependency changes
   - `npm run perf:py:timing` - Perf timing lane for slow-test visibility
   - `npm run perf:discovery:benchmark` / `npm run perf:startup:cold` - Discovery or packaged-startup perf checks when you touch those paths
   - For packaged startup architecture and command ownership, use [`docs/startup-probe-architecture.md`](docs/startup-probe-architecture.md)
5. **Refresh docs:** If commands, contracts, routing, edit boundaries, or process guidance changed, update the owning docs in the same change and use [`docs/DOCS_WORKFLOW.md`](docs/DOCS_WORKFLOW.md) for doc ownership and maintenance rules
6. **Lint:** `npm run lint:precommit:changed` before committing or pushing

## Process notes

- Update docs when commands, contracts, or routing change
- Add tests for new features in `tests/`
- Keep PRs focused and describe your changes clearly
- Dependency vulnerability findings fail CI unless an entry in `tools/security/pip-audit-allowlist.json` has an advisory id, package, reason, owner, and unexpired review date
- Keep perf artifacts in repo-local paths such as `.tmp/` and `_out/`; avoid new workflows that depend on `%LOCALAPPDATA%\\Temp`

## Docs reference

- [README.md](README.md) - product overview
- [docs/AI_ASSISTANT_GUIDE.md](docs/AI_ASSISTANT_GUIDE.md) - AI coder entry point
- [docs/DOCS_WORKFLOW.md](docs/DOCS_WORKFLOW.md) - documentation discovery and maintenance workflow
- [docs/testing.md](docs/testing.md) - test layout and run commands
- [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md) - setup/runtime commands
