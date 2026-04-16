# Contributing to Baluffo

## Quick start for contributors

1. **Start here:** Read [`docs/AI_ASSISTANT_GUIDE.md`](docs/AI_ASSISTANT_GUIDE.md) - the primary entry point for all code work
2. **Run setup:** `npm install && npm run setup:hooks`
3. **Make changes:** Create a branch, edit code, add tests
4. **Verify:** Run the smallest relevant check for your change:
   - `npm run test:py` - Python/backend tests
   - `npm run test:unit` - Frontend unit tests
   - `npm run verify` - Full verification for risky or broad changes
   - `npm run perf:py:timing` - Perf timing lane for slow-test visibility
   - `npm run perf:discovery:benchmark` / `npm run perf:startup:cold` - Discovery or packaged-startup perf checks when you touch those paths
5. **Lint:** `npm run lint:precommit:changed` before committing or pushing

## Process notes

- Update docs when commands, contracts, or routing change
- Add tests for new features in `tests/`
- Keep PRs focused and describe your changes clearly
- Keep perf artifacts in repo-local paths such as `.tmp/` and `_out/`; avoid new workflows that depend on `%LOCALAPPDATA%\\Temp`

## Docs reference

- [README.md](README.md) - product overview
- [docs/AI_ASSISTANT_GUIDE.md](docs/AI_ASSISTANT_GUIDE.md) - AI coder entry point
- [docs/testing.md](docs/testing.md) - test layout and run commands
- [docs/LOCAL_SETUP.md](docs/LOCAL_SETUP.md) - setup/runtime commands
