# Documentation Index

> **Navigation guide for Baluffo project documentation.** Use this page to find the right document for your task.

---

## Quick Reference

| Your Goal | Start Here |
|-----------|------------|
| **I need to understand the project** | [`README.md`](../README.md) |
| **I'm an AI agent coding on this repo** | [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) |
| **I need to make a code change** | [`AGENTS.md`](../AGENTS.md) |
| **I want to understand the architecture** | [`architecture-ai-map.md`](architecture-ai-map.md) |
| **I need to debug an issue** | [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) |

---

## All Documentation Files

### Project Overview

| File | Lines | Description |
|------|-------|-------------|
| [`README.md`](../README.md) | 136 | Project overview, quick start, features, developer setup |
| [`AGENTS.md`](../AGENTS.md) | 223 | AI agent guardrails, command cheat sheet, verification matrix |
| [`LICENSE`](../LICENSE) | 21 | MIT License |

### Architecture & Design

| File | Lines | Description |
|------|-------|-------------|
| [`architecture-ai-map.md`](architecture-ai-map.md) | 369 | **Comprehensive architecture guide** — system boundaries, frontend/backend topology, data flow, task-to-files mapping |
| [`LOCAL_SETUP.md`](LOCAL_SETUP.md) | 192 | Local-first mode setup, storage model, backup/restore, source discovery, ship bundle, portable EXE |

### Data & Contracts

| File | Lines | Description |
|------|-------|-------------|
| [`DATA_CONTRACT.md`](DATA_CONTRACT.md) | 171 | Data structures between Python pipeline and JS frontend, CanonicalJob/SavedJob schemas, UI interaction contracts |
| [`fetcher-runtime-contracts.md`](fetcher-runtime-contracts.md) | 85 | Fetcher CLI options, admin presets (default/incremental/retry_failed/force_full/uncapped), runtime files |
| [`game-studios-sheet.md`](game-studios-sheet.md) | 26 | Google Sheet contract for game studios directory |

### API & Integration

| File | Lines | Description |
|------|-------|-------------|
| [`admin-bridge-api.md`](admin-bridge-api.md) | 100 | **API reference** — all Admin Bridge endpoints (GET/POST) for desktop local data, registry, discovery, pipeline, sync, ops |
| [`scraping-pipeline.md`](scraping-pipeline.md) | 62 | Scraping flow overview, where Playwright is used, before/after job count comparison |

### Development & Build

| File | Lines | Description |
|------|-------|-------------|
| [`RELEASE.md`](RELEASE.md) | 222 | **Release process** — ship bundle, portable EXE, versioning policy, build procedures, verification checklist |
| [`testing.md`](testing.md) | 89 | Testing guide — Python pytest, frontend smoke (Playwright), test layout, fixtures |
| [`refactor-charter-template.md`](refactor-charter-template.md) | 76 | Template for planning structural refactors |

### Source Adapters

| File | Lines | Description |
|------|-------|-------------|
| [`adapter-plugin-inventory.md`](adapter-plugin-inventory.md) | 161 | Adapter plugin inventory — source loaders map, static plugins, how to add new sources by family |

### Historical / Archive

| File | Lines | Description |
|------|-------|-------------|
| [`scraping-pipeline-run-notes.md`](scraping-pipeline-run-notes.md) | 147 | Historical run notes from 2026-03-17 — **may be outdated**, consider archiving |

---

## Documentation by Role

### For Developers

1. Start: [`README.md`](../README.md) for overview
2. Setup: [`LOCAL_SETUP.md`](LOCAL_SETUP.md) for local dev environment
3. Reference: [`architecture-ai-map.md`](architecture-ai-map.md) for architecture
4. Testing: [`testing.md`](testing.md) for test execution

### For AI Agents

1. Start: [`AI_ASSISTANT_GUIDE.md`](AI_ASSISTANT_GUIDE.md) — comprehensive guide
2. Guardrails: [`AGENTS.md`](../AGENTS.md) — repo-specific rules
3. Reference: [`admin-bridge-api.md`](admin-bridge-api.md) — API endpoints

### For Release Management

1. Process: [`RELEASE.md`](RELEASE.md) — build and release
2. Versioning: See versioning policy in [`RELEASE.md`](RELEASE.md)
3. Config: [`baluffo.config.json`](../baluffo.config.json)

### For Debugging

1. Start: [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) — common issues
2. Logs: Check `data/` directory for reports
3. Architecture: [`architecture-ai-map.md`](architecture-ai-map.md) for system understanding

---

## Documenting Changes

When adding or modifying documentation:

1. **Update this INDEX.md** — add new files to the appropriate table
2. **Cross-reference** — link related documents (e.g., architecture-ai-map.md → DATA_CONTRACT.md)
3. **Keep AI_ASSISTANT_GUIDE.md in sync** — it's the primary AI entry point
4. **Use consistent formatting** — see style guide below

### Style Guide

- Use tables for lists of files/endpoints/commands
- Use `code` for file names, paths, and commands
- Use **bold** for key terms
- Include line counts for reference
- Add last-updated dates where appropriate

---

## Related Resources

- Source code: [`src/`](../src/) — Python backend
- Frontend: [`frontend/`](../frontend/) — Vanilla JS
- Tests: [`tests/`](../tests/) — Python pytest
- Scripts: [`scripts/`](../scripts/) — Build automation

---

*Last updated: 2026-03-22*