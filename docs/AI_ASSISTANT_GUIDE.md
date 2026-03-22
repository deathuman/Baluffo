# AI Assistant Guide — Baluffo Repository

> **For AI coding assistants:** This document is your primary reference for understanding, navigating, and contributing to the Baluffo project. Start here before making any code changes.

---

## Quick Reference

| Category | Command/Info |
|----------|--------------|
| **Type** | Desktop app (Windows) + Browser UI |
| **Tech Stack** | Python (backend) + Vanilla JS/HTML/CSS (frontend) |
| **Package Manager** | npm |
| **Start Bridge** | `npm run dev:bridge` |
| **Run Pipeline** | `npm run dev:pipeline` |
| **Test** | `npm run test:py` |
| **Build** | `npm run build` |

## Commands

- `npm run dev:bridge` — Start Admin Bridge locally
- `npm run dev:pipeline` — Execute core Job Pipeline
- `npm run build` — Full Baluffo Ship bundle build
- `npm run test:py` — Python pytest suite

## Guidelines

For detailed guidance, see:
- [Directory Structure](agent/structure.md)
- [Entry Points](agent/entry-points.md)
- [Development Setup](agent/setup.md)
- [Code Conventions](agent/code-conventions.md)
- [Configuration](agent/config.md)
- [Contributing](agent/contributing.md)

## Existing Documentation

| Document | Purpose |
|----------|---------|
| [architecture-ai-map.md](architecture-ai-map.md) | Detailed architecture |
| [DATA_CONTRACT.md](DATA_CONTRACT.md) | Data contracts |
| [admin-bridge-api.md](admin-bridge-api.md) | API reference |
| [testing.md](testing.md) | Testing guide |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Common issues |
| [AGENTS.md](../AGENTS.md) | AI agent guardrails |

---

*Last updated: 2026-03-22*
