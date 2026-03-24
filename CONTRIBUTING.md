# Contributing to Baluffo

Thank you for your interest in contributing to Baluffo!

## Getting Started

1. **Fork** the repository
2. **Clone** your fork: `git clone https://github.com/YOUR_USERNAME/Baluffo.git`
3. **Create a branch** for your changes: `git checkout -b my-feature`

## Development Setup

### Prerequisites
- Node.js 18+
- Python 3.11+
- npm

### Install Dependencies
```bash
npm install
```

### Run Development Server
```bash
# Start the admin bridge (Python backend)
npm run dev:bridge

# In another terminal, serve the frontend
# (Use any static server, e.g., npx serve)
```

### Run Tests
```bash
# All tests
npm run test

# Python tests only
npm run test:py

# Frontend unit tests
npm run test:unit

# Frontend smoke tests
npm run test:smoke
```

## Code Style

- **Python**: Follow PEP 8, use type hints where possible
- **JavaScript**: Use ES modules, avoid default exports
- **Pre-commit hooks**: Run `pre-commit install` to set up automatic formatting

## Pull Request Process

1. Update documentation for any changed functionality
2. Add tests for new features (see `tests/`)
3. Ensure all tests pass before submitting
4. Update the CHANGELOG.md if applicable
5. Submit a PR with a clear description of changes

## Commit Messages

Use clear, descriptive commit messages:
- `fix: resolve login timeout issue`
- `feat: add new filter for job location`
- `docs: update API documentation`

## Reporting Issues

Use GitHub Issues to report bugs or request features. Include:
- Clear description
- Steps to reproduce (for bugs)
- Environment details

## Resources

- [README.md](../README.md) - Project overview
- [AGENTS.md](../AGENTS.md) - AI agent guidelines
- [docs/](../docs/) - Full documentation
- [docs/testing.md](../docs/testing.md) - Testing guide
