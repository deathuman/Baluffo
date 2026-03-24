# 1. OBJECTIVE

Provide a lint report and fix remaining issues in phases.

# 2. CONTEXT SUMMARY

## ESLint Config Status

The `eslint.config.js` has already been updated with extensive browser and test globals:

**Browser globals defined** (60+ including):
- window, document, localStorage, sessionStorage, indexedDB
- fetch, URL, URLSearchParams, FormData
- navigator, history, location, alert
- Blob, FileReader, TextEncoder, TextDecoder
- atob, btoa, requestAnimationFrame, performance

**Test globals defined**:
- describe, it, test, expect, before, beforeEach, after, afterEach

## Ruff Config Status

`ruff.toml` selects: E, F, I, B, UP (errors, flakes, isort, bugs, pyupgrade)

# 3. PHASED EXECUTION

## Phase 1: Ruff Run + Autofix
```bash
pip install ruff
ruff check . --fix
# Review diff - expect ~3200+ UP035/UP006 changes
ruff check .  # See remaining
```

## Phase 2: ESLint Run
```bash
npx eslint .  # Should show remaining issues
```

## Phase 3: Fix Remaining
- B008 issues (function calls in default args - real code quality)
- Any remaining ESLint no-undef issues

# 4. EXPECTED REMAINING ISSUES

After running the commands above, report will show:

**Ruff**:
- B008: Function calls in default arguments (real issues)
- Any non-fixable style issues

**ESLint**:
- Any remaining no-undef for missing globals
- No-unused-vars warnings

# 5. VALIDATION CHECKLIST

- [ ] Run ruff check . --fix
- [ ] Report ruff remaining issues
- [ ] Run npx eslint .
- [ ] Report eslint remaining issues
- [ ] Fix manually
