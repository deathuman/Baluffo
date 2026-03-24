# 1. OBJECTIVE

Run linters in phases with review between each, fixing issues methodically.

# 2. PHASED APPROACH

## Phase 1: Ruff Autofix + Review
1. Run `ruff check . --fix` to auto-fix safe changes
2. Review the diff (expect ~3200+ changes - mostly UP035/UP006 modernization)
3. Run `ruff check .` to see what remains
4. Focus on: B008 issues (real code quality), non-fixable issues

## Phase 2: Fix ESLint Config
1. Add missing browser globals (window, document, fetch, URL, etc.) to config
2. Add test globals (describe, it, expect, etc.) for test files
3. Run `npx eslint .`
4. Review remaining issues

## Phase 3: Manual Fixes
1. Fix remaining Ruff issues (especially B008)
2. Fix remaining ESLint issues

# 3. EXPECTED ISSUES

## Ruff
- UP035/UP006: Modernization (Dict → dict, etc.) - ~3200+ safe changes
- B008: Function calls in default arguments - often real issues
- Unused imports, import sorting

## ESLint
- no-undef: Missing browser globals (window, document, fetch, URL, etc.)
- Missing test globals (describe, it, expect)

# 4. VALIDATION CHECKLIST

- [ ] Phase 1: ruff check . --fix + review diff
- [ ] Phase 1: ruff check . (see remaining)
- [ ] Phase 2: Fix eslint.config.js with browser/test globals
- [ ] Phase 2: npx eslint . (check remaining)
- [ ] Phase 3: Manual fixes for remaining issues
