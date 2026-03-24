# 1. OBJECTIVE

Fix remaining 25 ESLint errors, prioritizing test file issues first.

# 2. CURRENT STATE

- **Ruff**: ✅ Clean (commit 9a92b20)
- **ESLint**: ❌ 25 errors remaining

# 3. FIX ORDER (Recommended)

## Priority 1: Test File Errors (highest impact)
Fix test files first - these are actual errors, not warnings:
- Duplicate keys in test fixtures
- Missing globals in test files

## Priority 2: Root-level JS Files
Fix any remaining issues in root JS files

## Priority 3: Frontend Files
Fix any remaining issues in frontend code

# 4. IMPLEMENTATION STEPS

## Step 1: Run ESLint to Get Error List
```bash
npx eslint . 2>&1 | head -50
```

## Step 2: Fix Test File Errors First
Common fixes:
- Remove duplicate keys in test fixtures
- Add missing test globals (if any)

## Step 3: Fix Remaining Issues
- Fix root-level JS file issues
- Fix frontend file issues

## Step 4: Verify
```bash
npx eslint .  # Should show 0 errors
```

# 5. VALIDATION CHECKLIST

- [ ] Get 25 ESLint error list
- [ ] Fix test file errors (priority)
- [ ] Fix remaining errors
- [ ] Verify: npx eslint . returns 0 errors
