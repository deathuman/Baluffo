# 1. OBJECTIVE

Upgrade GitHub Actions to support Node 24 before June 2026 deprecation.

# 2. CONTEXT SUMMARY

**Deadline**: Node 20 deprecated June 2, 2026, removed Fall 2026

**Current workflow files to check**:
- `.github/workflows/lint.yml`
- `.github/workflows/test.yml`
- `.github/workflows/build-portable-exe.yml`

**Actions to upgrade**:
| Current | New | Notes |
|---------|-----|-------|
| actions/checkout@v4 | actions/checkout@v6 | Node 24, credential changes |
| actions/setup-python@v5 | actions/setup-python@v6 | Node 24 |
| actions/setup-node@v4 | actions/setup-node@v6 | Node 24, npm caching changes |
| actions/cache@v4 | actions/cache@v5 | Node 24, requires runner 2.327.1+ |

# 3. IMPLEMENTATION STEPS

## Step 1: Audit Current Workflow Files
Check all `.github/workflows/*.yml` files for action versions.

## Step 2: Upgrade Action Versions
Update each workflow file:
- checkout@v4 → v6
- setup-python@v5 → v6
- setup-node@v4 → v6
- cache@v4 → v5

## Step 3: Add Node 24 Test Flag
Add to test compatibility before June 2026:
```yaml
env:
  FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true
```

## Step 4: Verify Behavior Changes
Watch for:
- `setup-node@v6` npm auto-caching
- `checkout@v6` credential persistence

# 4. VALIDATION CHECKLIST

- [ ] All workflow files updated
- [ ] Node 24 test flag added
- [ ] Workflows run successfully
- [ ] No breaking behavior changes
