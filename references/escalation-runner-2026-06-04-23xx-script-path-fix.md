# Escalation Runner 2026-06-04 (23:xx UTC) — Script Path Security Model Fix

## Summary

Resolved the script path block issue for 8 cron jobs by identifying the root cause:
Hermes security model validates script paths against `HERMES_HOME/scripts/`, not the
profile-specific scripts directory.

## Root Cause

`cron/scheduler.py` line 943:
```python
scripts_dir = _get_hermes_home() / "scripts"  # → <hermes-home>/scripts/
```

The `path.relative_to()` check at line 956 rejects paths in
`<hermes-home>/profiles/indigo/scripts/` even though the error message
paradoxically names that directory.

## Fix Applied

Updated 8 job `script` fields in `<hermes-home>/profiles/indigo/cron/jobs.json`:
- Changed `<hermes-home>/profiles/indigo/scripts/<name>` → `<hermes-home>/scripts/<name>`
- Verified target files exist and are identical (via `cmp`)
- Verified path validation passes (via `path.relative_to()` test)

## Jobs Fixed

voyage:update, reach:update, imagine:update, spot:update, vibes:update,
multipass:update, vesper:deliver-morning, plaid-transaction-sync

## New Reference File

Created `references/cron-script-path-security-model.md` with full documentation
of this pattern, detection, fix, and verification procedure.

## Key Insight

The previous "fix" (2026-06-03) moved scripts from `<hermes-home>/scripts/` to
`<hermes-home>/profiles/indigo/scripts/` to avoid a security block. But the
security model actually expects `HERMES_HOME/scripts/`. The move made things worse.
Scripts existed in both locations — the fix was to point back to the main directory.
