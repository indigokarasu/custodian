# Post-Fix Wrapper Script Verification Pattern

## When to Use

After applying a Tier 1 fix that replaces a compound `&&` command in a `no_agent: true` cron job's `script` field with a wrapper script, verify the fix is actually working — not just that the file exists.

## Why

A wrapper script can exist at the correct path and be the correct target of the `script` field, yet still fail to execute (wrong path inside the script, missing dependency, wrong working directory). The stale `last_error` from the old compound command will persist in `jobs.json` until the scheduler runs the job again, which could be hours or days. Proactive verification confirms the fix is real.

## Pattern

```bash
# 1. Run the wrapper script directly
bash <hermes-home>/profiles/<profile>/scripts/<wrapper>.sh 2>&1
echo "EXIT: $?"

# 2. If EXIT=0, the stale error in jobs.json is confirmed resolved
#    Classify as `oc_cron_stale_error_script_mismatch` (Tier 2, surface only)
#    No further action needed — the scheduler will update last_status on next run

# 3. If EXIT≠0, the fix is incomplete — investigate the actual runtime error
```

## Real Example — dispatch:triage-morning/evening (2026-06-25)

Both triage wrapper scripts were verified after confirming they existed:

```bash
$ bash <hermes-home>/profiles/indigo/scripts/triage_morning.sh 2>&1 | tail -5
EXIT: 0

$ bash <hermes-home>/profiles/indigo/scripts/triage_evening.sh 2>&1 | tail -5
EXIT: 0
```

Result: Both scripts ran successfully (exit 0, produced triage output). The `last_error` showing `Script not found: <hermes-home>/profiles/indigo/scripts/triage.py && python3 ...` was confirmed stale — the old compound command no longer matched the current `script` field (`triage_morning.sh` / `triage_evening.sh`).

Classification: `oc_cron_stale_error_script_mismatch` — no fix needed, surface only.

## Integration with Light Scan

During light scan, when you find a `no_agent: true` job with `Script not found` in `last_error` but the `script` field points to a different (non-compound) file:

1. Check if the wrapper script exists at the profile path
2. Run it via `bash` and check exit code
3. If exit 0 → mark as stale error, no action needed
4. If exit ≠ 0 → the wrapper itself has a bug, escalate as active error

This avoids waiting for the next scheduled run to discover the fix didn't take.