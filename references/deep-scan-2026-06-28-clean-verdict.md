# Deep Scan Clean Verdict — 2026-06-28

Updated clean verdict pattern incorporating disabled stale errors and SOUL.md restart-delay nuance.

## Classification Flow

1. Parse all error jobs from `jobs.json` (dict shape: `{"jobs": [...]}`)
2. For each error job, classify:
   - **Transient**: cf=None/0 + known pattern (provider error, 429, futures shutdown, etc.)
   - **Stale (disabled)**: `enabled: false` + `last_error` predates disable or fix action
   - **Stale (script mismatch)**: `last_error` references different path than current `script` field
   - **Stale (symlink fix)**: `no_agent: true` "Script not found" + symlink created AFTER `last_run_at`
   - **Active**: cf >= 1, or unknown pattern, or error matches current script

## Disabled Stale Error Pattern (`oc_cron_disabled_stale_error`)

When a job has `enabled: false` or `paused_at` set:
- Its `last_status=error` is from BEFORE it was disabled
- The `last_error` may show "Script not found" or "Blocked: script path resolves outside the scripts directory" — these are stale if the script was fixed after the last run
- **Verification**: Compare `last_run_at` to fix timestamp (symlink creation via `stat`). If `last_run_at < fix_timestamp`, error is stale
- **Confirmed 2026-06-28**: `brief:email-evening` showed "Blocked: script path resolves outside the scripts directory" with `enabled: false`, `last_run_at=05:27`, symlink created at `05:33` — correctly classified as stale
- Do NOT count disabled stale errors toward actionable error total
- Do NOT attempt fix — the job is intentionally not running

## SOUL.md Truncation Resolution Verification

After updating `context_file_max_chars` in config.yaml:
1. Confirm config has new limit on BOTH main (`<hermes-home>/config.yaml`) AND profile (`<hermes-home>/profiles/<profile>/config.yaml`)
2. Confirm gateway has restarted since config change (`grep "Starting Hermes Gateway" gateway.log`)
3. Confirm no new truncation errors after restart timestamp
- If all three conditions met → truncation is resolved. Stale log entries from pre-restart runs are noise.
- **Confirmed 2026-06-28**: SOUL.md at 8814 chars, limit set to 12000 on 06-26, gateway restarted 06-27 09:27. Zero truncation errors since restart. Resolved.

## Clean Verdict Journal Template

```json
{
  "run_id": "deep-scan-YYYYMMDD-HHMMSSZ",
  "timestamp": "ISO8601",
  "type": "observation",
  "scan_type": "deep",
  "not_activity_reason": "All N error jobs classified as transient or stale. No active issues requiring fix.",
  "summary": {
    "total_jobs": N,
    "error_jobs": N,
    "transient_errors": N,
    "stale_errors": N,
    "fixes_applied": 0,
    "tier1_fixes": 0,
    "escalations": 0
  },
  "error_classifications": [
    {
      "job": "name",
      "fingerprint": "pattern_name",
      "classification": "transient|stale",
      "consecutive_failures": null,
      "note": "Brief explanation"
    }
  ]
}
```

## Key Principle

A clean scan means the system is healthy, not that the scan missed something. Do not force-fix non-issues. Write journal with `not_activity_reason` and return `[SILENT]`.