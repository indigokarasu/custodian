# Deep Scan Stale Error Verification

**Confirmed 2026-06-26:** During deep scan clean verdict, verified stale errors on `no_agent: true` jobs by comparing error timestamps to fix timestamps.

## Pattern

After applying a Tier 1 fix (e.g., symlink creation) in a light scan, the job's `last_error` in `jobs.json` persists with the pre-fix error until the next successful run. The deep scan must distinguish:

1. **Active error**: Script genuinely missing, no fix applied → apply fix now
2. **Stale error**: Fix already applied (symlink exists, script resolves), error is from a pre-fix run → classify as stale, skip fix

## Verification Steps

For each error job with `last_error` containing "Script not found":

```
1. Extract the script basename from the current `script` field
2. Check if file exists at profile path: <hermes-root>/profiles/<profile>/scripts/<basename>
3. If it exists AND is a symlink → check symlink target resolves
4. Compare error timestamp (parsed from `last_run_at`) to fix timestamp
5. If error predates fix → STALE, do not re-apply
6. If error postdates fix → ACTIVE, re-apply or escalate
```

## Example: brief:email-morning (2026-06-26)

| Field | Value |
|-------|-------|
| Job | `brief:email-morning` |
| `last_error` | "Script not found: <hermes-home>/scripts/email_morning_brief.py" |
| `last_run_at` | 2026-06-26T13:40:02 |
| Symlink created | 2026-06-26 14:02 (by light scan at 21:04 UTC) |
| Error predates fix? | YES → stale |
| Action taken | None — classified as stale error |
| `next_run_at` | 2026-06-27T13:30:00 (will self-verify) |

## Example: brief:email-evening (2026-06-26)

| Field | Value |
|-------|-------|
| Job | `brief:email-evening` |
| `last_error` | "Script not found: <hermes-home>/scripts/email_evening_brief.py" |
| `last_run_at` | 2026-06-26T03:39:17 |
| Symlink created | 2026-06-26 05:02 |
| Error predates fix? | YES → stale |
| Action taken | None — classified as stale error |

## Symlink Timestamp Verification

```bash
# Check symlink creation time
stat <hermes-home>/scripts/email_morning_brief.py
# Look at "Modify" time — if it's AFTER the job's last_run_at, the error is stale

# Or use ls -la with full timestamps
ls -la --time-style=full-iso <hermes-home>/scripts/email_morning_brief.py
```

## Distinction from Other Stale Patterns

| Pattern | Error | Script Field | Fix |
|---------|-------|-------------|-----|
| `oc_cron_stale_error_script_mismatch` | Different path than current script | Points to old config | Fix the script field |
| `oc_cron_script_not_found_transient` | Same path, exists + executable | Correct | Transient race, no fix |
| **This pattern** | Path matches current script, fix applied post-error | Correct (bare basename) | Fix already applied, error stale |

## When to Escalate

- If the file does NOT exist at either path AND no symlink → re-apply fix (Tier 1)
- If the symlink exists but target is broken → repair symlink (Tier 1)
- If error postdates the fix (newer than symlink creation) → escalate (Tier 2/3)

---

## Disabled-Job Stale Error Pattern (Confirmed 2026-06-27)

A distinct stale error path: when another cron job (e.g., `finch:work`) sets a job's `enabled: false`, the disabled job retains its `last_error` from the pre-disable run. This produces `status=error` + `enabled=false` + `consecutive_failures=None`.

### Detection Steps

```
1. Check job's `enabled` field in jobs.json — if false, job is intentionally disabled
2. Identify which job disabled it (check cron jobs with similar names, e.g. finch:work)
3. Cross-reference timestamps: finch:work.last_run_at vs disabled job's last_run_at
4. If disabled job's error predates the disable action → STALE (not active)
```

### Example (2026-06-27)

| Job | State | Error | Error Time | Disabled By | Disable Time |
|-----|-------|-------|------------|-------------|--------------|
| brief:email-morning | enabled=false | "Script not found" | 2026-06-26 13:40 | finch:work | 2026-06-27 13:32 |
| brief:email-evening | enabled=false | "Blocked: script path" | 2026-06-27 05:27 | finch:work | 2026-06-27 13:32 |

Both errors predate the disable action. Classification: `oc_cron_stale_error_disabled_job` — Tier 2, surface only. No fix needed.

### Integration with Deep Scan Clean Verdict

Per the clean verdict shortcut: when all error jobs are transient OR disabled-with-stale-error, skip Steps 3b (RCA), 4 (activity model), 5 (schedule optimization). Include disabled disabled-job classification in the error classification table. The scan remains clean.
