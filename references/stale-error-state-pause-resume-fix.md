# Stale Error State: Pause/Resume Fix Pattern

## Pattern

A cron job shows `status=error` with `consecutive_failures=None` (literal null) or `consecutive_failures=0`, and `last_error` references a provider/path that no longer exists in the current configuration.

## Root Cause

The Hermes scheduler doesn't always update `consecutive_failures` or `last_status` when an underlying issue is resolved externally (e.g., a broken fallback provider is removed from config.yaml by a prior escalation run). The job's scheduler state remains stuck on the old error.

## Diagnosis Steps

1. **Check `last_error` content**: Look for provider references (e.g., `kepler.ai.cloud.ovh.net`, specific provider names)
2. **Verify current config.yaml**: `grep -i <provider> <hermes-root>/profiles/<profile>/config.yaml` — if the provider is no longer listed, the error is stale
3. **Check `next_run_at`**: If the job is still being scheduled (next_run_at is future), the scheduler is running it but stuck on the old error state
4. **Check `consecutive_failures`**: `None` (null) means the scheduler never updated the counter; `0` with `status=error` means it reset but didn't clear the status

## Fix

```bash
hermes cron pause <job_id>
hermes cron resume <job_id>
```

This forces the scheduler to recalculate internal state from jobs.json. On resume, `next_run_at` will be recalculated and `status` will reset.

## Verification Before Fix

**Always verify the underlying cause is gone before resetting:**
- If `last_error` references a provider still in config.yaml → error is ACTIVE, not stale. Do NOT pause/resume; fix the provider instead.
- If `last_error` references a provider that was removed → error is STALE. Safe to pause/resume.

## Example (2026-06-18)

`genie:update` and `soul:sync` both had `status=error` with `consecutive_failures=None` and `last_error` referencing `kepler.ai.cloud.ovh.net` (OVH provider). Investigation showed:
1. OVH providers were already removed from config.yaml by prior escalation run (2026-06-18-0925)
2. Both jobs had `next_run_at` in the future (still being scheduled)
3. Both had `provider=openrouter` and `model=openrouter/owl-alpha` explicitly set

Fix: `hermes cron pause de59a77614e9 && hermes cron resume de59a77614e9` (same for soul:sync). Both resumed with clean state and next_run_at recalculated.
