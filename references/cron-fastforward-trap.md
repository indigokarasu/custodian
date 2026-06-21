# Cron Job "Missed Scheduled Time" Fast-Forward Diagnostic Trap

## The Trap

When scanning for cron job failures, you encounter a job with `last_status=error` and many
log entries like:
```
INFO cron.jobs: Job 'email:check' missed its scheduled time (2026-05-30T15:11, grace=300s).
  Fast-forwarding to next run: 2026-05-30T15:31
```

**Your instinct:** "This job is broken — flag it as an error."

**Reality:** This is NORMAL behavior for `no_agent=True` jobs that execute scripts. The script
runs, produces output (possibly delivered to telegram), and the scheduler marks subsequent
scheduled executions within the same window as "missed" — fast-forwarding to the next slot.

## How to Diagnose Correctly

When you see `last_status=error` + many "missed" entries:

1. **Look up the job ID** from jobs.json by name
2. **Search agent.log** for that job ID with "delivered":
   ```
   grep '<job_id>' agent.log | grep -i delivered
   ```
3. **Count delivered vs missed:**
   - `delivered > 0` → Job IS running. The "missed" entries are just scheduler bookkeeping.
     `last_status=error` may be from earlier consecutive failures that have since resolved.
     → Action: Monitor, do NOT create a new issue.
   - `delivered = 0` AND `started = 0` AND `error_entries = 0` across ALL log history →
     Job has NEVER successfully run. True `oc_cron_job_never_completed` issue.
     → Action: Create Tier 2 issue, escalate.

## Worked Example — 2026-05-30 Scan

| Job | Missed Entries | Delivered Entries | Verdict |
|-----|---------------|-------------------|---------|
| `email:check` (e9147019932f) | 21 | 6 | Running fine, fast-forward is normal |
| `elephas:ingest` (f7ac50c7e09f) | 14 | 0 | NEVER ran — real issue, escalated |

The email:check job had `consecutive_failures=2` from early in the day but recovered.
The elephas:ingest job has zero non-missed entries in all of agent.log history.
