# Scheduler State Lag vs Actual Execution Failure

**Observed:** 2026-07-07 light scan — multiple enabled jobs showed `next_run_at` in the past (overdue) but `last_run_at` recent (<1 hour ago).

## Pattern

| Job | next_run_at | last_run_at | Status |
|-----|-------------|-------------|--------|
| Gateway health monitor | 3 min ago | 17:37:58 | ok |
| weave-enrichment-health-check | 1 min ago | 15:16:58 | ok |
| thread-renamer:active | 1 min ago | 15:36:55 | ok |
| ...and 13 more | ... | ... | ok |

All 16 "overdue" jobs had `last_status=ok` and recent `last_run_at`. The scheduler had not yet advanced `next_run_at` to the next cycle, but the jobs *had executed successfully*.

## Root Cause

Hermes cron scheduler state update lag: `next_run_at` is recalculated after job completion, but there's a window where the job has run, `last_run_at` is updated, but `next_run_at` still shows the previous scheduled time. This is especially visible on frequent schedules (every 10 min, every minute).

## Diagnosis Checklist

Before flagging a job as "not running" / "stale":

1. **Check `last_run_at` recency** — if < 2× schedule interval, job likely ran
2. **Check `last_status`** — if `ok`, execution succeeded
3. **Check `consecutive_failures`** — if 0 or None, no failure streak
4. **Verify via output file** — `{profile}/cron/output/{job_id}/{timestamp}.md` is ground truth
5. **Consider schedule frequency** — high-frequency jobs (≤10 min) show this lag routinely

## False Positive Triggers

- Light scan "jobs not running" check (Step 7 in checklist)
- Manual inspection of `jobs.json` without cross-referencing `last_run_at`
- Timezone-aware cron expressions (daylight-hour windows) — see `references/timezone-schedule-window-false-positive.md`

## Correct Classification

- **Scheduler state lag**: `last_status=ok` + recent `last_run_at` + `next_run_at` slightly past → **not an error**, do not escalate
- **Actual execution failure**: `last_status=error` OR `last_run_at` genuinely stale (>2× interval) + no output file → **investigate**

## Recommendation

Update light scan Step 7 to require: `last_run_at` older than `2 * schedule_interval` AND `last_status != ok` before flagging as "not running." Current check triggers on `next_run_at < now` alone, which produces false positives on healthy high-frequency jobs.