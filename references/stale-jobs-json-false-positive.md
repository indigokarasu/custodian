# Stale jobs.json False Positive — Mass Scheduling Gap

## The Trap

When scanning for mass cron scheduling gaps, you check `jobs.json` for overdue `next_run_at` values. If you read the **wrong** `jobs.json`, you'll see 90+ overdue jobs and flag a critical scheduling failure.

**There are TWO jobs.json files:**

| File | Role | Updated by |
|------|------|-----------|
| `<hermes-home>/cron/jobs.json` | **ACTIVE** — the scheduler reads/writes this file | Gateway cron ticker |
| `<hermes-root>/cron/jobs.json` | **STALE COPY** — only updated on gateway restart or manual sync | Occasionally overwritten |

After a gateway restart with `--replace`, the main `<hermes-root>/cron/jobs.json` retains pre-downtime `next_run_at` values. The indigo jobs.json is updated live by the scheduler.

## How to Diagnose Correctly

When detecting mass overdue jobs:

1. **Always read the indigo jobs.json first:**
   ```python
   data = json.loads(open('<hermes-home>/cron/jobs.json').read())
   jobs = data.get('jobs', data)
   ```

2. **Cross-check with `hermes cron list`** — this queries the live scheduler state, not the file.

3. **Compare the two files** — if main shows 90+ overdue but indigo shows 0, the issue is a stale file, not a real scheduling gap.

4. **Verify gateway health** — if the gateway is running and `hermes cron list` shows active jobs with future `next_run_at`, the scheduler is working.

## Worked Example — 2026-06-04 Escalation Runner

Custodian deep scan at 21:15 UTC created issue `mass_cron_scheduling_gap_20260604` based on the main jobs.json showing 92 overdue jobs. The escalation runner checked the indigo jobs.json and found 0 overdue jobs. `hermes cron list` confirmed all jobs active. The issue was a false positive from the stale file.

**Resolution:** Closed the issue immediately — no fix needed.

## Key Rule

> **Never create a mass scheduling gap issue from `<hermes-root>/cron/jobs.json` alone. Always verify against the indigo jobs.json and `hermes cron list`.**
