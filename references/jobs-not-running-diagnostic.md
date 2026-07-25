# Jobs Not Running — Diagnostic Pattern

## The Pattern

During light scans, you may find that `next_run_at` values are stale (hours or days in the past) for many or all jobs. This is **normal** after gateway downtime — the scheduler's in-memory state diverges from jobs.json on disk (see `references/cron-fastforward-trap.md`).

**However**: stale `next_run_at` does NOT necessarily mean the job IS running. You must also check `last_run_at`.

## Diagnostic Steps

1. **Count how many jobs are simultaneously overdue** — this determines the diagnostic path:
   - **<10% overdue**: Individual job issues. Use the per-job diagnostic below.
   - **10–50% overdue**: Possible partial scheduler issue. Check gateway uptime and restart history.
   - **>85% overdue (MASS EVENT)**: Gateway downtime / scheduler state divergence. Skip per-job triage — escalate the root cause (see Mass Event Procedure below).

2. **For individual jobs, identify jobs where `last_run_at` is older than expected** based on their schedule:
   - `email:check` (every 10 min) with `last_run_at` 3+ days old → **not running**
   - `corvus:deep` (daily) with `last_run_at` 3+ days old → **may have missed a few runs but could be scheduler lag**
   - A job with `schedule.expr: '0 */2 * * *'` (every 2h) and `last_run_at` 60h old → **not running**

3. **Cross-reference with `last_status`**:
   - `status=error` + old `last_run_at` + `consecutive_failures > 0` → Job tried to run and is failing
   - `status=error` + old `last_run_at` + `consecutive_failures=None` → Stale error, job may not have run since the error
   - `status=ok` + old `last_run_at` → Job ran successfully at least once but may have stopped

4. **Check if the job has a script path issue**:
   - Compare traceback paths in `last_error` against `script` field in jobs.json
   - Mismatch = stale error (see `references/known-script-auth-issues.md`)

## Mass Event Procedure (>85% jobs overdue)

When nearly all jobs are simultaneously overdue:

1. **Check gateway uptime**: `ps -o etime <pid>` and `curl -s http://localhost:8080/health`
2. **Check gateway restart history**: `journalctl -u hermes-gateway --since "7 days ago"` or check `gateway.log` for shutdown/restart entries
3. **Check `jobs.json` `updated_at`**: If the timestamp is recent but `next_run_at` values are stale, the scheduler wrote the file but didn't recalculate schedules (known `--replace` restart pattern)
4. **Classify**: This is `oc_cron_stuck_missed` mass event, Tier 2. Don't triage individual jobs — they're all victims of the same root cause.
5. **Escalation**: Write issue with `escalation_needed: true`. Recommend `hermes cron resume --all` or equivalent scheduler reset.
6. **Don't create per-job issues**: Pre-existing error jobs will have stale errors from before the downtime. Creating new issues for them creates noise. Note them as "stale, blocked by scheduling gap" in the scan journal.

## Verdict Table

| last_run_at age | consecutive_failures | % of jobs affected | Verdict | Action |
|---|---|---|---|---|
| Normal for schedule | None/0 | Any | Running fine | none |
| Normal for schedule | > 0 | <10% | Active failure | Triage |
| Older than 2x schedule interval | None | <10% | Job not scheduling | Tier 2 — investigate why scheduler isn't picking it up |
| Older than 2x schedule interval | > 0 | <10% | Job failing AND not re-running | Tier 2 — investigate both the error and the scheduling gap |
| Never (null) | Never (null) | <10% | Job never ran | Tier 2 — `oc_cron_job_never_completed` |
| **>48h old** | **Any** | **>85%** | **Mass event — gateway downtime / scheduler divergence** | **Tier 2 — escalate root cause, skip per-job triage** |

## Worked Example — 2026-06-04 Scan

92 of 105 jobs had stale `next_run_at`. Of those:

- **90 jobs**: `last_run_at` was >48h old → Mass event confirmed
- **Root cause**: Gateway was down ~67h (2026-06-01T19:19 → 2026-06-04T10:34)
- **All 5 HTTP 429 errors**: From before the downtime, stale
- **jobs.json `updated_at`**: Current (2026-06-04T18:00) but `next_run_at` values stale — scheduler wrote file but didn't recalculate
- **Verdict**: Mass `oc_cron_stuck_missed` event. Tier 2 escalation. Scheduler reset needed.

## Worked Example — 2026-06-04 Earlier Scan

91 of 105 jobs had stale `next_run_at` (known post-downtime pattern). Of those:

- **90 jobs**: `last_run_at` was recent, `status=ok` → Running fine in scheduler memory
- **1 job** (`email:check`): `last_run_at=2026-06-01T17:33` (3+ days old), `status=error`, `consecutive_failures=2` → Job genuinely not running. Stale `next_run_at` (2026-06-01T19:01) confirms scheduler lost its slot. Known root cause: OAuth `invalid_grant` prevents the script from running, and the scheduler may have fast-forwarded past it entirely.

## Recovery

If a job is genuinely not running (individual, not mass event):
1. Check if the job is `enabled: false` — if so, it's intentionally paused
2. Check if `script` path exists and resolves correctly
3. Check if the job's `prompt` or `skill` references are valid
4. For `no_agent=True` script jobs: manually trigger with `hermes cron run <id>` to test
5. For prompt-based jobs: check if `consecutive_failures` is auto-incrementing (indicates it IS being tried)

For mass events: `hermes cron resume --all` or equivalent scheduler reset.