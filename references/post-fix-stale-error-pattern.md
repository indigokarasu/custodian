# Post-Fix Stale Error Pattern

## Pattern

After applying a Tier 1 auto-fix (e.g., removing broken providers from config.yaml), the affected cron jobs may continue to show `last_error` containing the old error message for hours or days — even though the fix is correct and the jobs are no longer actively failing.

## Why It Happens

Cron jobs run on schedule (e.g., midnight). If the fix is applied at 09:20, the jobs already errored at 00:00 (pre-fix run). The `last_error` field retains the old error until the job runs again. Meanwhile, `consecutive_failures` resets to 0 on the next successful run, but `last_error` may persist in the jobs.json until the scheduler overwrites it.

## Classification Rules

When scanning after a fix has been applied:

1. **Check `consecutive_failures`**: If `consecutive_failures == 0` (or has decreased from the pre-fix count), the job is NOT actively failing. The `last_error` is stale.
2. **Check `last_status`**: If `last_status == ok`, the job is healthy regardless of `last_error` content.
3. **Check timing**: If the error timestamp predates the fix, the error is stale.
4. **Do NOT re-escalate**: A job with stale `last_error` + `consecutive_failures=0` + `last_status=ok` is resolved. Do not create a new issue or re-escalate.

## Example (2026-06-18)

**Fix applied:** 09:20 — removed broken `ovhcloud`/`llm7` providers from profile config.yaml.

**Affected jobs at 10:10 scan (1.5h after fix):**
- `genie:update`: `last_error: "403 from kepler.ai.cloud.ovh.net"`, `consecutive_failures: 0`, `last_status: error`
- `soul:sync`: `last_error: "403 from kepler.ai.cloud.ovh.net"`, `consecutive_failures: 0`, `last_status: error`
- `dispatch-email-15min`: `last_error: "403 from kepler.ai.cloud.ovh.net"`, `consecutive_failures: 0`, `last_status: ok`

**Correct classification:** All three are stale errors from the midnight run (pre-fix). `consecutive_failures=0` confirms they are not actively failing. The issue `oc_provider_ovh_403_auth-20260618` is marked `resolved` with a note explaining the stale errors.

**Journal approach:** Write an observation journal confirming the fix is holding (no new errors since fix time), list the stale errors as "confirmed non-active". Then apply the pause/resume reset so `last_status: error` doesn't persist (see below).

## Active Reset Step (Required, Not Optional)

When stale errors are confirmed post-fix and the affected jobs have `last_status: error` + `consecutive_failures: 0` or `None`:

```bash
hermes cron pause <job_id>
hermes cron resume <job_id>
```

This resets the scheduler's internal state. The `last_error` field will still show the old error until the next successful run overwrites it, but the scheduler will recalculate `next_run_at` from scratch and the job will no longer be stuck in a phantom error state.

**Do NOT skip this step** — without it, `last_status: error` persists in jobs.json indefinitely, causing false-positive error counts in every subsequent custodian scan and potentially triggering re-escalation.

**Confirmed pattern (2026-06-18):** genie:update and soul:sync both had `last_status: error` + `consecutive_failures: None` at 17:30 (8h after config fix at 09:20). Pause/resume at 17:30 cleared the scheduler state. Both jobs scheduled for next midnight run.

## Distinction from Genuine Failures

| Signal | Stale Error | Active Failure |
|--------|-------------|----------------|
| `consecutive_failures` | 0 (or decreasing) | >0 (or increasing) |
| `last_status` | `ok` | `error` |
| Error timestamp | Predates fix | Post-fix |
| Same error on jobs that should be unaffected | Yes (all jobs that ran pre-fix) | No (only affected jobs) |

## Long-Term Fix

The `last_error` field should ideally be cleared when `consecutive_failures` resets to 0. Until the Hermes scheduler implements this, custodian must treat `last_error` as historical evidence, not live state — always cross-reference with `consecutive_failures` and `last_status`.
