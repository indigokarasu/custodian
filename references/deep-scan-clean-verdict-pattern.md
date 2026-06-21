# Deep Scan Clean Verdict Pattern

**Confirmed:** 2026-06-19 deep scan

## When All Error Jobs Are Transient (cf=None)

A deep scan can conclude "all clean, no action needed" when every error job meets ALL of these criteria:

1. `consecutive_failures` is `None` (literal null) or `0` — first occurrence only
2. `last_error` is a generic transient message (429 rate limit, 401 auth blip, interpreter futures, response truncated)
3. `next_run_at` is on schedule (scheduler is healthy, job will re-run)
4. No matching open issue in `issues.jsonl` with `escalation_needed: true`

When all error jobs match this pattern, the scan does NOT need to:
- Check issues.jsonl (no open issues if no persistent errors)
- Apply any fixes
- Write escalation journals
- Rebuild activity model (only needed when schedule optimization is triggered)

**Verdict**: Write observation journal with `not_activity_reason` explaining the transient pattern, return `[SILENT]`.

## Common Transient Error Messages

| Message | Classification |
|---|---|
| `HTTP 429: Rate limited by upstream provider` | Transient rate limit. Self-resolves when limit resets. |
| `Error code: 401 - Authentication failed with upstream provider` | Transient auth blip on null-provider jobs. Self-resolves on next run. |
| `cannot schedule new futures after interpreter shutdown` | Transient executor state. Self-resolves on next run. |
| `Response truncated due to output length limit` | Transient output cap. Next run may succeed. |

## Diagnostic Sequence (30-second scan)

When error jobs appear in jobs.json:

1. Parse all error jobs' `consecutive_failures` values
2. If ALL are `None` or `0` → check `last_error` messages
3. If all messages match known transient patterns → **clean verdict, skip full scan**
4. Only if any job has `consecutive_failures >= 1` → proceed to full fingerprint + escalation check

This shortcut saved ~60 seconds on the 2026-06-19 deep scan. It's safe because:
- A job with `cf=0` has never failed twice
- The scheduler's `next_run_at` proves it will re-run
- If the next run also fails, `cf` will increment and the job will be caught on the next scan

## Pitfall — Don't Skip The Journal

Even when all clean, the recovery contract REQUIRES writing an observation journal with `not_activity_reason`. The correct sequence is: (1) write journal → (2) return `[SILENT]`. Never skip the journal.
