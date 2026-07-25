# Transient 401 Self-Resolution Pattern

**First observed:** 2026-06-18 (finch:scan)

## Pattern

A cron job with `provider=null` (uses default provider) hits a transient HTTP 401 from the upstream. The job shows `status=error` with `consecutive_failures=0` at one scan, but by the next scan the job has already re-run successfully (`last_status=ok`, `last_error=null`, `consecutive_failures=0`).

## Key Observations

- The 401 is NOT from a broken fallback provider (no OVH/kepler reference in the error).
- It's a generic "Authentication failed with upstream provider" from the default openrouter provider.
- The job self-resolves on its next scheduled run without any intervention.
- This is distinct from the OVH fallback routing pattern (which produces 403 errors referencing `kepler.ai.cloud.ovh.net`).

## Classification

- **Tier 2** (non-fatal, monitor only)
- **First occurrence + cf=0 + self-resolved** = transient upstream auth blip
- Do NOT escalate. Do NOT attempt fix. Just note in journal and move on.

## Diagnostic Check

When you see a new 401 error on a null-provider job:
1. Check `last_error` for provider references (if it mentions OVH/kepler → it's the fallback routing pattern, not this one)
2. Check if the job's `next_run_at` is on schedule (proving the scheduler is healthy)
3. Check if `consecutive_failures=0` (first occurrence = transient until proven otherwise)
4. On next scan, verify the job re-ran successfully

## Example (finch:scan, 2026-06-18)

```
15:30 scan: status=error, last_error="HTTP 401 Authentication failed", cf=0
16:10 run:  status=ok, last_error=null, cf=0
23:40 scan: confirmed resolved, no action needed
```