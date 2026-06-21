# Thread Renamer Backfill — LLM Hang Pattern

## Fingerprint: `oc_cron_thread_renamer_backfill_hang`

## Pattern

The `thread-renamer:backfill` cron job (`active_thread_renamer.py --backfill`) intermittently hangs on individual sessions during LLM title generation. The `call_llm(timeout=30.0)` does not always abort the HTTP request, causing the script to stall for 2-5+ minutes per hung session. A single hung session blocks all subsequent sessions in the batch.

## Detection

- Job runs longer than 15 minutes for a backfill of 48 sessions
- Log shows same session ID with no progress for 3+ minutes
- Process is still alive (`ps aux | grep active_thread_renamer`) but producing no output

## Severity: Low

- Does not affect system stability
- Hung sessions are retried on the next run
- Only impacts session title cosmetic quality

## Custodian Action

**Do NOT escalate.** This is a known intermittent pattern. If detected during a scan:
1. Note the hung session ID in the scan log
2. Check if the next run completed successfully (sessions were retried)
3. Only escalate if the same session fails 3+ consecutive runs

## Historical Occurrences

- **2026-05-20:** Backfill of 48 sessions. 4 sessions hung (2 min each), 1 hung indefinitely (killed after ~5 min). 31 of 48 sessions processed before kill. Remaining 17 retried on next run.
