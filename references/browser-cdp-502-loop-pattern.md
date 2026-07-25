# Browser CDP 502 Loop Pattern

## Pattern

A cron job that uses browser tools enters a retry loop where the CDP (Chrome DevTools Protocol) WebSocket connection returns HTTP 502 Bad Gateway. The supervisor retries every ~10 seconds, accumulating dozens to hundreds of failed attempts.

## Observed Instances

### 2026-06-01 — spot:watch-sweep
- **Job:** `spot:watch-sweep` (id: 7f0c31ab7ff1)
- **CDP Supervisor:** eebf04c3-4b4f-4fe9-a163-7da161fde332
- **Error count:** 139+ consecutive 502 errors over ~6 minutes
- **Error messages:** "server rejected WebSocket connection: HTTP 502", "Auto-launch failed: CDP WebSocket connect failed: HTTP error: 502 Bad Gateway"
- **Job last_status:** `ok` — the job eventually completed despite the browser failures
- **Root cause:** Browser process was unhealthy/unresponsive; the job's browser dependency was non-critical enough that it completed via other means

## Classification

- **Tier:** 2 (non-fatal, surface only)
- **Auto-fix:** None
- **Action:** Log and move on. Do NOT escalate unless the job's `last_status` is also `error`.

## Diagnostic Checklist

1. Check `last_status` of the affected job — if `ok`, the browser failures were non-fatal
2. Count 502 errors: `grep -c "CDP supervisor.*502" <hermes-home>/logs/errors.log`
3. Check if the job has `browser` in its prompt or name
4. Verify gateway health: `curl -s http://localhost:8080/health` — if `ok`, the issue is browser-specific

## Related Patterns

- MCP simultaneous failure (mempalace + stealth-browser TaskGroup errors) — see pitfall #9 in `critical-pitfalls.md`
- Both patterns can co-occur after gateway restart