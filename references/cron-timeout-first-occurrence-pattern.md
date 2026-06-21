# Cron Timeout First Occurrence Pattern

## Observation (2026-06-14 Deep Scan)

Job `bones:paper-trade` (ID: 5f88971fb87b) failed at 09:46 with:
- `last_status`: `error`
- `last_error`: `RuntimeError: Upstream idle timeout exceeded`
- `consecutive_failures`: `null` (not tracked / first occurrence)
- Schedule: Daily 09:00

## Key Insight

**`consecutive_failures: null` or `0` on a job with `status: error` indicates a FIRST OCCURRENCE (transient candidate), not a persistent failure.**

This is distinct from the existing "Stale failure counter vs stale error" gotcha:
- **Stale counter**: `status: ok` + `consecutive_failures > 0` + `last_error: null` → counter is stale from previous transient failure that resolved
- **First occurrence**: `status: error` + `consecutive_failures: null/0` + `last_error: present` → genuine error but only happened once so far

## Classification

Matches `oc_cron_timeout` in `known_issues.json` (Tier 2):
- `match_patterns` includes `"idle for.*limit.*s"` — "Upstream idle timeout exceeded" matches
- Tier 2 = "may indicate API issue or oversized prompt" — monitor, don't auto-fix

## Action Protocol

1. **First occurrence** (`consecutive_failures: null/0`): Log as observation, monitor next scheduled run
2. **Recurrence** (`consecutive_failures >= 1`): Escalate to Tier 3, investigate provider/API issues
3. **Never** auto-fix Tier 2 timeout patterns — they require diagnosis

## Related Patterns

- `oc_cron_job_inactivity_timeout` (Tier 2) — structural timeout from long-running tools (session_search on large state.db)
- `oc_http_429_rate_limit` (Tier 2) — rate limit can manifest as timeout
- `oc_provider_error_transient` (Tier 2) — HTTP 502/503 provider errors

## References

- `known_issues.json` → `oc_cron_timeout`
- `critical-pitfalls.md` → Pitfall #46 (cron job inactivity timeout)
- `critical-pitfalls.md` → Pitfall #68 (last_status=error with consecutive_failures=0 means transient)