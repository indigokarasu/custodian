# MCP Server Reconnect-Loop Escalation Pitfall

## Pattern
An **external HTTP MCP server** (no local PID; registered in `config.yaml` `mcp_servers`) can sit in a perpetual reconnect loop that a prior scan dismissed as "info-only transient." This is the **hail** case (2026-07-14): `hail` (https://mcp.hail.so, 18 telephony/email/SMS tools) was logged as "keepalive 404, info-only, 2x" at 06:14Z, but by 17:03Z it had 58 reconnect lines, a real `connection lost (attempt 1-3/5)` burst at 09:28Z, `Session termination failed: 404`, and **no successful "registered N tools" line since 2026-07-13 17:44 UTC (>23h non-recovery)**.

## Why the prior "info-only" call is wrong
Reconnect lines are not self-evidently transient. Distinguish:
- **Benign blink**: a few `keepalive failed ... reconnecting` lines, then `registered N tool(s)` reappears within minutes. → info-only, no action.
- **Real outage**: `connection lost (attempt N/5)` sequences, `Session termination failed: 404/...`, and the gap since the last `registered N tool(s)` line grows past the normal reconnect window (hours). → escalate (Tier 3, user-gated external OAuth/credential). A stale journal note saying "info-only" does NOT suppress re-escalation when the loop has worsened.

## Detection recipe (run in terminal)
```bash
# last successful re-registration (the recovery signal)
<<<<<<< Updated upstream
grep "hail" <hermes-home>/logs/agent.log | grep -i "registered"

# reconnect volume + first/last occurrence today
grep "hail" <hermes-home>/logs/agent.log | grep "2026-07-14" | wc -l
grep "hail" <hermes-home>/logs/agent.log | grep "2026-07-14" | head -1
grep "hail" <hermes-home>/logs/agent.log | grep "2026-07-14" | tail -1

# real connection-loss attempts (not just keepalive)
grep "hail" <hermes-home>/logs/agent.log | grep "connection lost (attempt"
=======
grep "hail" ~/.hermes/logs/agent.log | grep -i "registered"

# reconnect volume + first/last occurrence today
grep "hail" ~/.hermes/logs/agent.log | grep "2026-07-14" | wc -l
grep "hail" ~/.hermes/logs/agent.log | grep "2026-07-14" | head -1
grep "hail" ~/.hermes/logs/agent.log | grep "2026-07-14" | tail -1

# real connection-loss attempts (not just keepalive)
grep "hail" ~/.hermes/logs/agent.log | grep "connection lost (attempt"
>>>>>>> Stashed changes
```
**Decision rule:** if `registered N tool(s)` is absent for longer than the normal reconnect window AND reconnect lines show `connection lost (attempt N/5)` or `Session termination failed: 404`, escalate even if a prior journal said "info-only."

## Cron-reference / pause rule
An external MCP outage does not block cron jobs (it is interactive-session-only). Verify with `grep -il "<name>" jobs.json` → no match means not cron-referenced → **no job pause**. Still write an `escalation_needed: true` issue for Mentor because interactive tool access is degraded. (Contrast with local MCP processes: those have a PID you can check liveness on — see the "MCP server PIDs running but connection failing" gotcha.)

## Scanner false-positive: `find_missed_user_gated_jobs.py`
`find_missed_user_gated_jobs.py` only checks whether a job appears in any open issue's `jobs_paused` list. Under the **no-pause policy** for provider outages, jobs stay `enabled` + `jobs_paused=[]` even when they ARE enrolled in an open issue's `affected_job_ids`. Result: the script reports them as MISSED and recommends re-enrolling/pausing — a false positive.
**Mitigation:** before acting on a MISSED classification, cross-check the job ID against `affected_job_ids` / `affected_components` of all open issues (not just `jobs_paused`). If present, it is already tracked; do not re-pause or re-enroll.
Confirmed 2026-07-14: 3 jobs flagged MISSED (haiku:content-review, art:engagement, EHCS Monthly Refill Form) were already in `oc_openrouter_402_credits_exhausted_20260712T040120`'s `affected_job_ids`; the script's recommended_issue id (`...20260706`) was a stale/merged id.