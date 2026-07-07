# Gateway Restart Frequency as Health Signal

## Diagnostic Signal

Gateway restart frequency is a leading indicator of resource pressure and systemd session instability.

## Classification

| Restarts/24h | Loadavg at restart | Classification | Action |
|---|---|---|---|
| 0-2 | <1.0 | Normal | None |
| 3-5 | 1.0-1.5 | Elevated | Monitor restarts, check drain timeout count |
| 6-10 | 1.5-2.0 | High | Investigate: drain timeouts, memory pressure, CLOSE-WAIT accumulation |
| >10 | >2.0 | Critical | Immediate investigation: check for crash loops, OOM kills, socket leaks |

## Drain Timeout as Severity Amplifier

Drain timeout count (grep "drain timed out" gateway.log) correlates with SIGTERM-induced restarts. High drain timeout count means agents were active during shutdown — the gateway is being killed while doing work.

- drain_timeouts < 10/day: Normal (session teardown takes time)
- drain_timeouts 10-30/day: Elevated (agents running long tasks during SIGTERM windows)
- drain_timeouts > 30/day: High (systemd killing gateway during active sessions daily)

## Root Causes (in order of likelihood)

1. **SIGTERM from session/pod lifecycle**: Container or systemd user session terminates, gateway process receives SIGTERM. Confirmed by "under_systemd=yes" in shutdown logs. Frequency increases with container restarts or session switches.

2. **Drain timeout cascade**: Gateway waits 60s for active agents to finish. If agents are in long-running tool calls (300+ sec responses), the drain times out, gateway is force-killed, systemd restarts it.

3. **Memory pressure**: Gateway RSS grows, OOM killer or systemd kills it. Check with `ps aux | grep hermes` at restart time.

4. **Gateway collision**: Multiple gateway processes attempt to bind the same port, one kills the other.

## Correlated Patterns

- Restarts + loadavg > 1.5: Resource pressure from cron workers or concurrent sessions
- Restarts + high drain timeout count: Long-running agent sessions being interrupted
- Restarts + Telegram disconnect: Network issue causing adapter crash (rare)

## Example (2026-06-21)

9 restarts in 24h, drain_timeouts=49 (since logging started), loadavg 1.17-2.03 at restart. 60 SIGTERM total since log start. Pattern: 3 drain-timeout restarts during active agent sessions (the current conversation), 6 SIGTERM from systemd session transitions. All recover cleanly via Restart=on-failure. No action needed but frequency is elevated.

Note: Gateway restarts are NOT inherently failures — the systemd Restart=on-failure service handles them. The concern is the underlying cause (resource pressure, session instability) and whether restarts are interrupting active work.

## Log Extraction Commands

```
# Restart count (today)
grep -c "Starting Hermes Gateway" gateway.log

# Drain timeout count
grep -c "drain timed out" gateway.log

# Loadavg at each restart
grep "Shutdown context" gateway.log | grep -oP "loadavg_1m=\K[0-9.]+"

# Restart type breakdown
grep "Shutdown context" gateway.log | grep -oP "signal=\K\w+" | sort | uniq -c
```
