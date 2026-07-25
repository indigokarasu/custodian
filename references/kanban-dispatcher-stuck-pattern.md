# Kanban Dispatcher Stuck Pattern

## Fingerprint

`oc_kanban_dispatcher_stuck`

## Detection

In gateway.log:
```
WARNING gateway.run: kanban dispatcher stuck: ready queue non-empty for N consecutive ticks but 0 workers spawned. Check profile health (venv, PATH, credentials) and `hermes kanban` status.
```

**Key indicator**: "ready queue non-empty for N consecutive ticks but 0 workers spawned". The tick count resets when a worker is spawned. If it is growing, the dispatcher is consistently failing to launch workers.

## Severity

| Consecutive Ticks | Severity | Action |
|---|---|---|
| 1-5 | Low | Monitor — may resolve when queue drains |
| 6-15 | Medium | Investigate profile health |
| 16-30 | High | Workers are consistently failing to spawn — check venv, PATH, credentials |
| 30+ | Critical | Kanban system is effectively down — all queued work stalled |

## Diagnostic Procedure

1. Check `hermes kanban` status — is the board accessible? Are there queued items?
2. Check profile health: `which python3`, `echo $PATH`, credential check
3. Check gateway log for worker spawn errors: `grep "kanban.*worker\|kanban.*spawn\|kanban.*error" gateway.log`
4. Check for zombie workers: `grep "kanban dispatcher: reaped.*zombie" gateway.log`

## Common Root Causes

1. **Gateway mass-restart correlation**: After 10+ SIGTERM restarts in 24h, the kanban scheduler may lose track of available workers while jobs queue up.
2. **Profile venv corruption**: If the profile's virtual environment is broken, workers fail immediately on spawn.
3. **Credential expiry**: Kanban workers needing API credentials that expired while the queue was building up.

## Fix Sequence

1. If ticks < 15: monitor, self-resolves when queue drains or gateway stabilizes
2. If ticks >= 15: `hermes kanban` status → check for obvious issues
3. If gateway had mass restarts: the dispatcher typically self-recovers within 30 min of stable uptime
4. If persistent after 30 min of stable gateway: escalate as Tier 2

## Example (2026-06-23)

Gateway received 17 SIGTERM restarts on 2026-06-23 (average interval 36.8 min). Starting from 10:11, kanban dispatcher reported "ready queue non-empty for 6 consecutive ticks but 0 workers spawned". By 11:15, count reached 26 ticks. A zombie worker was reaped at 11:06 (PID 2949454). Correlation with gateway restarts — workers likely lost registration during a restart while the queue accumulated items during the restart gap.

**Classification**: Tier 2 — monitor and surface. Self-resolves when gateway stabilizes and workers re-register.