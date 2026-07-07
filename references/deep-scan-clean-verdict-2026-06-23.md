# Deep Scan Clean Verdict — 2026-06-23

## Summary

Deep scan 2026-06-23-200400 completed with a **clean verdict**: all 16 error jobs were transient/non-faulty, 0 Tier 1 fixes applied, no open issues.

## Conditions

- 132 total jobs, all `status=pending` (normal post-restart state)
- 16 jobs had `last_error` but ALL were transient patterns:
  - `cannot schedule new futures after interpreter shutdown` (cf=None) — 2 jobs
  - HTTP 429 rate limit (cf=None) — 3 jobs
  - `no_agent` scripts exit 1 for no-op (cf=None/0) — 8 jobs
  - Stale failure counter (cf=1, last_status=ok) — 1 job
  - Stale 401 from null-provider job (cf=None) — 2 jobs
- 0 jobs with `consecutive_failures >= 1` AND `last_status=error`
- 0 error-level entries in gateway.log today
- Config clean (no null sections)
- All issues.jsonl entries resolved

## Decision Path

1. Gateway running (pid 111958, uptime 1h42m) — OK
2. All error jobs have cf=None/0 → transient classification
3. No new fingerprints → no RCA needed
4. No open issues → no escalation needed
5. Config clean → no Tier 1 fixes
6. Result: **Clean verdict** → write observation journal → `[SILENT]`

## Key Insight

A clean scan means the system is healthy, not that the scan missed something. The deep scan early-exit shortcut (skip RCA/activity-model when all errors are transient) saves 45-60 seconds. The remaining time is spent on conformance checks and journal writing. Total scan time: ~30 seconds.

## Error Classification Table

| Pattern | Count | Verdict |
|---|---|---|
| `cannot schedule new futures` | 2 | Transient, auto-resolves on next run |
| HTTP 429 rate limit | 3 | Transient, scheduler doesn't count as consecutive failure |
| no_agent exit 1 noop | 8 | By design — scripts exit 1 when no work to do |
| Stale 401 (null-provider) | 2 | Stale — no 401 in recent gateway log |
| Stale counter (cf=1, ok) | 1 | Non-fatal — counter resets on next successful run |

## State

- state.db: 8.55 GB (Tier 2 monitor, disk 88% — VACUUM not safe)
- Disk: 84G used / 96G total (88%)
- Kanban dispatcher: recovered from stuck state (36 ticks → spawned=2 → operational)
- Telegram: 1 "Message thread not found" (first occurrence, transient)
