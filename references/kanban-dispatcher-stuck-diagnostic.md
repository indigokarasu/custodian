# Kanban Dispatcher "Stuck" Diagnostic

**Confirmed:** 2026-06-23 light scan

## Pattern

Gateway log shows repeated warnings:
```
kanban dispatcher stuck: ready queue non-empty for N consecutive ticks but 0 workers spawned
```

## Root Cause Hierarchy (check in order)

### 1. Workers crashing (most common)
The dispatcher spawns workers (logged as `spawned=2`) but they die immediately. Evidence:
- `hermes kanban show <task>` shows `consecutive_failures >= 1` and `most_recent_outcome=crashed`
- Gateway log shows `kanban dispatcher [default]: spawned=2 ... crashed=1` or `crashed=2`
- Tasks have been "ready" for >30 minutes with no progress

**Cause**: Task code errors (assertion failures, import errors, OOM in worker subprocess). Not a dispatcher bug.

**Action**: Tier 2 — surface only. The kanban auto-decompose and retry mechanism handles this. Do NOT escalate as `oc_kanban_dispatcher_failure`.

### 2. max_in_progress reached (less common)
Workers are running but hit the `max_in_progress=2` or `max_in_progress_per_profile=2` limit. The dispatcher can't spawn more until existing workers finish.

**Evidence**: `hermes kanban list --status in_progress` shows 2 tasks. The "stuck" warning fires because the ready queue is non-empty but the spawn limit is reached.

**Action**: Normal behavior. The dispatcher will spawn workers as in-progress tasks complete. Do NOT escalate.

### 3. No workers needed (rare)
The dispatcher has nothing to dispatch because all ready tasks are blocked by parent dependencies.

**Evidence**: `hermes kanban list --status ready` shows tasks with unresolved parents.

**Action**: Normal behavior. Do NOT escalate.

## Quick Diagnostic (5 seconds)

```bash
# Check if workers are crashing
hermes kanban list --status ready | head -5
hermes kanban show <task_id> 2>/dev/null | grep -E "consecutive_failures|most_recent_outcome"

# Check if max_in_progress is the bottleneck
hermes kanban list --status in_progress | wc -l

# Check gateway log for crash counts
grep "kanban dispatcher.*crashed" <hermes-home>/profiles/indigo/logs/gateway.log | tail -3
```

## Pitfall — Don't Confuse Stuck With Broken

The "stuck" warning is a **symptom**, not a diagnosis. Always check `hermes kanban show` for the actual failure mode before classifying. The dispatcher itself is almost never the problem — it's the workers or the config limits.

### 4. Zombie session lease blocking spawns (2026-06-29)

Workers spawn but immediately die with "Hermes is at the active session
limit (N/N)". The gateway permanently occupies one slot; a zombie
kanban worker (state Z, process still in `/proc` but defunct) holds
another. With `max_concurrent_sessions=2`, this leaves 0 slots for new
workers.

**Evidence**: Worker log shows "active session limit (2/2)".
`cat /proc/<pid>/stat | awk '{print $3}'` returns `Z` for the stale
PID. `active_sessions.json` shows dead PIDs.

**Fix** (2026-06-29): `_pid_alive` in `active_sessions.py` now rejects
zombies (state Z/X/x). `_prune_dead` runs on every lease acquisition.
After the code fix, zombies are cleaned up within one dispatch tick.

**Pre-fix workaround**: manually remove stale entry from
`active_sessions.json`, or restart gateway.

**Do NOT raise `max_concurrent_sessions`** as a fix — causes OOM on
constrained VPS hosts.

### 5. Rate-limit misclassified as protocol_violation (2026-06-29)

Workers hit 429 rate limits on ALL models, exit rc=0 (should be 75),
dispatcher marks as `protocol_violation` (immediate circuit breaker
trip, failure_limit=1), task blocks forever.

**Evidence**: `task_runs` shows outcome "protocol_violation" but worker
log contains "429", "rate limit", or "quota".

**Fix** (2026-06-29): `detect_crashed_workers` now checks worker log
for `_RESPAWN_BLOCKER_RE` patterns before classifying clean_exit as
protocol_violation. If matched → `rate_limited` (requeue, no failure).

**Pre-fix workaround**: manually unblock task and set
`consecutive_failures=0`.

See `kanban-orchestration` skill for full details on both fixes.

---

## Recurring Pattern with Active Sessions (2026-06-22 to present)

Since 2026-06-22, the kanban dispatcher has shown "stuck" warnings recurring night after night (typically 6-26 consecutive ticks). Investigation on 2026-06-24 confirmed:

- Workers are crashing (`crashed=1` in dispatcher logs) but not because of a venv/PATH issue
- <operator> is actively working on the kanban board during these periods (e.g., ocas-reach migration)
- The crushed workers are part of **interactive task processing**, not cron job failures
- New tasks continue to be spawned and processed (promoted=1, auto_blocked=1 show the system is working)

**Classification**: During active sessions, recurring "stuck" warnings are Tier 2 monitor-only. The dispatcher is healthy — workers crash because of task-specific code issues during development, and the dispatcher correctly waits. Do NOT escalate as `oc_kanban_dispatcher_failure`.

**Rule of thumb**: If <operator> has sent a Telegram message about kanban work in the last 2 hours, the stuck warnings are explained. Verify by checking `gateway.log` for recent inbound messages from the user.