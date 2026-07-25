# monitor:journals no-op read-only verification

When `find_missed_user_gated_jobs.py` buckets `monitor:journals` (no_agent job,
script `monitor_journals.py`, `last_error: "Script exited with code 1"`, `cf=None`)
as **UNKNOWN**, do NOT blindly trust a prior loop's "no-op" label and do NOT run the
script to find out — running it manually can **double-enqueue** a work item if a
journal appeared in the last minute. Verify read-only instead.

## Exit-code semantics (from monitor_journals.py)
- `sys.exit(1)` — no new journals since last checkpoint (the **by-design no-op**,
  `oc_cron_no_agent_exit_1_noop`, Tier 2, leave running). No stderr.
- `sys.exit(0)` — new journals found → enqueues to monitor queue. (Would mean the
  job is NOT a no-op; investigate why it errored instead of enqueuing.)
- `sys.exit(2)` — exception, prints `error: ...` to stderr (real failure).

## Read-only verification (no state mutation)
1. Read `STATE_FILE = <hermes-home>/commons/data/monitor_state/journal_ingest_state.json`
   → `state["latest_mtime"]` (a unix epoch float).
2. Compute the actual latest journal mtime under
   `JOURNALS_DIR = <hermes-home>/profiles/indigo/commons/journals`
   (`rglob("*.json")`, take `max(f.stat().st_mtime)`).
3. Compare:
   - `state.latest_mtime >= actual_latest_mtime` → script hits `if latest_mtime <= last_mtime: sys.exit(1)`
     (line 47) → **confirmed no-op**. Leave running.
   - `actual_latest_mtime > state.latest_mtime` → script would enqueue + exit 0 →
     **NOT a no-op**; the job errored despite new journals. Inspect why (exception path,
     queue write failure) and treat as ACTIVE.

### One-liner (UTC datetime compare, safe)

**CONCURRENT-SIBLING CONTAMINATION PITFALL (confirmed 2026-07-17):** the naive
recipe below can FALSELY return `ACTIVE-new-journals` during a `custodian:light`
run that coincides with other agents writing journals. The bug: `actual_latest_mtime`
is sampled at wall-clock *now*, but OTHER cron jobs (e.g. `mentor:light`, a
sibling `custodian:light`) write `commons/journals/**/*.json` continuously. If one
lands even 1 second after `monitor:journals` ran, the comparison sees a "newer"
journal than the checkpoint and wrongly concludes the monitor missed work. In the
2026-07-17 case, `ocas-mentor/mentor-light-...005538Z.json` had mtime
`00:55:38Z` — exactly 1s after the job's own `last_run_at` (`00:55:37Z`) —
producing a false `ACTIVE` verdict that a manual mtime-window check overturned.

**Corrected recipe:** compare the checkpoint only against journals that EXISTED at
the job's own run time. Pass the job's `last_run_at` (from jobs.json) and filter
`commons/journals/**` to `st_mtime <= job_run_epoch` BEFORE taking the max.

```python
from pathlib import Path
import json, datetime
J = Path("<hermes-home>/profiles/indigo/commons/journals")
S = Path("<hermes-home>/commons/data/monitor_state/journal_ingest_state.json")
last = json.loads(S.read_text()).get("latest_mtime",0.0) if S.exists() else 0.0
# Job's own run time (from jobs.json last_run_at). Journals written AFTER this
# are from OTHER agents and must NOT count as work the monitor missed.
job_run_at = datetime.datetime.fromisoformat("2026-07-17T00:55:37-07:00")
job_run_epoch = job_run_at.timestamp()
relevant = [f for f in J.rglob("*.json")
             if f.is_file() and f.stat().st_mtime <= job_run_epoch]
latest = max((f.stat().st_mtime for f in relevant), default=0.0)
print("noop" if latest <= last else "ACTIVE-new-journals")
```

If you lack the job's exact `last_run_at`, subtract a small tolerance (e.g. 5s)
so a sibling journal written milliseconds later is excluded — but prefer the real
run time. After applying the filter, if the verdict flips to `noop`, the original
`ACTIVE` was a false positive from sibling write contention, NOT a genuine miss.

## Why this matters
The escalation-execution-loop requires every UNKNOWN job to be fully classified
(an unresolved UNKNOWN is a silent monitoring gap). `monitor:journals` is a
recurring UNKNOWN in steady-state (it exits 1 whenever no new journals exist,
which is most of the time). The state-file comparison resolves it deterministically
without side effects. Confirmed in production 2026-07-09: `state.latest_mtime`
(2026-07-09T00:24:45Z) exactly equalled the latest journal mtime → no-op, job left
running.

## General principle
For ANY no_agent monitor that exits 1, prefer reading its state/checkpoint file and
comparing against the actual resource (latest journal mtime, latest queue entry,
last successful probe time) over executing the monitor. Execution can mutate shared
state (enqueue, advance checkpoint) and produce false side effects in a probe context.
