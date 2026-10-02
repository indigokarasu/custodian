# Classifying a cron worker's death: re-run the job AND read executions.db

Applies to: `Restart-safe cron worker exited with status N after adopting this execution, without writing a durable terminal state`.

## Why the job row lies

`hermes cron run <id>` does not necessarily clear `jobs.json`'s `last_status`. After a clean re-run the job row can still read `last_status=error` with `failure_streak=1` carried over from the original failure — because the row tracks the last *scheduler-observed* outcome, and a re-run whose execution is later reclaimed as `unknown` never writes a terminal success back to the row.

## The real ledger

`<hermes-home>/cron/executions.db` (SQLite, read-only):

- `executions(id, job_id, source, process_id, pid, process_started_at, status, claimed_at, started_at, finished_at, error, handoff_pending, handoff_started_at, scheduled_instant, delivery_outcome)`
- `cron_incidents(id, job_id, error_sig, state, failure_type, first_seen_at, last_seen_at, acked_at, closed_at, error, output_file, alerted_at)`

Read it before classifying:

```python
import sqlite3
c = sqlite3.connect("file:/root/.hermes/cron/executions.db?mode=ro", uri=True)
for r in c.execute("SELECT id,status,claimed_at,finished_at,substr(error,1,90) "
                   "FROM executions WHERE job_id=? ORDER BY id DESC LIMIT 10", (JOB_ID,)):
    print(r)
for r in c.execute("SELECT error_sig,state,failure_type,first_seen_at,acked_at,closed_at "
                   "FROM cron_incidents WHERE job_id=? ORDER BY id DESC LIMIT 5", (JOB_ID,)):
    print(r)
```

Statuses seen in practice: `completed`, `failed` (often "Fire claim lost; execution was not started."), `unknown` (owner died before a terminal state — usually a gateway restart or drain).

## The two-fault trap

Re-running a job to test it can itself end `unknown`:

> "Scheduler restarted after this execution's owner exited before a durable terminal state"

That is the drain/restart class (`oc_gateway_drain_timeout_marks_cron_interrupted`), **not** the signature you were investigating. Attribute each execution to its own class before writing the issue, or one investigation will be charged for two faults and the second will look "reproduced" when it was not.

## Decision rule

1 occurrence with N prior successful handoffs of the same job on the same day is transient — no fix candidate. Re-running and finding the signature gone closes it as not-active.

If the signature returns, the next step is a 2x2 reproduction matrix (see `cron-worker-dependency-pin-race.md`) and an InsightProposal, not a retry.

## Related

- `references/operational-gotchas.md` — the gotchas entry for this procedure
- `references/cron-worker-dependency-pin-race.md` — the dependency-pin/lease failure pair