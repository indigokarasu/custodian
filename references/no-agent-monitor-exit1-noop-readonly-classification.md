# Classifying a no_agent Monitor exit-1 No-Op — Read-Only, Side-Effect-Free (2026-07-08)

## Problem
A `no_agent: true` monitor job shows `last_error: "Script exited with code 1"` with NO
stderr. Per the escalation execution loop, an UNKNOWN job must be classified. The naive
test — run the script directly — is wrong because it mutates state.

## Why running the script is unsafe
Many no_agent monitors have SIDE EFFECTS on their "found work" path:
- `monitor_journals.py` reads `STATE_FILE.latest_mtime`; if a NEW journal appeared it
  ENQUEUES a work item to `monitor_queue.jsonl` and updates state, then exits 0. Running
  it manually either (a) enqueues a DUPLICATE work item (if a sibling process already
  enqueued the same new journals), or (b) masks the no-op if no new journals exist.
  Neither helps classification and one corrupts the queue.

## Read-only classification procedure
1. Read the script source to learn its exit-1 conditions. A no-op monitor exits 1 when
   there is nothing new to do (e.g. `monitor_journals.py` exits 1 when
   `latest_journal_mtime <= STATE_FILE.latest_mtime`). It only exits 0 (and enqueues)
   when new work exists. (It exits 2 + stderr on exception — that is a real failure, not
   a no-op.)
2. Replicate the comparison READ-ONLY, without executing the script:
   - Find the latest journal file mtime under the monitor's `JOURNALS_DIR`:
     `Path(JOURNALS_DIR).rglob("*.json")` -> `max(f.stat().st_mtime for f if f.is_file())`.
   - Read the monitor's state file (e.g.
<<<<<<< Updated upstream
     `<hermes-home>/commons/data/monitor_state/journal_ingest_state.json`) ->
=======
     `~/.hermes/commons/data/monitor_state/journal_ingest_state.json`) ->
>>>>>>> Stashed changes
     `latest_mtime`.
   - If `latest_journal_mtime <= state.latest_mtime` -> genuine no-op
     (`oc_cron_no_agent_exit_1_noop`, Tier 2, leave running). The monitor ran, found
     nothing new, exited 1 BY DESIGN.
   - If `latest_journal_mtime > state.latest_mtime` -> the monitor SHOULD have enqueued
     and exited 0; an exit-1 here is a REAL fault (or a state-file write failure) —
     inspect further (do not leave as no-op).
3. No stderr in `last_error` + read-only `latest <= last` => confirmed no-op. Leave the
   job RUNNING; do not pause, do not enqueue.

## Note on mtime lag
Journal file mtimes lag their content timestamps by ~7h12m (see Mentor
`cron-mtime-discovery-gotcha.md`). This does NOT break the comparison: both the state
file and the journal files use real file mtimes, so relative ordering is internally
consistent. The monitor only cares about which journal is newest, not absolute time.

## Distinguish from upstream-degraded
This no-op case is the OPPOSITE of `oc_<svc>_upstream_degraded_no_results` (see
`no-agent-monitor-exit1-upstream-degraded-pitfall.md`): that fault has stdout like
`UNHEALTHY: no results` + `Restart FAILED` and is a REAL infra fault. A pure no-op has
no such stdout and exits 1 simply because there was no work. If in doubt, probe the
dependency LIVE (per the upstream-degraded reference) before classifying.