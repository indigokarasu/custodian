# gateway.log Frozen / Stale Artifact Pattern

**Confirmed:** 2026-07-07 (light scan).

## Symptom

A log-scanning step (e.g., Light Scan Step 2: "tail gateway log for new errors
since last scan timestamp") returns **0 errors**, suggesting a clean system — but
real failures exist elsewhere (in `jobs.json` `last_error` fields, or the cron
output dir).

## Root cause

`<hermes-home>/logs/gateway.log` is **no longer written by the current gateway
instance**. The live gateway (started via `python -m hermes_cli.main ... gateway run`)
logs to a different destination (journald, a rotated/redirected path, or nowhere
that this file captures). The file sits with old content and a stale mtime.

On 2026-07-07 the file's last line was `2026-06-24T13:24:46Z` while the running
gateway PID 4087544 had started at `04:04Z` that same day. Its mtime read
`2026-06-24T13:24:46Z` (confirmed via `ls --time-style=+%Y-%m-%dT%H:%M:%SZ`),
despite the process being ~17h old and active.

## Detection recipe

```bash
# 1. Is the gateway even running, and when did it start?
ps aux | grep 'gateway run' | grep -v grep

# 2. What is the LAST real timestamp inside the log (not the file mtime)?
grep -oE '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}' <hermes-home>/logs/gateway.log | tail -1

# 3. Compare against the running instance's start. If the log's last line
#    predates the process start, the log is frozen.
ls --time-style=+%Y-%m-%dT%H:%M:%SZ -la <hermes-home>/logs/gateway.log

# 4. Where is the LIVE evidence? The cron output dir is written by the
#    scheduler regardless of where the gateway logs:
ls -la --time-style=+%Y-%m-%dT%H:%M:%SZ <hermes-home>/profiles/<profile>/cron/output/ | tail
```

## Correct handling

- **Never conclude "no new gateway errors" from a frozen `gateway.log`.**
- Fall back to the authoritative cron-error sources:
  - `jobs.json` per-job `last_error` (already the primary signal for cron health).
  - The cron output dir (`<hermes-home>/profiles/<profile>/cron/output/`) — its
    mtime advances with every real job run.
  - Journal dirs under `commons/journals/<skill>/<YYYY-MM-DD>/`.
- Note the frozen log in the observation journal so future scans don't re-chase it.

## Distinction from the cron/output freeze

The existing gotcha "Cron output file is ground truth — UNLESS a gateway restart
froze it" covers the `cron/output/` tree stopping after a gateway restart. This
pattern is the **gateway.log itself** being stale while the gateway runs fine —
a different failure (log redirection/rotation mismatch), same misleading "clean"
result. Both produce false 'clean' verdicts from stale sources.

## Related false-'clean' sources (re-derive, don't trust summaries)

- **Prior scan miscounts:** a previous scan's journal may report
  `error_jobs_enabled_actionable: N` with paused jobs counted as actionable
  (it forgot to filter `oc_cron_disabled_stale_error`). Always re-derive the
  enabled-error set from the LIVE `jobs.json` each scan; never trust a prior
  scan's summary count.
- **Frozen cron/output/ dir** (see the cron-job-repair verification pitfall).
