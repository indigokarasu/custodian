# Elephas Ingest Timeout — Diagnostic Pattern

## Symptom
`elephas:ingest` cron job times out at 600s with error:
```
Script timed out after 600s: <hermes-root>/scripts/elephas_ingest_wrapper.sh
```

## Root Cause
The `elephas:ingest` job in `jobs.json` has a `"script"` field pointing to `elephas_ingest_wrapper.sh`. This causes the job to run in `no_agent` script mode, bypassing the agent skill pipeline entirely.

The deprecated wrapper then:
1. Tries to stop the Ladybug bridge (may be a zombie -- ignores SIGTERM)
2. Runs the pipeline via direct DB access (may fail due to lingering locks)
3. Tries to restart the bridge (races with supervisor auto-restart)
4. Times out at 600s

## How to confirm
```bash
grep -A5 '"elephas:ingest"' <hermes-root>/cron/jobs.json | grep '"script"'
```
If this returns a line, the `script` field is set -> root cause confirmed.

## Fix
Remove the `script` field entirely from the `elephas:ingest` job. The job should run in agent mode using `prompt` + `skill`:

```bash
# Find the exact line number
grep -n '"script".*elephas_ingest_wrapper' <hermes-root>/cron/jobs.json

# Delete that line (replace NNN with actual line number)
sed -i 'NNNd' <hermes-root>/cron/jobs.json
```

## Prevention
- The `elephas:ingest` job should NEVER have a `script` field
- It should only have `prompt` + `skill` to run in agent mode
- If you find this job with a `script` field again, remove it immediately

## Related
- ocas-elephas `references/bridge-pipeline-pattern.md` -- bridge zombie recovery
- ocas-elephas `references/skill-update-log-2026-05-31.md` -- provenance
