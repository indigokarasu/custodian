# Bones Kalshi Credentials Missing

## Fingerprint
`oc_bones_missing_kalshi_creds_file` — Tier 3

## Detection
Two no_agent cron jobs fail with `FileNotFoundError: $HERMES_HOME/commons/data/ocas-bones/kalshi_creds.json`:
- `bones:position-tracker` (script: `rr_bones_position_tracker.sh`)
- `bones:market-monitor` (script: `rr_bones_market_monitor.sh`)

## Root Cause
The `kalshi_creds.json` credential file does not exist in the Bones data directory. The scripts assume it will be present after Kalshi OAuth setup.

## Fix Direction
User-gated — requires user to create `kalshi_creds.json` with valid Kalshi API credentials in `commons/data/ocas-bones/`. The scripts should ideally handle missing credentials gracefully (exit 0 with a warning rather than crashing with FileNotFoundError), but the primary fix is credential provisioning.

## Related
- This is distinct from `oc_google_tasks_access_token_missing` (which is a token-refresh race, not a missing file)
- Both Bones jobs share the same `CREDS_PATH` variable, so a single credential file fixes both