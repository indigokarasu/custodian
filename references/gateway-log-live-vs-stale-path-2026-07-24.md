# Gateway log: live vs stale path (2026-07-24)

## The trap
In this deployment there are TWO files named `gateway.log`:
- `~/.hermes/logs/gateway.log` — **STALE copy**. Last entries dated 2026-06-11 / 2026-06-24. Grepping it returns pre-restart noise (e.g. the June `store.py` `criticality_reason` tracebacks) and MISSES live July plugin tracebacks.
- `~/.hermes/profiles/<profile>/logs/gateway.log` — **LIVE**. This is the file `scripts/verify_plugin_defect_postrestart.py` reads. Shows the real last-restart marker and post-restart recurrence counts.

Plugin tracebacks ALSO land in `~/.hermes/logs/errors.log` in real time (e.g. `chronicle/engine/store.py` `append_event` / `upsert_belief` tracebacks).

## Reliable detection recipe (Step 2 gateway-traceback-gap)
1. Verify recency of any candidate `gateway.log` BEFORE trusting it:
   `grep -oE "20[0-9]{2}-[0-9]{2}-[0-9]{2}" ~/.hermes/logs/gateway.log | tail -3` → if it returns June, it's the stale copy; use the profile path instead.
2. Run the post-restart verifier against the LIVE log:
   `python3 ~/.hermes/profiles/indigo/skills/ocas-custodian/scripts/verify_plugin_defect_postrestart.py --pattern actor_check=CHECK\ constraint\ failed --pattern seq_unique=UNIQUE\ constraint\ failed`
   It reports `pre`/`post` restart counts per signature and a LIVE vs DORMANT verdict.
3. For any `store.py` traceback found in `errors.log`, check its DATE — a month-old (pre-restart) traceback is noise to drop per the Step 2 'drop pre-restart-only' rule; only a post-restart recurrence is actionable.

## Confirmed case (2026-07-24 light scan)
- Root `gateway.log`: 14 `store.py` tracebacks, ALL dated 2026-06-12 → stale, dropped.
- Live profile `gateway.log` (verifier): `actor_check` pre=11 post=11 last=2026-07-23T11:59Z → LIVE; `seq_unique` pre=4 post=8 last=2026-07-23T21:45Z → LIVE; `compress_force` pre=14 post=0 → DORMANT.
- Both LIVE signatures were already `open` + `escalation_needed: true` in the authoritative `issues.jsonl` → Step 8b gap satisfied, no new issue written.
