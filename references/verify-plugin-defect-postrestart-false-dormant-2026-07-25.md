# VERIFIER FALSE-DORMANT (inverse of the false-LIVE bug)

**Date confirmed:** 2026-07-25

## The trap

`verify_plugin_defect_postrestart.py` only counts signatures present in its
**curated regex catalog**. A signature that is NOT in the catalog — or whose
traceback text differs from the catalog regex — silently reports
`post_restart_total=0 → DORMANT`, even when it is actively recurring
post-restart. The verifier is a secondary filter; it can MISS uncatalogued
signatures and produce a false-DORMANT verdict.

## Confirmed case

- `oc_chronicle_contextengine_compress_force_kwarg_20260722` was marked
  `resolved`. A light scan found the signature live again in the profile
  gateway log at `2026-07-24T16:51:12Z` — AFTER the 15:25:08Z restart.
- The verifier reported `compress_force: post=0 → DORMANT` (do not
  re-escalate).
- Independent confirmation:
  `awk 'NR>=2918' ~/.hermes/profiles/indigo/logs/gateway.log | grep -cE "got an unexpected keyword argument 'force'"`
  returned **1** post-restart hit. The catalog regex had not matched the
  actual traceback text, so the verifier undercounted to 0.

## Rule

The raw-grep detection in Light Scan Step 2 (grep `Traceback|IntegrityError|
TypeError|ERROR gateway` since the last restart) is the **authoritative live
signal**. The verifier is advisory only.

- If the verifier says **DORMANT** but the raw-grep window contains the
  signature post-restart → treat as **LIVE**, escalate / reopen. Do NOT trust
  the DORMANT verdict.
- Symmetric to the false-LIVE guard: if the verifier says **LIVE** but
  independent `awk` returns 0 → the verdict is wrong, do not act.

Always confirm BOTH directions with independent `awk 'NR>=<restart_lineno>'`:
- false-LIVE: verifier LIVE, awk = 0 → ignore LIVE.
- false-DORMANT: verifier DORMANT, awk ≥ 1 → treat as LIVE.
