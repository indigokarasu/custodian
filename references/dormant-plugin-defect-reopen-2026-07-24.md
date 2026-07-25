# Dormant Plugin-Defect Reopen (inverse forward-stale for gateway plugin defects)

## The trap
An escalation loop can reclassify a gateway plugin-code-defect issue to
`latent_dormant` (or `resolved`) on a "0 post-restart occurrences / N-hours clean"
premise derived from `scripts/verify_plugin_defect_postrestart.py`. That premise is a
point-in-time SNAPSHOT. The defect can (and did) recur later in the same log, after the
close. A `latent_dormant` issue is NOT `open`, so the normal Step 2 "drop signatures
already represented by an OPEN issue" dedup SKIPS it — and the "new recurring signature"
branch won't fire because the issue already exists (just dormant). Result: a live,
recurring plugin defect goes silently untracked until a human notices.

This is the plugin-defect analog of the provider/auth forward-stale trap (step 8d/8e and
`references/escalation-false-recovered-note-trap.md`). There it is "don't trust a stored
error string; verify live." Here it is "don't trust a stored DORMANT close; verify the
defect didn't recur after the close."

## Confirmed case (2026-07-24)
- Issue `oc_chronicle_event_seq_unique_constraint_20260722` reclassified
  `latent_dormant` at 2026-07-23T16:10Z with `dormant_evidence` =
  "0 occurrences after 2026-07-23T04:02:50Z restart; 12h+ clean".
- Light scan 2026-07-24T04:03Z re-ran `verify_plugin_defect_postrestart.py`:
  `seq_unique: post_restart_total=7 -> LIVE (escalate / keep open)`,
  last hit `2026-07-23T20:52:37Z` — 4.7h AFTER the 16:10Z close.
- Reopened via `scripts/race_safe_issue_patch.py`:
  `status=open`, `escalation_needed=true`, `user_gated=false`, `tier=4`,
  `reopened_at=2026-07-24T04:03:43Z`, `post_restart_recurrence=7`.

## Detection recipe (run in every light scan, after Step 2)
1. Run `python3 skills/ocas-custodian/scripts/verify_plugin_defect_postrestart.py`.
   It prints per-signature `pre`/`post` and `last` timestamps, and a SUMMARY with
   `LIVE` (post_restart_total>0) vs `DORMANT` (post_restart_total=0).
2. For each signature the verifier marks LIVE, grep `issues.jsonl` for issues whose
   `fingerprint` or `error_signature` matches that signature AND whose `status` is
   `latent_dormant` or `resolved`.
3. For each match, compare the verifier's `last` post-restart timestamp against the
   issue's `reclassified_at` / `dormant_evidence` window END. If `last` is NEWER →
   false-dormant close → reopen (race-safe patch, see SKILL.md step 8g).

## PATH GOTCHA (critical)
The verifier scans BOTH of these and aggregates in its SUMMARY:
- `~/.hermes/profiles/<profile>/logs/gateway.log`   <- where the recurrence lived
- `~/.hermes/logs/gateway.log`                      <- system log, may be stale/empty
When you manually `grep` to confirm a verifier "LIVE" verdict, grep BOTH paths.
Grepping only `~/.hermes/logs/gateway.log` returned 0 hits for `seq_unique`
and would have falsely confirmed the dormant close. Always check the profile log.

## Why this is distinct from Step 2's "new recurring signature" branch
Step 2 creates a NEW issue only when a signature has NO matching entry in `issues.jsonl`.
A `latent_dormant` issue DOES have a matching entry — it just isn't `open`. So Step 2
neither flags it nor creates it. The reopen must be driven explicitly by cross-referencing
the verifier's LIVE verdict against NON-open plugin-defect issues (step 8g).

## Future improvement (optional)
`verify_plugin_defect_postrestart.py` could itself flag dormant issues whose
`error_signature` reappears after `reclassified_at`, emitting a "DORMANT-REOPEN" line so
the scan doesn't have to re-derive it. Not yet implemented — the scan-side step 8g covers it.
