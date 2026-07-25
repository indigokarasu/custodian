# Resolved Code-Defect Fix Regression Verification

When a custodian issue is marked `resolved` with a code-level fix
(`tier: 4` or a `fingerprint` of the form `oc_*_bug` / `oc_*_missing` /
`oc_*_path_*`), the automated `reopen_false_resolutions.py` guard does
NOT catch it. That script only matches provider/auth/credit outage
signatures (`token_expired`, `402 credits`, `owl-alpha 404`). A code fix
that was applied incompletely slips through as "resolved" forever.

## Detection recipe (manual, in light/deep scan)

For each `resolved` issue whose fingerprint is a code defect:

1. **Does the error still fire live?** Grep `jobs.json` for any enabled
   job whose `last_error` still contains the original error signature
   (e.g. `no such table: facts_fts`). If yes → the fix did not hold.
   BUT first check the job's `last_run_at` against the fix timestamp:
   if the job ran BEFORE the fix landed, the error is STALE, not live.
   Re-run the script (step 3) before reopening.

2. **Does the source still contain the broken reference?** The fix may
   have patched one code path (or only added a comment) while another
   path still references the broken construct. Grep the touched file:
   `grep -rn "<broken_token>" <file_or_dir>`
   Confirmed 2026-07-13: `oc_chronicle_facts_fts_missing_20260713` was
   marked resolved, but `enrich_embeddings.py:121` still executed
   `DELETE FROM facts_fts` — a table absent from `chronicle.db`
   (which has `belief_fts`/`observed_fts` only). The fix's comment
   claimed "we now rebuild the real tables" yet the erroring line was
   never changed. Live run aborted `OperationalError`. Reopened as
   Tier 4.

3. **Re-run the script to separate stale from live.** When in doubt,
   execute the job's `script` directly in its real env
   (`<hermes-venv>/bin/python <script>` or `bash <wrapper>`)
   and inspect the exit code + stderr. Exit 0 with real output =
   fix holds (stale error). Traceback = fix incomplete (reopen).
   Confirmed 2026-07-13: `vibes:update` showed a stale git-auth
   error; running `update_vibes.sh` manually returned `OK: ocas-vibes`
   exit 0 → fix held, no reopen.

## Reopen shape

```json
{
  "status": "open",
  "escalation_needed": true,
  "tier": 4,
  "resolved_at": null,
  "reopened_at": "<utc iso>",
  "reopen_note": "Regression: <file>:<line> still references <broken_token>. Live run <ts> aborted <error>. Fix was incomplete (comment-only / single-path)."
}
```

## REVISION 2026-07-13 — the chronicle case was a MISREAD, do NOT reopen

The original Step 8e + this file cited `oc_chronicle_facts_fts_missing_20260713`
as a confirmed false resolution ("`enrich_embeddings.py:121` still executed
`DELETE FROM facts_fts` ... the line was never changed"). **That was wrong.**
Root-cause diagnosis:

- The string `facts_fts` appears in `enrich_embeddings.py` ONLY inside
  historical comment lines (≈120, 124). The executable statements use the
  LIVE-schema tables `belief_fts` and `observed_fts` (lines 130/149), which
  exist in `chronicle.db`. Dry-executing both DELETEs against the live DB
  returns no `OperationalError`.
- The job's stored `last_error` was STALE: the job last ran 2026-07-13
  10:02 UTC, but the fix file's mtime is 11:36 UTC — the error predates the
  fix. Comparing `last_run_at` to the fix mtime is mandatory (Step 8e (a))
  and was skipped in the original write-up.
- The companion `oc_script_timeout_chronicle_embed_20260713` was likewise
  resolved correctly: `chronicle_daily_embed.py` has a substantive
  `DAILY_DOC_LIMIT=500` + `SOFT_TIMEOUT_SECS=540` + enforced `deadline`
  break; its stored timeout is also pre-fix (10:12 vs 11:40 UTC).

**Grep-pitfall the misread exposed:** `grep -rn "<token>" <file>` matches
comments too. A token in a comment is NOT proof the broken path still runs.
Before reopening on a grep hit, READ the executable lines at/around the cited
line number, or execute the real code path. Verify `last_run_at` > fix mtime.
Leave both chronicle issues `resolved`.

## REVISION 2026-07-13 (late) — wrapper env-export pitfall: re-run VIA the wrapper, not bare python

Step 8e item 3 says to "execute the job's `script` directly … (`<hermes-venv>/bin/python <script>` or `bash <wrapper>`)". When the fix was applied to the **wrapper** (e.g. adding `export AGENT_ROOT=<hermes-home>/profiles/indigo` to a `rr_*.sh` so the skill stops falling back to `Path.home()`), running the script via bare `python3` **bypasses the fix** and reproduces the original error — a false-positive regression.

Confirmed 2026-07-13: `oc_weave_skill_path_bug_20260713T1315` (weave:sync-google) was marked resolved after `AGENT_ROOT` was exported in `rr_weave_sync.sh`. Bare `python3 skills/ocas-weave/scripts/google_sync.py` in a clean env raised `FileNotFoundError: …/ocas-weave/config.json` (the old `Path.home()`-fallback path) → looked like a live failure. Re-running **through the wrapper** (`bash scripts/rr_weave_sync.sh`, which sets `AGENT_ROOT` then `exec`s the script) returned `EXIT=0` (587 contacts pushed) → resolution VALID. The bare-python test was simply running the unfixed code path.

**Rule:** For any `no_agent` job whose fix touched a wrapper script (env exports, `cd`, arg changes), re-run the **wrapper cron actually invokes**, not the underlying script directly. Only fall back to bare-script invocation when the fix was inside the script itself (not the wrapper). When in doubt, run both: wrapper-exit-0 + bare-script-reproduces-bug ⇒ wrapper fix holds, not a regression.