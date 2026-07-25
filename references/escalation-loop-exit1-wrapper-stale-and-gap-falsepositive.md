# Escalation-loop addendum: wrapper exit-1 stale mask + journal-gap base-fingerprint false positives

Two non-obvious traps surfaced during a 2026-07-14 escalation-execution-loop run. Both produce
"something is broken / dropped" signals that are actually benign. Capture them so future loops
don't open phantom issues or reopen resolved ones.

## 1. no_agent wrapper that translates rc → exit code (stale-error trap)

When `find_missed_user_gated_jobs.py` (or light-scan Step 6) reports a job as UNKNOWN with a bare
`Script exited with code 1` plus `stdout:` but NO stderr, do NOT assume a fresh failure. A `no_agent`
wrapper may translate the inner script's exit code — e.g. `rr_rally_daily_activity_check.sh` maps
`rc=1` (warnings-report) → `exit 0`. The job's stored `last_error` ("Script exited with code 1")
can then be STALE: recorded before a wrapper fix landed, even though the wrapper now exits 0.

**Definitive stale-vs-active recipe (used 2026-07-14 on `rally:daily-activity-check`):**
<<<<<<< Updated upstream
1. Read the job's `script` field → locate the wrapper (`find <hermes-home> -name "<basename>.sh"`).
   Both profile (`<hermes-home>/profiles/indigo/scripts/`) and system (`<hermes-home>/scripts/`)
   copies usually exist; the job runs the profile copy.
2. `stat -c '%y' <wrapper>` → wrapper mtime.
3. Read job `last_run_at` (UTC; mind `-07:00` offsets — convert before comparing).
4. **Run the WRAPPER itself** with the same env it exports (`HERMES_HOME=<hermes-home>/profiles/indigo`),
=======
1. Read the job's `script` field → locate the wrapper (`find ~/.hermes -name "<basename>.sh"`).
   Both profile (`~/.hermes/profiles/indigo/scripts/`) and system (`~/.hermes/scripts/`)
   copies usually exist; the job runs the profile copy.
2. `stat -c '%y' <wrapper>` → wrapper mtime.
3. Read job `last_run_at` (UTC; mind `-07:00` offsets — convert before comparing).
4. **Run the WRAPPER itself** with the same env it exports (`HERMES_HOME=~/.hermes/profiles/indigo`),
>>>>>>> Stashed changes
   capture its real exit code: `bash <wrapper>; echo "EXIT=$?"`.
5. If the wrapper exits 0 AND its mtime is AFTER the job's `last_run_at` → the stored
   "Script exited with code 1" is STALE; the job self-heals on its next scheduled run.
   Do NOT open a new issue, do NOT pause. Running only the inner python is insufficient —
   the scheduler invokes the wrapper, and the wrapper is what translates the code.
6. If the wrapper still exits non-zero live → ACTIVE failure; de-aggregate further (run the inner
   script for the real traceback) per `no-agent-script-exit-1-deaggregation-pitfall.md`.

Contrast with `monitor-list-exit1-mask-gap.md` (a subprocess *masks* the inner traceback by
propagating non-zero). Here the wrapper *absorbs* a benign rc=1. Both yield a bare
"Script exited with code 1"; the discriminator is running the wrapper live and checking its exit
code + mtime. The existing Gotchas bullet "Re-run the actual script to confirm stale-vs-active
tracebacks" (2026-07-13) already says `bash <wrapper>` — this addendum makes the rc-translation
variant explicit and gives the mtime-vs-last_run_at proof step.

## 2. Journal→issues gap check: base-fingerprint false positives (8b / 8b-variant)

The 8b/8b-variant gap check compares each custodian journal's cited `fingerprint` / `escalation_refs`
against the set of open-issue IDs in `issues.jsonl`. Exact-string matching produces FALSE gaps because:
- Journals often cite a **generic base fingerprint** (e.g. `oc_nous_api_key_invalid_20260707`,
  `oc_openrouter_402_credits_exhausted`) while `issues.jsonl` tracks the same root cause under a
  **dated/variant id** (`oc_nous_401_key_invalid_20260707`,
  `oc_openrouter_402_credits_exhausted_20260712T040120`).
- Resolved code-defect issues (`oc_bones_kalshi_sdk_validation_...`, `oc_chronicle_facts_fts_missing_...`)
  are still cited by recent journals but are `status: resolved` and NOT live-failing (jobs paused/migrated).

**Mitigation before concluding a real gap:**
1. For each "missing" fingerprint, `grep -ac "<base_token>" issues.jsonl` (try both the exact
   string AND a truncated base, e.g. `oc_nous_api_key_invalid_20260707` and `oc_nous_api_key_invalid`).
   If the base token OR a sibling variant id appears → naming divergence, not a dropped escalation.
2. For resolved code-defects flagged as gaps, confirm live state: are the cited jobs still erroring?
   If their jobs are paused or the error is stale, leave resolved (do NOT reopen — see deep-scan 8e
   grep-pitfall: a token in comments/old ids is not proof of live breakage).
3. Only write a NEW issue when the fingerprint is genuinely absent from `issues.jsonl` under ANY id
   AND ≥1 live job currently errors with that signature.
4. `oc_state_db_oversized` is frequently cited but only actionable when disk > 80% (contextual
   threshold; state.db commonly 5–10GB in prod). At 71% it is non-actionable — do not escalate.

## Companion probes (run in order during an escalation loop)
- `scripts/verify_escalation_state.py` — bidirectional staleness, no false-resolution / inverse-gotcha counts.
- `scripts/find_missed_user_gated_jobs.py` — surfaces MISSED enrollments + UNKNOWN jobs to de-aggregate.
- `scripts/reopen_false_resolutions.py` (dry-run; `--write` to persist) — catches prior false resolutions.
- `scripts/classify_error_jobs.py` — deterministic per-job fingerprint bucket + the "Script exited with
  code 1" list to inspect individually.