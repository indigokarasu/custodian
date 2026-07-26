# Paused job with a real untracked error — escalation gap

**Confirmed:** 2026-07-13. **Class:** Custodian scan gap (light + deep).

## The blind spot

The skill says: *filter paused jobs from the actionable error count* (`oc_cron_disabled_stale_error`).
That is correct for the 95% case (paused jobs hold frozen stale-provider / transient errors).

But it creates a silent gap for the other 5%: a job paused as mitigation for a **real
non-provider failure** (skill-code bug, missing file, subprocess traceback) where **no
tracking issue was ever written**. The standard reconciliation tooling cannot see it:

- `scan_escalation_journal_gaps.py` — only flags journals with `escalation_needed: true`
  whose fingerprint is absent from `issues.jsonl`. If the prior scan paused the job
  *without* flagging escalation, nothing references it.
- `verify_escalation_state.py` — only validates jobs ALREADY listed in some issue's
  `jobs_paused`. An untracked paused job is in no issue, so it is invisible.
- Step 8b / 8b-variant — only reconcile issues that already exist or were named in a
  delta's `stable_root_cause`. A job paused with no issue is in neither.

Result: the root cause is paused (good) but **untracked** (bad) — it never reaches
Mentor, and a future "all clean" delta silently drops it.

## Detection recipe (run after the paused-job filter)

1. From the live `jobs.json`, collect `enabled == False` (or `state: paused`) jobs.
2. For each, read `last_error`. Skip if it matches a benign class:
   - provider-outage / credits / `token_expired` / `portal.nousresearch.com` / `402 credits`
   - transient (futures shutdown, gateway restart import window, `Script exited with code 1`
     with no stderr = no-op, gateway collision, 429)
   - already-known `oc_cron_disabled_stale_error` (error pre-dates a config change)
3. For the survivors (REAL failures: `FileNotFoundError`, `ModuleNotFoundError`,
   subprocess traceback, `Database file does not exist`, skill path bug), check
   `issues.jsonl` (brace-depth parse) for an open issue whose `jobs_paused` or
   `affected_components` names the job.
4. If none, **persist one**:
   ```json
   {"issue_id": "oc_<fp>_<UTCstamp>", "status": "user_gated",
    "escalation_needed": true, "fingerprint": "oc_<fp>",
    "jobs_paused": ["<job names>"], "paused_reason": "<why paused + that fix needs skill-code edit>"}
   ```
   Do NOT leave the job running to "retry" if the failure is a deterministic code bug
   (it will fail identically every run) — pausing is the correct mitigation; the issue
   is what was missing.

## Worked example (2026-07-13)

- `weave:enrichability-recalc` (id `33063cc8b3b9`), `no_agent`, paused.
  `last_error`: `Script exited with code 1 ... Database: <hermes-home>/profiles/indigo/home/.hermes/commons/db/ocas-weave/weave.sqlite ... ERROR: Database file does not exist.`
- `weave:sync-contacts` (id `4d424eaf9185`), `no_agent`, paused.
  `last_error`: `FileNotFoundError: .../home/.hermes/commons/data/ocas-weave/config.json`.
- Real cause: `ocas-weave` scripts build paths as `<profile>/home/.hermes/commons/...`
  but the real layout is `<profile>/commons/...` (no `home/.hermes` segment). Skill-internal
  path-resolution bug; Custodian cannot edit skill package files → user-gated Tier 3.
- Both were paused by a prior scan but no issue existed. Created
  `oc_weave_skill_path_bug_20260713T1315` with `jobs_paused` listing both.