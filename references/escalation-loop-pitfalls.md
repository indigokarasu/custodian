# Escalation Loop — Chronic Pitfalls (2026-07-13)

Pitfalls observed running the escalation-execution-loop against a 7-issue backlog. Each produces wasted churn (spurious issues, false reopens, racing repairs) if not known in advance.

## 1. Journal-gap probe reports FALSE gaps for resolved issues
`scripts/scan_escalation_journal_gaps.py --hours N` compares each `escalation_needed: true` journal against the set of **OPEN** issues only. If the cited fingerprint maps to an issue that EXISTS but is `resolved`, the probe still reports it as a GAP ("flagged but no matching open issue").

A 24h scan produced 9 "GAP" journals; on inspection every cited fingerprint (`oc_no_agent_git_https_no_credential_20260713`, `oc_weave_skill_path_bug_20260713T1315`, `oc_owl_alpha_model_404_20260701`, etc.) mapped to an existing, mostly-`resolved` issue. NONE were genuinely missing.

**Rule:** Before creating a missing issue from a GAP report, confirm the cited fingerprint/issue_id does NOT exist anywhere in `issues.jsonl` (any status) AND corresponds to a LIVE failure (enabled+erroring job with that fingerprint today). A resolved issue surfacing in the gap probe is expected noise, not a persistence gap. Do not create issues for resolved fingerprints.

## 2. Stale last_error vs. live failure — verify by inspecting the fix, not the error
A job may still carry `last_error` matching a "resolved" issue's fingerprint even though the root cause is gone (fix landed after the job's last run; job hasn't re-run).

Pattern that caught a false reopen: `weave:sync-google` and `weave:enrichability-recalc` still errored with `home/.hermes` path failures, but their wrapper scripts (`rr_weave_sync.sh`) ALREADY set `AGENT_ROOT=<hermes-home>/profiles/indigo` and the canonical data (`config.json`, `weave.sqlite`) exists at the correct path. The errors were stale (last runs 01:09 / 04:00, before the wrapper fix).

**Rule:** Before reopening a resolved issue, (a) read the actual wrapper/script the job runs and confirm whether the fix is present, (b) confirm canonical data/paths exist, (c) compare `last_run_at` against the fix timestamp. If the fix is in place and `last_run_at` predates it, the error is stale — leave the issue resolved; the next scheduled run clears it.

## 3. Cooperate with in-flight sanctioned sibling repairs — do not race
A data-store / skill-internal issue may already be under repair by a SANCTIONED maintainer script running concurrently (e.g. `infrastructure/chronicle-ops/scripts/repair_chronicle_fts.py`). Launching your own competing fix (re-running the enrichment script, creating the table, etc.) causes lock contention and wasted work.

**Rule:** Before auto-fixing a chronicle/DB/skill-internal issue, check for a running sanctioned repair: `ps -eo args | grep repair_.*\.py`, and inspect its source to confirm it targets the same root cause. If active, YIELD: re-enable the affected jobs (so they re-run post-repair), record the sanctioned repair as the fix, and verify its outcome instead of racing it. The sanctioned repair is authoritative.

## 4. Verifying a fix when a downstream step hits `database is locked`
When you re-run a script to PROVE a root cause is gone and it aborts with `sqlite3.OperationalError: database is locked`, check WHICH step failed. If the script reached and passed the previously-failing step (e.g. the FTS rebuild now executes via `belief_fts` with no `facts_fts` error) and only later hit the lock (contention with the concurrent repair), the root cause is still confirmed eliminated. The lock is a transient scheduling artifact, not a fix failure.

**Rule:** Read the traceback location, not just the final exception. A downstream lock error does not invalidate an earlier confirmed-passed root-cause step.

## 5. `user_gated` mislabeling on auto-fixable issues
The chronicle `facts_fts` issue was filed with `user_gated: true`, but the root cause was a skill-internal schema bug already patched in on-disk code + removable triggers. It was auto-fixable, not user-gated.

**Rule:** On taking an "open" issue, verify the actual fix surface (on-disk script, DB schema/triggers, config) before accepting a `user_gated` flag. If the code fix is already present, resolve it and set `user_gated: false` — don't leave it open waiting on a user who isn't actually needed.
