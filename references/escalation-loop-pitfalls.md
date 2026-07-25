# Escalation Loop — Chronic Pitfalls (2026-07-13)

Pitfalls observed running the escalation-execution-loop against a 7-issue backlog. Each produces wasted churn (spurious issues, false reopens, racing repairs) if not known in advance.

## 1. Journal-gap probe reports FALSE gaps for resolved issues
`scripts/scan_escalation_journal_gaps.py --hours N` compares each `escalation_needed: true` journal against the set of **OPEN** issues only. If the cited fingerprint maps to an issue that EXISTS but is `resolved`, the probe still reports it as a GAP ("flagged but no matching open issue").

A 24h scan produced 9 "GAP" journals; on inspection every cited fingerprint (`oc_no_agent_git_https_no_credential_20260713`, `oc_weave_skill_path_bug_20260713T1315`, `oc_owl_alpha_model_404_20260701`, etc.) mapped to an existing, mostly-`resolved` issue. NONE were genuinely missing.

**Rule:** Before creating a missing issue from a GAP report, confirm the cited fingerprint/issue_id does NOT exist anywhere in `issues.jsonl` (any status) AND corresponds to a LIVE failure (enabled+erroring job with that fingerprint today). A resolved issue surfacing in the gap probe is expected noise, not a persistence gap. Do not create issues for resolved fingerprints.

### 1b. SECOND distinct false-positive: `escalation_refs` (name) vs `issue_id` (id) mismatch — OPEN issues reported as missing
**Confirmed 2026-07-24.** The gap-scan matches each journal's `escalation_refs` (which are **skill/job NAMES** like `ocas-autobio-observe`, `taste:sync-spotify`) against the OPEN issues' `issue_id` field (e.g. `oc_autobio_content_policy_blocked_20260723T0505Z`, `oc_taste_spotify_token_missing_20260713`). A name can never equal an id, so the matcher reports every such OPEN-and-present issue as "missing" — even though it is real and open. The scan output looks alarming ("5 missing issues") but is 100% false.

**Rule:** When the gap-scan reports GAPs, verify against the AUTHORITATIVE data-path `issues.jsonl` (never the scan's own report) before acting:
1. `python3 scripts/parse_issues_jsonl.py` → open count is the source of truth.
2. `python3 scripts/verify_escalation_state.py` → live job/issue reconciliation.
3. For each cited "missing" id, grep the authoritative file directly:
   ```python
   python3 -c "d=open('~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl').read(); print('PRESENT' if '<id>' in d else 'ABSENT')"
   ```
   PRESENT ⇒ **false positive**, not a persistence gap.
4. **Do NOT pass `--write`** to the gap-scan. It would re-persist existing open issues as duplicate escalations.

Concrete 2026-07-24 case: the scan reported 5 "missing" ids (`oc_taste_spotify_token_missing_20260713`, `oc_chronicle_event_actor_check_constraint_20260722`, `oc_state_db_oversized_20260722T0205Z`, `oc_autobio_content_policy_blocked_20260723T0505Z`, `oc_cron_config_drift_unpinned_rally_sift_20260722`) — all were PRESENT + `status: open`. No issues were missing. Full recipe + contrast with the 1a resolved-fingerprint guard: `references/escalation-gap-scan-refs-vs-issue-id-false-positive-2026-07-24.md`.

## 2. Stale last_error vs. live failure — verify by inspecting the fix, not the error
A job may still carry `last_error` matching a "resolved" issue's fingerprint even though the root cause is gone (fix landed after the job's last run; job hasn't re-run).

<<<<<<< Updated upstream
Pattern that caught a false reopen: `weave:sync-google` and `weave:enrichability-recalc` still errored with `home/.hermes` path failures, but their wrapper scripts (`rr_weave_sync.sh`) ALREADY set `AGENT_ROOT=<hermes-home>/profiles/indigo` and the canonical data (`config.json`, `weave.sqlite`) exists at the correct path. The errors were stale (last runs 01:09 / 04:00, before the wrapper fix).
=======
Pattern that caught a false reopen: `weave:sync-google` and `weave:enrichability-recalc` still errored with `home/.hermes` path failures, but their wrapper scripts (`rr_weave_sync.sh`) ALREADY set `AGENT_ROOT=~/.hermes/profiles/indigo` and the canonical data (`config.json`, `weave.sqlite`) exists at the correct path. The errors were stale (last runs 01:09 / 04:00, before the wrapper fix).
>>>>>>> Stashed changes

**Rule:** Before reopening a resolved issue, (a) read the actual wrapper/script the job runs and confirm whether the fix is present, (b) confirm canonical data/paths exist, (c) compare `last_run_at` against the fix timestamp. If the fix is in place and `last_run_at` predates it, the error is stale — leave the issue resolved; the next scheduled run clears it.

## 3. Cooperate with in-flight sanctioned sibling repairs — do not race
A data-store / skill-internal issue may already be under repair by a SANCTIONED maintainer script running concurrently (e.g. `infrastructure/chronicle-ops/scripts/repair_chronicle_fts.py`). Launching your own competing fix (re-running the enrichment script, creating the table, etc.) causes lock contention and wasted work.

**Rule:** Before auto-fixing a chronicle/DB/skill-internal issue, check for a running sanctioned repair: `ps -eo args | grep repair_.*\.py`, and inspect its source to confirm it targets the same root cause. If active, YIELD: re-enable the affected jobs (so they re-run post-repair), record the sanctioned repair as the fix, and verify its outcome instead of racing it. The sanctioned repair is authoritative.

## 4. Verifying a fix when a downstream step hits `database is locked`
When you re-run a script to PROVE a root cause is gone and it aborts with `sqlite3.OperationalError: database is locked`, check WHICH step failed. If the script reached and passed the previously-failing step (e.g. the FTS rebuild now executes via `belief_fts` with no `facts_fts` error) and only later hit the lock (contention with the concurrent repair), the root cause is still confirmed eliminated. The lock is a transient scheduling artifact, not a fix failure.

**Rule:** Read the traceback location, not just the final exception. A downstream lock error does not invalidate an earlier confirmed-passed root-cause step.

## 5. `user_gated` mislabeling on auto-fixable issues
The chronicle `facts_fts` issue was filed with `user_gated: true`, but the root cause was a skill-internal schema bug already patched in on-disk code + removable triggers. It was auto-fixable, not user-gated.

<<<<<<< Updated upstream
**Rule:** On taking an "open" issue, verify the actual fix surface (on-disk script, DB schema/triggers, config) before accepting a `user_gated` flag. If the code fix is already present, resolve it and set `user_gated: false` — don't leave it open waiting on a user who isn't actually needed.
=======
**Rule:** On taking an "open" issue, verify the actual fix surface (on-disk script, DB schema/triggers, config) before accepting a `user_gated` flag. If the code fix is already present, resolve it and set `user_gated: false` — don't leave it open waiting on a user who isn't actually needed.

## 6. Watchdog `DEGRADED` exit-1 can be a FALSE POSITIVE — confirm against the referenced entity's lifecycle, not its snapshot
A `no_agent` watchdog that exits 1 with a `DEGRADED <invariant>` message is a GENUINE failure signal per the escalation rules (NOT `oc_cron_no_agent_exit_1_noop`), and the rule book says "needs Mentor judgment." But the judgment must first rule out a false positive, because watchdogs count STATES, not EVENTS.

Confirmed 2026-07-22: `rally:pipeline-watchdog` reported `no_staged_rebalance` and was escalated as a real degraded pipeline. The watchdog's step-6 invariant counts only `status == "staged"` in `pending_actions.jsonl`. The 2026-07-22 rebalance `pa_20260722_090716_rebalance` was `staged` at 09:07 and **submitted 8/8 orders at 12:30** (`status=in_progress`). By the 15:18Z watchdog run it had ALREADY executed — so `STAGED=0` was correct and benign. The pipeline was healthy; the watchdog fired spuriously in the daily gap between executions because it missed the `in_progress`/`complete` statuses.

**Rule — reproduce-the-watchdog confirm step before resolving an escalated watchdog issue:**
1. Read the watchdog wrapper/source (e.g. `wrappers/rr_<name>_watchdog.sh`). Identify EXACTLY which invariant produced the failure (the `PROB` token, e.g. `no_staged_rebalance`) and how it is computed (the exact file + filter + Python snippet).
2. Reproduce that computation faithfully against the LIVE data file the watchdog reads — do NOT trust the issue note's summary. Write a tiny `/tmp/*.py` (no pipe; see shell-write-pattern) that replicates the watchdog's collapse/filter logic.
3. Then check the REFERENCED ENTITY'S LIFECYCLE, not its snapshot: for an `no_staged_rebalance` invariant, look at the action's full history (`staged` → `in_progress` → `complete`/`superseded`). If a corresponding action exists in a later (post-watchdog-run) state, the invariant is a stale empty-window, not a failure.
4. If benign, RESOLVE with `user_gated:false` and a note citing the evidence (the action_id + its `in_progress`/`complete` status + timestamp). Note the watchdog's own defect as a Forge recommendation (e.g. "step-6 should also accept in_progress/complete").
5. Only if the referenced entity genuinely has NO corresponding in-progress/complete activity AND the upstream job it guards actually failed, treat it as a real failure.

Generalization: any watchdog that flags on a *count of things in state X* will false-positive in the window AFTER those things transition out of X. Always check the transition history before concluding the pipeline is broken.
>>>>>>> Stashed changes
