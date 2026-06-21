# RCA Backfill — Analysis of Last 3 Weeks (2026-05-15 to 2026-06-05)

## Seeded RCA Records

Based on analysis of fixes.jsonl, issues.jsonl, fix_effectiveness.jsonl, and scan report files.

### 1. oc_cron_dead_script_ref — Pattern B (Fix Didn't Hold)

**Recurrence count**: 3 fix applications in 21 days (2026-05-15, 2026-05-19, 2026-05-30)
**Sub-fingerprints**:
- `oc_cron_dead_script_ref:google_oauth_refresh` — single occurrence, path corrected from `refresh_google_tokens.py` to correct path
- `oc_cron_dead_script_ref:batch_path_update` — 12 jobs updated from relative to absolute paths on 2026-05-19
- `oc_cron_dead_script_ref:manual_verification` — resolved manually on 2026-05-19

**Root cause hypothesis**: The `oc_cron_dead_script_ref` fingerprint conflates at least two distinct problems:
1. **Individual jobs with wrong script paths** (e.g., google-oauth-refresh pointing to a deleted script) — one-off fixes, correctly resolved by updating/deleting the job
2. **Batch path failures** where groups of jobs use relative paths that break when cron cwd doesn't match `HERMES_HOME` — the 2026-05-19 fix updated 12 jobs to absolute paths, but this doesn't prevent the same issue from recurring for new jobs or after gateway reinstalls

**Variable**: path_resolution (cron cwd vs `HERMES_HOME`)
**Fix stickiness**: Low. The batch fix on 2026-05-19 resolved 12 jobs, but the systemic issue (cron security model rejecting paths outside `HERMES_HOME/scripts/`) remains unaddressed for any newly registered jobs.
**Recommended action**: When registering new cron jobs, always use absolute paths under `$HERMES_HOME/profiles/<profile>/scripts/`. Add a validation step in custodian init that checks new job script paths.
**Pattern**: C (different root causes under same fingerprint — individual dead refs vs systemic path resolution)
**Tier**: 2 (systemic fix requires validation logic change, not just a path update)

### 2. oc_cron_orphaned_job — Pattern C (Different Root Cause Each Time)

**Recurrence count**: 3 fix applications in 21 days
**Occurrences**:
- 2026-05-13: haiku:haiku-post incorrectly removed (was declared in SKILL.md but never ran)
- 2026-05-14: scout:sources-refresh incorrectly registered (not declared), then removed
- 2026-05-20: bones:lirr-watch removed (genuinely orphaned)

**Root cause hypothesis**: The orphaned job detection has no false-positive guard. It removes jobs based on a simple heuristic (never ran + not declared) without checking for edge cases like:
- Jobs declared in YAML frontmatter instead of markdown tables
- Jobs with names that differ slightly between SKILL.md and jobs.json
- Newly registered jobs with future `next_run_at`
- User-created infrastructure jobs not declared in any SKILL.md

**Variable**: detection_logic (heuristic too broad)
**Pattern**: C (each occurrence was a different failure mode of the same heuristic)
**Recommended action**: Never remove a job with `next_run_at` in the future. Always verify against both SKILL.md locations. When in doubt, escalate instead of deleting.
**Tier**: 2 (requires detection logic improvement)

### 3. oc_gateway_service_failed — Pattern A Then Resolved

**Recurrence count**: 2 fix applications in 21 days
**Occurrences**:
- 2026-05-20: Gateway systemd unit in failed state, ExecStart path stale → fixed by `hermes gateway install --force`
- 2026-05-30: systemd inactive but health=ok → identified as --replace takeover pattern, no fix needed

**Root cause hypothesis**: Recurring after gateway updates that change the Python path. The systemd unit's `ExecStart` references a venv path that doesn't survive updates. The `--replace` pattern (gateway takeover without systemd) is by design but looks like a failure.
**Fix stickiness**: High for the systemd fix. The `--force` reinstall corrects the path. Recurrence is expected after each gateway update.
**Pattern**: D (transient, self-resolving after each update cycle)
**Tier**: 2 (known pattern, no structural fix needed beyond existing auto-detection)

### 4. oc_http_429_rate_limit — Pattern D (Transient)

**Recurrence count**: 4+ occurrences in 21 days, always resolved
**Occurrences**: 2026-05-30 (3 jobs), 2026-05-31 (2 new jobs + 2 returns), 2026-06-04 (5 occurrences)

**Root cause hypothesis**: Provider-side rate limiting from OpenRouter (weekly usage limits, concurrent request limits). Always self-resolves. No Custodian action needed.
**Pattern**: D (transient, self-resolving)
**Tier**: 2 (monitoring only)
**Note**: Current handling is correct — all occurrences correctly classified as transient.

### 5. oc_cron_no_agent_mismatch — Pattern A (Fixed, No Recurrence)

**Recurrence count**: 2 occurrences on same day (2026-05-30), both fixed by setting `no_agent=False`
**Jobs affected**: elephas:ingest, rally:update
**Root cause**: Scheduler internal state mismatch — `jobs.json` had `no_agent=False` but scheduler's internal representation had `no_agent=True`. Root cause of the state divergence is unknown but may be a race condition during job creation.
**Fix**: Set `no_agent=False` in jobs.json (was already correct) and pause/resume to reset scheduler state.
**Pattern**: A (fix applied, holding as of 2026-06-04)
**Tier**: 1 (resolved)

### 6. stealth_browser_mcp_server_connection_failure — Pattern D (Transient/Persistent Non-Fatal)

**Recurrence count**: 2+ occurrences in 21 days (2026-05-31, 2026-06-01)
**Root cause**: MCP server connection failures — stealth-browser fails initial connection retries (3 attempts). Known on-demand MCP pattern. Gateway health remains ok.
**Pattern**: D (persistent but non-fatal, degrades gracefully)
**Tier**: 2 (monitoring only)

### 7. oc_background_task_missing — Pattern A (Systematic Registration)

**Recurrence count**: 11 fix applications in 21 days — **highest frequency**
**Analysis**: This is not a recurring failure — it's a systematic registration of missing cron jobs across 11 different skills. Each fix registered a different missing job (rally:daily, haiku:haiku-post, scout:sources-refresh, odds:update, vesper:deliver-morning, etc.).
**Pattern**: A for each individual job (each was genuinely missing and correctly registered)
**Root cause**: Jobs declared in SKILL.md but cron registration was missed during initial setup or skill installation.
**Recommendation**: After installing or updating any skill, a custodian scan should automatically check for unregistered declared tasks. This is already the designed behavior — the 11 registrations represent Custodian doing its job correctly.
**Tier**: 1 (correctly resolved)

### 8. oc_disk_full — Pattern D (Transient Environmental)

**Recurrence count**: 4 occurrences in 21 days (state-snapshots directory growth)
**Root cause**: Gateway update process creates large state-snapshot directories (14-29GB). Disk fills, jobs fail, cleanup resolves it.
**Fix**: Remove stale snapshot directories. Space freed: 29G (05-13), 14GB (05-14), individual snapshots (05-16).
**Pattern**: D (transient, triggered by gateway update cycle)
**Recommendation**: Add a pre-update disk check. If disk >75%, warn before gateway update instead of cleaning up after.
**Tier**: 2 (Tier 3 on 05-13 when disk hit 99%, but resolved before escalation)

## Summary of Findings

| Fingerprint | Pattern | Root Cause Category | Fix Holding? | Recommendation |
|---|---|---|---|---|
| oc_cron_dead_script_ref | C (multi-cause) | path_resolution | Partially | Decompose into sub-fingerprints |
| oc_cron_orphaned_job | C (multi-cause) | detection_logic | N/A (false positives) | Add guards for edge cases |
| oc_gateway_service_failed | D (transient) | dependency_shift | Yes after fix | Document --replace pattern |
| oc_http_429_rate_limit | D (transient) | environmental | N/A | Continue monitoring |
| oc_cron_no_agent_mismatch | A (resolved) | state_mutation | Yes | Monitor for recurrence |
| stealth_browser_mcp_failure | D (transient) | dependency_shift | N/A | Continue monitoring |
| oc_background_task_missing | A (systematic) | initialization | Yes | Working as designed |
| oc_disk_full | D (transient) | environmental | Yes after cleanup | Add pre-update disk check |

## Key Insight

The two fingerprints that represent actual fix-loop risk (Patterns B/C) are:
1. **oc_cron_dead_script_ref** — needs sub-fingerprint decomposition
2. **oc_cron_orphaned_job** — needs detection logic hardening

Everything else is either correctly resolved (Pattern A), genuinely transient (Pattern D), or working as designed (background task registration). Custodian has been handling the transient cases well — the fix-loop problem is concentrated in 2-3 fingerprints that need the RCA treatment.
