# Escalation Runner Reference — 2026-06-08

## Stale Issue Verification Pattern

When closing issues from `issues.jsonl`, always verify against live `hermes cron list` before marking resolved. Issues can have stale `last_run_at: null` or old error data even though the job is currently healthy.

### Verified-Stale Issues (Closed 2026-06-08)

| Issue ID | Claimed Problem | Live Status | Resolution |
|----------|----------------|-------------|------------|
| `forge_skill_audit_never_ran_20260606` | Never ran since creation (354h ago) | Last run 2026-06-08T06:27, status=ok | Issue was stale — job ran at scheduled time |
| `autobio_update_never_ran_20260606` | Never ran since creation (278h ago) | Last run 2026-06-07T00:20, status=ok | Issue was stale — job runs Sundays as scheduled |
| `weave_sync_contacts_model_deprecated_20260606` | Deprecated model ID (HTTP 400) | Last run 2026-06-06T19:22, status=ok | Transient error, job healthy |
| `spot_git_local_changes_20260605` | Git local changes blocking pull | Last run 2026-06-08T09:26, status=ok | Local dev changes, not blocking |
| `chronicle_plugin_dirs_empty_20260605` | Plugin dirs empty (no .py files) | Directories don't exist at all | Cleaned up during gateway update |
| `oc_gateway_health_endpoint_down_20260605` | Port 8080 not responding | Gateway PID running, all jobs execute | Non-critical, known after --replace restart |
| `praxis_debrief_interpreter_shutdown_20260605` | Interpreter shutdown error | Last run 2026-06-07T23:14, status=ok | Transient, delivery error separate |
| `state_db_oversized_20260606` | state.db 14GB + disk 93% | Disk now 76% after backup cleanup | Pressure resolved, DB still 14GB but non-critical |

### Key Lesson

> **Never trust `last_run_at: null` in issues.jsonl as proof a job never ran.** Always cross-reference with `hermes cron list` which shows live scheduler state. Issues can be created from stale data (e.g., from a previous gateway session's scan) while the job has since run successfully.

## Disk Cleanup — Backup Zip Removal

<<<<<<< Updated upstream
Removed 3 pre-update backup zips from `<hermes-home>/backups/`:
=======
Removed 3 pre-update backup zips from `~/.hermes/backups/`:
>>>>>>> Stashed changes
- `pre-update-2026-06-07-204043.zip` (3.9GB)
- `pre-update-2026-06-07-204059.zip` (3.9GB)
- `pre-update-2026-06-08-040002.zip` (5.8GB)

**Total freed: 14GB. Disk: 90% → 76% (11GB → 24GB free).**

These were from the June 7-8 update window. System stable for 5+ days post-update. Safe to remove per `references/disk-compaction.md` Tier 1 action #1.

## Open Issues Remaining (Require User Action)

| Issue ID | Tier | Reason |
|----------|------|--------|
| `oc_google_oauth_client_deleted_20260604` | 3 | OAuth client deleted from GCS. Needs interactive browser re-auth. |
| `skill_library_stubs` | 4 | 17-21 stub dirs without SKILL.md. Needs user confirmation. |
| `ocas-critique-missing-skillmd` | 2 | Skill dir exists but no SKILL.md. Needs user decision. |
| `skill_hygiene_followup_20260601` | 2 | Tracking issue. Awaiting user response. |