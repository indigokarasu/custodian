# Escalation Runner 2026-06-17

**Run ID**: esc-run-20260617-1714
**Outcome**: 1 fix applied, 0 new escalations

## Workflow Optimization Confirmed

The "check latest esc-run journal first" optimization (from 2026-06-08) continues to work well:
- Latest esc-run journal (21:09 June 16) showed 0 escalated issues, system healthy
- Full scan confirmed: 115/116 jobs OK, 1 transient error (CF=0)
- 5-second journal check replaced a 60-second full scan

## Fix Applied

- `oc_subdirectory_hints_home_dir` — Patched `subdirectory_hints.py` line 147 to catch `RuntimeError` from `expanduser()` when `$HOME` is unset. See `references/subdirectory-hints-home-dir-pattern.md`.

## Issues Verified Still Open (all need user action)

- `skill_library_stubs` (Tier 4) — 15+ stub dirs without SKILL.md
- `ocas-critique-missing-skillmd` (Tier 2) — SKILL.md missing
- `skill_hygiene_followup` (Tier 2) — 21 stub dirs + 35 nested .git

## Issues Resolved Since Last Check

- `oc_google_oauth_refresh_400` / email:check — `last_status=ok`, `consecutive_failures=0`
- `custodian_status_keyerror_attempts` — 0 occurrences in current errors.log
- SearXNG Health Watchdog — Transient, self-resolved

## Key Lesson: Verify Before Acting

Multiple issues flagged in custodian journals as `escalation_needed: true` were already resolved by the time the escalation runner checked:
- Google OAuth re-authorized by <operator> (email:check fixed)
- Plugin code bug fixed (custodian_status KeyError)
- Transient errors self-resolved (SearXNG, script path blocks)

**Always verify current live state before acting on escalated issues.** The journal flag `escalation_needed: true` is a signal to investigate, not a guarantee that action is needed.
