# Config Empty Section Fix-Loop Status

The `oc_config_empty_section` fix-loop has been the single most recurring issue in the custodian system (8+ occurrences from 2026-06-16 to 2026-06-28).

## Current Status: ACTIVE / DORMANT CYCLE (2026-06-28)

- Main config (`<hermes-home>/config.yaml`): **CLEAN** after 2026-06-28 deep scan
- the agent profile config (`<hermes-home>/profiles/indigo/config.yaml`): **CLEAN**
- Last fix applied: 2026-06-28 (8th occurrence — 3 null keys removed via PyYAML)
- **Pattern B RCA says null keys regenerate on gateway restart**

## Key Insight (2026-06-28)

Despite being Pattern B (no fix per policy), this fingerprint is safe to fix during deep scan because:
- Removing null keys has no destructive side effects
- The fix resolves TUI warnings immediately
- The recurrence is an architectural issue (gateway regeneration), not a data integrity risk
- Escalation of the root cause should continue alongside the tactical fix

**Resolution**: Apply fix during scan → note fix-loop status → escalate architectural root cause.

## Occurrence History

| Date | Issue ID | Notes |
|------|----------|-------|
| 2026-06-16 | `oc_config_empty_sections_20260616` | First occurrence. Fixed by removing null keys. |
| 2026-06-17 | — | Reappeared. Fixed in profile config. |
| 2026-06-18 | — | Reappeared. Fixed in both main + profile configs. |
| 2026-06-23 | `oc_config_empty_section_fixloop_20260623` | Reappeared after gateway restart (fallback_model). |
| 2026-06-24 | `oc_config_empty_section_fixloop_20260624` | Reappeared after restart (max_concurrent_sessions, context_file_max_chars, max_in_progress_per_profile). |
| 2026-06-25 | `oc_config_empty_section_fixloop_20260625` | 7th occurrence. 4 null keys in main config. Marked resolved, fix applied. |
| 2026-06-25 | — | 8th occurrence avoided — configs verified clean by esc-run. |
| 2026-06-28 | — | **9th occurrence**. 3 null keys (max_concurrent_sessions, kanban.max_in_progress_per_profile, cron.max_parallel_jobs) removed. Pattern B RCA exists — escalated root cause. |

## Known Recurring Keys

- `max_concurrent_sessions` (top-level)
- `context_file_max_chars` (top-level)
- `max_in_progress_per_profile` (nested under `kanban`)
- `max_parallel_jobs` (nested under `cron`)
- `fallback_model` (top-level — usually YAML debris, not true fix-loop)

## Root Cause Hypothesis

Gateway config migration or startup template regenerates null keys on restart. The gateway writes default template values (null) when keys are missing from the active config, or a post-startup hook re-inserts them from a schema template.

## Fix Protocol

1. **Diagnose**: `grep -nE ': ($|null$)' <hermes-home>/config.yaml <hermes-home>/profiles/*/config.yaml`
2. **Verify with PyYAML**: `yaml.safe_load` + recursive null-key detection (catches both `key: null` and `key:` forms)
3. **Remove**: Use PyYAML `del config[key]` (preferred) or sed with pitfall awareness
4. **Verify-after**: Re-run grep to confirm all null keys gone from BOTH files
5. **For Pattern B fix-loops**: Apply the fix, BUT write escalation journal with `escalation_needed: true` for the architectural root cause

## Inactive Profile Exception

Null keys in inactive profiles (no `cron/jobs.json`, >90 days dormant) are legacy YAML debris. Do NOT escalate as `oc_config_empty_section`. Example: braun profile has 3 null keys but no cron jobs — Bucket C (ignore).

## References

- `references/fallback-model-null-yaml-debris-pattern.md` — distinguishing YAML debris from true fix-loop
- `references/escalation-runner-2026-06-24-2108.md` — YAML null-key detection pitfall (Form 1 vs Form 2)
- `references/escalation-runner-clean-verdict-pattern.md` — clean verdict decision tree
- SKILL.md "Config empty section: Tier 1 auto-fix vs Pattern B contradiction" — resolution: fix + escalate root cause