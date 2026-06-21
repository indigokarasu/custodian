# Escalation Runner 2026-06-18 09:25

**Run ID**: esc-run-20260618-0925
**Outcome**: 2 fixes applied, 0 new escalations

## Fixes Applied

### 1. `oc_provider_ovh_403_auth-20260618` — OVH/LLM7 provider 403

**Root cause:** `ovhcloud` and `llm7` providers in profile config.yaml had empty `api_key`. `fallback_providers` list also referenced them. Jobs with explicit `provider:openrouter` still hit OVH Kepler via fallback routing.

**Fix:** Removed both `ovhcloud` and `llm7` from `providers` and `fallback_providers`. Only `aion_labs` remains.

**Affected jobs:** `genie:update`, `soul:sync` (both have explicit provider=openrouter), `dispatch-email-15min` (provider:null, model:null — newly identified as affected)

### 2. `oc_checkpoint_store_git_corrupted-20260617` — Checkpoint store git corruption

**Root cause:** `checkpoints/store/.git` missing `refs/heads/` and `objects/`. 94+ errors/day since 2026-06-17T05:20.

**Fix:** Backed up corrupted .git to .git.bak, removed, ran `git init`. refs/heads/ and objects/ now exist.

## Non-Actionable

- `skill_library_stubs` (Tier 4, esc=false) — needs user confirmation
- `skill_hygiene_followup_20260601` (Tier 2, esc=false) — needs user confirmation

## Notes

- Profile path issues.jsonl had newer issues; commons copy was stale
- Both fixes followed existing documented patterns — no new technique needed
