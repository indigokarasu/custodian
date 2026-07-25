# Skill Reference Path Mismatch Pattern

**Fingerprint:** `oc_skill_reference_path_mismatch`
**Tier:** 2 (requires investigation, not auto-fixed)
**First observed:** 2026-06-17 (light scan)

## Problem

A skill's agent-side code attempts to read reference/support files from:
```
<hermes-home>/commons/data/<skill-name>/references/
```

But the files actually exist at:
```
<hermes-home>/profiles/<profile>/skills/<skill-name>/references/
```

This causes `File not found` errors when the agent executes the skill.

## Root Cause

1. **Skill development convention**: Skills are developed with references in `skills/<skill>/references/`
2. **Profile isolation**: When installed under a profile, the skill lives at `profiles/<profile>/skills/<skill>/`
3. **Agent runtime**: The skill's code (or update scripts) hardcodes or derives the wrong base path
4. **Update script mismatch**: `update_skill.sh` pulls to global `<hermes-home>/skills/` not profile dir

## Observed Instance: `ocas-spot`

**Job:** `spot:update` (77251188598b)
**Error in logs:**
```
File not found: <hermes-home>/commons/data/ocas-spot/references/cron-sweep-pattern.md
File not found: <hermes-home>/commons/data/ocas-spot/references/journal-schema.md
```

**Actual location:** `<hermes-home>/profiles/indigo/skills/ocas-spot/references/`

**Update script issue:** `update_skill.sh` targets `<hermes-home>/skills/ocas-spot/` (global) but profile uses `<hermes-home>/profiles/indigo/skills/ocas-spot/`

## Detection

During log scanning, watch for:
- `File not found: <hermes-home>/commons/data/<skill>/references/`
- Skill name in path matches an installed skill
- References directory exists in profile skills dir but not in commons/data

## Resolution Options

| Approach | Pros | Cons |
|----------|------|------|
| Fix skill code to use profile-aware path | Correct, durable | Requires skill modification |
| Symlink commons/data → profile skills dir | Quick fix | Fragile, breaks on profile switch |
| Update `update_skill.sh` to target profile dir | Fixes update flow | Doesn't fix runtime reference reads |
| Standardize on `$HERMES_HOME/skills/` convention | Consistent | Requires ecosystem migration |

## Recommended Fix

1. **Immediate**: Update skill's reference-reading code to use `skill_view()` or derive path from `$HERMES_HOME`
2. **Systemic**: Update `update_skill.sh` to accept profile argument or detect profile from `$HERMES_HOME`
3. **Pattern**: Add to custodian scan — check for `commons/data/<skill>/references` access attempts

## Related Patterns

- `oc_cron_script_path_security_block` — script path must match `$HERMES_HOME/scripts/`
- `oc_plugin_init_missing_noise` — plugin discovery checks both profile and system paths
- `chronicle-plugin-dirs-empty-pattern` — empty plugin dirs in wrong location

## Observed Instance: `memory-system-design` (2026-06-18)

**Jobs:** `elephas:deep`, `elephas:update`, `elephas:ingest` all reference `skill: memory-system-design`
**Expected path:** `<hermes-home>/profiles/indigo/skills/memory-system-design/`
**Actual location:** `<hermes-home>/profiles/indigo/skills/infrastructure/memory-system-design/`

**Symptoms:**
- `skill_view(name='memory-system-design')` may fail or return wrong content
- Jobs execute successfully via prompt-based execution (not skill-referenced)
- Stale scheduler state because skill path resolution interferes

**Root Cause:** Job JSON `skill` field set to bare name (`memory-system-design`) but skill is organized under `infrastructure/` subdirectory. The Hermes skill loader may not find it by bare name.

**Resolution Options:**
1. Create symlink: `ln -s infrastructure/memory-system-design memory-system-design`
2. Update job `skill` field to `infrastructure/memory-system-design`
3. Move skill to bare path (may conflict with infrastructure organization)

**Note:** This is distinct from the code-path mismatch (agent code reading wrong path). Here the JOB's `skill` field references a path that doesn't resolve in the skills directory tree.

---

## Pitfall: Sed multiline delete in YAML

When using `sed -i '/pattern/{N;/other/d}'` to remove entries from config.yaml, the command deletes the matched line AND the next line if it matches `other`. This can also remove adjacent indented provider entries that share the matched pattern. Always run `grep` after sed to verify the resulting file state. (2026-06-18)

## Prevention

When creating/updating skills:
- Use `skill_view(name, file_path)` for reading references (handles path resolution)
- Avoid hardcoding `<hermes-home>/commons/data/` or `<hermes-home>/skills/`
- Derive paths from `$HERMES_HOME` or use Hermes-provided skill resolution

## Escalation

Tag journal `escalation_needed: true` — requires Forge or skill author to fix code path.