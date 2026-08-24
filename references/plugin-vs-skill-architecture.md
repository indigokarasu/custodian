# Plugin vs Skill Architecture — Custodian

## Overview

Custodian v2.0.0+ runs as a **Hermes plugin** (active operational code) while the **skill** is retained as a reference copy for backward compatibility and documentation.

## Locations

| Component | Path | Purpose |
|-----------|------|---------|
| Plugin | `~/.hermes/plugins/custodian/` | Active code — hooks, tools, slash commands loaded by gateway |
| Skill | `$HERMES_HOME/../<profile>/skills/ocas-custodian/` | Reference copy — SKILL.md + references + scripts for docs |

## Version Tracking

| Source | How to Check |
|--------|--------------|
| Plugin (active) | `cat ~/.hermes/plugins/custodian/hermes_custodian_plugin/__init__.py \| grep __version__` or `cd ~/.hermes/plugins/custodian && git log -1 --oneline` |
| Skill (reference) | `head -30 $HERMES_HOME/../indigo/skills/ocas-custodian/SKILL.md \| grep version` |

## Update Procedure

**Actual update (run in plugin directory):**
```bash
cd ~/.hermes/plugins/custodian && git pull
```

**What the cron job does:** The `custodian:update` cron job (schedule `0 7 * * *`) runs the skill's `custodian.update` command which returns a JSON note: `{"status": "update", "note": "Self-update from GitHub — use 'git pull' in plugin directory"}` — it does NOT perform the git pull itself.

**What the slash command does:** `/custodian update` returns the same JSON note.

## Editable Install

The plugin is installed via `pip install -e ~/.hermes/plugins/custodian/` (see `pyproject.toml` entry point `custodian = "hermes_custodian_plugin"`). The active code is the plugin directory itself — changes to files in `~/.hermes/plugins/custodian/hermes_custodian_plugin/` take effect on next gateway reload.

## Skill Directory Recovery

If the skill directory is missing (e.g., deleted by a faulty self-update):

1. Find source URL from cron output logs:
   ```bash
   grep -r "source:" $HERMES_HOME/../indigo/cron/output/*/*.md | grep custodian
   ```
2. Clone and restore:
   ```bash
   git clone <source_url> /tmp/custodian-src
   mkdir -p <skill_dir>/references <skill_dir>/scripts
   cp /tmp/custodian-src/SKILL.md <skill_dir>/
   cp /tmp/custodian-src/references/* <skill_dir>/references/
   cp /tmp/custodian-src/scripts/* <skill_dir>/scripts/
   ```
3. Verify: `head -5 <skill_dir>/SKILL.md`

The canonical source is always in the SKILL.md frontmatter `source:` field. Apply this pattern to ANY missing OCAS skill with a `source:` URL.

## Prevention

Self-update should never `rm -rf` the skill directory — only `git fetch`/`git merge` within it. Add a pre-update backup:
```bash
cp -r <skill_dir> /tmp/custodian-backup-$(date +%s)
```
before any git operations.