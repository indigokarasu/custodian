# Skill Library Hygiene — Custodian Procedures

## Overview

Custodian is responsible for detecting and cleaning up skill library bloat. This is a distinct task from per-skill artifact cleanup (finch) and operational failure diagnosis (custodian's primary role).

## Detection Heuristics

### Stub Directories (Failed Marketplace Installs)

Any directory in `~/.hermes/skills/` with a `DESCRIPTION.md` but no `SKILL.md`:

```bash
for d in ~/.hermes/skills/*/; do
  name=$(basename "$d")
  [[ "$name" == .* ]] && continue
  [[ ! -f "$d/SKILL.md" ]] && echo "STUB: $name"
done
```

**Exception**: Umbrella directories containing valid sub-skills (check before removing).

### Nested Git Repos

```bash
find ~/.hermes/skills/ -maxdepth 3 -name ".git" -type d
```

Skills with embedded `.git` directories waste space and cause confusion.

### Nested Skills

Skills at `~/.hermes/skills/<umbrella>/<skill>/` that should be at `~/.hermes/skills/<skill>/`:

```bash
find ~/.hermes/skills/ -maxdepth 3 -name "SKILL.md" | while read f; do
  skill_dir=$(dirname "$f")
  skill_name=$(basename "$skill_dir")
  [[ ! -f ~/.hermes/skills/$skill_name/SKILL.md ]] && echo "NESTED: $f"
done
```

### Orphaned Files

Non-directory entries in skills root:

```bash
for f in ~/.hermes/skills/*; do [[ ! -d "$f" ]] && echo "ORPHAN: $(basename "$f")"; done
```

## Cleanup Actions

1. **Remove stubs**: `rm -rf ~/.hermes/skills/<stub_name>` — safe, no executable content
2. **Remove nested .git**: `find ~/.hermes/skills/ -maxdepth 3 -name ".git" -type d -exec rm -rf {} +`
3. **Promote nested skills**: Copy to top-level, verify, remove nested copy
4. **Relocate orphaned files**: `.md` → `.references/`, `.py` → relevant skill's `scripts/`
5. **Rebuild index**: `hermes skills audit`

### Stale Git Lock Files

```bash
find <hermes-root>/checkpoints/ -name "*.lock" -type f -mtime +1 -empty
```

Stale git lock files in `checkpoints/store/indexes/` persist after crashed git operations.
These are 0-byte files that block all subsequent `git add -A` in checkpoint_manager.
Safe to remove (they are artifacts of interrupted git operations, not active locks).

## When to Run

Include skill library hygiene in the **deep scan** (every 6 hours) as step 0 before other checks. Report findings but do NOT auto-remediate — skill library changes require user confirmation.

## Expected State

- 60-75 skills for a personal agent with OCAS
- No `.git` directories inside skill folders
- No files (non-directory entries) in skills root except hidden directories
- 5 umbrella dirs (`infrastructure`, `productivity`, `private`, `autonomous-ai-agents`, `creative`) contain at least one valid sub-skill
- ~19 stub directories (failed marketplace installs, no SKILL.md): apple, data-science, diagramming, domain, email, gaming, gifs, github, inference-sh, mcp, media, mlops, note-taking, ocas-critique, private, productivity, research, smart-home, social-media — **requires user confirmation before removal**