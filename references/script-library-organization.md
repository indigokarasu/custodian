# Script Library Organization Standards

## Principles

1. **Scripts live in skills**: Every script belongs in its parent skill's `scripts/` directory. The `~/.hermes/scripts/` directory should contain ONLY symlinks pointing into skill `scripts/` dirs and truly shared infrastructure with no single parent skill.

2. **Naming**: Shared libraries must have descriptive names that indicate scope/purpose. Avoid generic names like `google_auth.py` that create ambiguity. Use suffixes like `_mcp` to indicate the auth mechanism.

3. **Update scripts**: Each skill's update mechanism lives in `skills/<skill>/scripts/update.sh`. The `~/.hermes/scripts/update_<skill>.sh` entry is always a symlink — never a regular file or wrapper script.

4. **Symlinks over wrappers**: When cron jobs need scripts in `~/.hermes/scripts/`, create symlinks directly to the skill script. Do not create wrapper files that call the skill script.

5. **Dead code cleanup**: When replacing an old method/script, search for ALL imports/references to the old name across all skill scripts AND `~/.hermes/scripts/`. Delete the old file AND any stale references. Verify with a second sweep — the first pass always misses something.

6. **Cron compatibility**: The cron system requires scripts at relative paths under `~/.hermes/scripts/`. Symlinks are the bridge — the canonical copy lives in the skill dir.

## Anti-patterns

- Regular files in `~/.hermes/scripts/` that could live in a skill
- Wrapper files that just call through to a skill script (use symlinks instead)
- Stale files left behind after a rename/move
- Scripts with generic names that don't indicate their purpose
- `update_*.sh` as regular files instead of symlinks