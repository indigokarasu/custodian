# Checkpoint Store Git Corruption Pattern

## Symptom
`checkpoint_manager` logs errors like "git command failed" or "refs/heads not found" on every checkpoint write. Frequency: 94+ errors/day.

## Root Cause
`checkpoints/store/.git` exists but is missing standard git directories:
- `refs/heads/` — missing
- `objects/` — missing

The `.git` directory may still contain: `HEAD`, `config`, `description`, `hooks/`, `indexes/`, `info/`, `packed-refs`, `projects/`

## How It Happens
- Store directory moved/copied without proper git migration
- Git process killed during init
- Cleanup script removes `.git/objects/` or `.git/refs/` but not `.git/` itself

## Fix

```bash
STORE_DIR="<hermes-home>/checkpoints/store"
GIT_DIR="$STORE_DIR/.git"

# 1. Back up corrupted .git
cp -r "$GIT_DIR" "$GIT_DIR.bak"

# 2. Remove corrupted .git
rm -rf "$GIT_DIR"

# 3. Reinitialize
cd "$STORE_DIR" && git init

# 4. Verify
ls "$GIT_DIR/refs/heads"  # should exist
ls "$GIT_DIR/objects"     # should exist
```

Store contents (`HEAD`, `config`, `indexes/`, `packed-refs`, `projects/`) are preserved across the reinit. Checkpoint manager rebuilds refs on next write.

## Occurrence
- First seen: 2026-06-17T05:20
- Fixed: 2026-06-18T09:25 by escalation runner
