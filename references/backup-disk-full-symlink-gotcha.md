# Backup disk-full + symlink-size gotcha

## Symptom
<<<<<<< Updated upstream
`backup_all_hermes_data.sh` (canonical: `<repo-root>/scripts/backup_all_hermes_data.sh`)
=======
`backup_all_hermes_data.sh` (canonical: `<fs-root>/indigo-repo/scripts/backup_all_hermes_data.sh`)
>>>>>>> Stashed changes
aborts with `cp: error copying '.../state.db' ...: No space left on device` and **never
reaches the GitHub LFS push** — the actual deliverable.

## Root cause
<<<<<<< Updated upstream
1. `state.db` is a **symlink** (`<hermes-home>/state.db` → `profiles/indigo/state.db`) and
=======
1. `state.db` is a **symlink** (`~/.hermes/state.db` → `profiles/indigo/state.db`) and
>>>>>>> Stashed changes
   ~12G. The backup script copies it **locally only** (never to LFS), but the system disk
   (96G) has no room for a 12G local copy. With `set -euo pipefail`, the failed `cp` aborts
   the whole script before the LFS commit/push step runs.
2. A failed run leaves a **partial `state.db` copy** in `<fs-root>/backup/<ts>/` that consumes
   the very space you just tried to free — so a naive retry re-fills the disk.

## THE SHARP GOTCHA: `stat -c%s` on a symlink
`stat -c%s /path/to/symlink` returns the **symlink path length (e.g. 38 bytes)**, NOT the
target file size. A free-space guard built on it computes `38 < avail` → guard passes →
`cp` attempts the full 12G → disk re-fills. This silently defeats any "skip if no room"
logic.

**Fix the size check — dereference first:**
```bash
<<<<<<< Updated upstream
STATE_SRC="<hermes-home>/state.db"
=======
STATE_SRC="~/.hermes/state.db"
>>>>>>> Stashed changes
STATE_REAL=$(readlink -f "$STATE_SRC" 2>/dev/null || echo "$STATE_SRC")
STATE_SIZE=$(stat -L -c%s "$STATE_REAL" 2>/dev/null || echo 0)   # -L follows symlink
AVAIL=$(df --output=avail -B1 <fs-root> 2>/dev/null | tail -1 | tr -d ' ')
if [ -n "$AVAIL" ] && [ "$STATE_SIZE" -gt 0 ] && [ "$AVAIL" -gt $((STATE_SIZE * 11 / 10)) ]; then
    cp "$STATE_SRC" "$BACKUP_DIR/state.db"
else
    echo "SKIP state.db: needs ~$((STATE_SIZE/1024/1024))M, only ${AVAIL:-?} bytes free (local-only, not pushed to LFS)"
fi
```
Alternatives: `du -b "$STATE_REAL"` for apparent size, or `stat -L` directly on the symlink
path (GNU stat follows the link for `-L`).

## Fix pattern that worked
1. **Remove the partial backup dir first** (reclaim the space the failed run ate):
   `rm -rf <fs-root>/backup/<failed-ts>`.
2. **Make the local-only oversized copy space-aware** (the patch above) so one 12G local
   copy can never brick the entire backup (incl. the LFS push).
3. Re-run. The LFS-pushed files are all small (chronicle 24M, chroma 253M, mempalace tar
   ~125M, transactions/styx, taste JSONL) and fit easily once the 12G local copy is skipped.

## Verification
After push, confirm objects actually landed on the remote:
```bash
cd <fs-root>/indigo-repo && git lfs push --dry-run origin main   # empty output = all on remote
git lfs ls-files | grep -E 'chronicle|chroma|mempalace|transactions|styx'
df -h <fs-root>   # should not be at 100%
```

## Why this lives in the backup skill
The canonical script is the one invoked by the daily backup cron. Patching it to skip a
local-only copy that the filesystem cannot hold is the durable fix; the symlink `stat`
gotcha is the non-obvious trap that turns a correct-looking guard into a disk-filling bug.
Disk was at 100% → 91% after the space-aware skip + partial cleanup.

## Reproduction recipe
```bash
# Force the trap: a guard using bare stat on the symlink
<<<<<<< Updated upstream
AVAIL=$(df --output=avail -B1 <fs-root> | tail -1 | tr -d ' ')
S_BAD=$(stat -c%s <hermes-home>/state.db)        # -> 38 (symlink length), NOT 12G
[ "$S_BAD" -gt 0 ] && [ "$AVAIL" -gt $((S_BAD*11/10)) ] && echo "WOULD COPY (WRONG)"
# Correct:
S_OK=$(stat -L -c%s "$(readlink -f <hermes-home>/state.db)")   # -> 12247752704
```
=======
AVAIL=$(df --output=avail -B1 /root | tail -1 | tr -d ' ')
S_BAD=$(stat -c%s ~/.hermes/state.db)        # -> 38 (symlink length), NOT 12G
[ "$S_BAD" -gt 0 ] && [ "$AVAIL" -gt $((S_BAD*11/10)) ] && echo "WOULD COPY (WRONG)"
# Correct:
S_OK=$(stat -L -c%s "$(readlink -f ~/.hermes/state.db)")   # -> 12247752704
```
>>>>>>> Stashed changes
