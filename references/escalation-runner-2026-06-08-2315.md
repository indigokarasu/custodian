# Escalation Runner 2026-06-08 23:15Z — State.db VACUUM Success

**Key finding:** Previous FTS-bloat theory for state.db was disproven.

## What happened
- state.db was 13.1 GB with 6.96 GB freelist (53% wasted space)
- VACUUM was feasible: 17.1 GB free disk, VACUUM needs ~13.1 GB temp
- VACUUM completed in 97 seconds, reclaimed 6.96 GB
- DB went from 13.1 GB → 6.2 GB
- Disk went from 83% (18 GB free) → 75% (25 GB free)

## System state
- 12,479 sessions, 84,081 messages
- 4,196 sessions older than 30 days
- 4,135 sessions already archived
- Messages older than 30 days: 0 (all recent)
- Messages older than 7 days: 78,713 of 84,081 (93%)
- Space breakdown: system_prompt = 4.82 GB, message content = 0.18 GB
- Freelist was entirely from old session rows (not FTS indexes)

## What this disproves
`disk-compaction.md` stated (from June 3): "FTS trigram indexes retain size even after VACUUM. Accept the size as operational cost."
This was wrong. The space was dead rows in the sessions table (old system_prompt data), not FTS bloat. VACUUM reclaimed it fully.

## Action items for future
- `disk-compaction.md` FTS theory needs correction
- VACUUM feasibility threshold: free_disk >= db_size (not 2x as previously stated for this DB)
- state.db main consumer is system_prompt column, not FTS indexes
- Sessions older than 30 days: 4,196 carrying 4.69 GB of system_prompt data
  - Pruning old sessions (not just messages) would yield significant savings
  - Session pruning requires user confirmation (don't auto-prune)

## Tool quirks observed
- CWD was non-standard (`/root/hermes-telegram-artifacts`) — use absolute paths for all terminal calls
- `read_file` returned "File not found" for files that `ls` confirmed existed; use `terminal(command="cat /path")` as fallback
- Empty scan journals (0 bytes) should be skipped when scanning for recent files
