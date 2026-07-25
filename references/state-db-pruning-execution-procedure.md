# state.db Message Pruning — Execution Procedure (2026-07-06)

## Summary

Executed batched pruning of 408,037 messages (62% reduction) from `state.db` messages table where `timestamp < 1782111600` (2026-06-22). Combined with stale snapshot removal (14GB) and WAL checkpoint (5.2GB → 78KB), enabled successful VACUUM. Result: state.db 15GB → 12GB, disk usage 89% → 66%.

## Preconditions

- state.db size: 15GB (14GB main + 1GB WAL)
- Disk usage: 89% (80G/96G used)
- Free disk: ~16GB (< state.db size, VACUUM infeasible)
- Message count: 657,167
- Date range: 2026-06-03 to 2026-07-06 (33 days)

## Step-by-Step Procedure

### 1. Free Disk Space First (Critical)

```bash
# Remove stale state-snapshots backup (14GB!)
<<<<<<< Updated upstream
rm -rf <hermes-home>/profiles/indigo/state-snapshots/20260706-110009-pre-update

# Checkpoint WAL to reclaim space
sqlite3 <hermes-home>/profiles/indigo/state.db "PRAGMA wal_checkpoint(TRUNCATE);"
=======
rm -rf ~/.hermes/profiles/indigo/state-snapshots/20260706-110009-pre-update

# Checkpoint WAL to reclaim space
sqlite3 ~/.hermes/profiles/indigo/state.db "PRAGMA wal_checkpoint(TRUNCATE);"
>>>>>>> Stashed changes
# WAL: 5.2GB → 78KB, disk usage: 89% → ~74%
```

**Result:** Freed ~19GB. VACUUM now feasible (free disk > db size).

### 2. Batched Message Deletion by Timestamp

**Why timestamp not session_id:** Direct timestamp filter on `messages.timestamp` is faster than JOIN with sessions. The `messages` table has an index on `timestamp` (via `idx_messages_session_active`).

```python
import sqlite3, time

<<<<<<< Updated upstream
conn = sqlite3.connect('<hermes-home>/profiles/indigo/state.db', timeout=300)
=======
conn = sqlite3.connect('~/.hermes/profiles/indigo/state.db', timeout=300)
>>>>>>> Stashed changes
conn.execute('PRAGMA journal_mode=WAL')
conn.execute('PRAGMA synchronous=NORMAL')
conn.execute('PRAGMA busy_timeout=300000')
conn.execute('PRAGMA recursive_triggers=OFF')

cutoff_ts = 1782111600  # 2026-06-22 00:00:00 UTC
batch_size = 5000
total_deleted = 0

while True:
    cursor = conn.execute(f"""
        DELETE FROM messages 
        WHERE timestamp < {cutoff_ts}
        AND id IN (
            SELECT id FROM messages 
            WHERE timestamp < {cutoff_ts} 
            ORDER BY id 
            LIMIT {batch_size}
        )
    """)
    deleted = cursor.rowcount
    total_deleted += deleted
    conn.commit()
    print(f"Deleted {deleted} messages (total: {total_deleted})")
    if deleted < batch_size:
        break
    time.sleep(0.2)

conn.close()
```

**Result:** 408,037 messages deleted in ~81 batches. ~5 min elapsed.

### 3. VACUUM

```bash
<<<<<<< Updated upstream
sqlite3 <hermes-home>/profiles/indigo/state.db "VACUUM;"
=======
sqlite3 ~/.hermes/profiles/indigo/state.db "VACUUM;"
>>>>>>> Stashed changes
# Took ~20 min (1187 seconds)
# state.db: 15GB → 12GB
```

### 4. Final WAL Checkpoint

```bash
<<<<<<< Updated upstream
sqlite3 <hermes-home>/profiles/indigo/state.db "PRAGMA wal_checkpoint(TRUNCATE);"
=======
sqlite3 ~/.hermes/profiles/indigo/state.db "PRAGMA wal_checkpoint(TRUNCATE);"
>>>>>>> Stashed changes
# WAL: 78KB → 4KB
```

## Key Learnings

| Step | Why It Matters |
|------|----------------|
| Delete snapshot FIRST | 14GB freed instantly; VACUUM requires free_disk >= db_size |
| Checkpoint WAL BEFORE prune | Reduces WAL size, prevents WAL growth during deletes |
| `PRAGMA recursive_triggers=OFF` | Disables FTS triggers during bulk delete — massive speedup |
| `PRAGMA busy_timeout=300000` | Prevents lock contention timeouts during long operation |
| Batch size 5000 | Balances transaction overhead vs lock duration |
| Delete by `messages.timestamp` directly | Avoids JOIN with sessions; uses existing index |

## What Didn't Work

- **DELETE by session_id JOIN** — Too slow, full table scan on sessions
- **Single large DELETE** — Locked database for minutes, blocked cron jobs
- **VACUUM before freeing space** — Failed with "database or disk is full" (error 13)

## Timing

| Phase | Duration |
|-------|----------|
| Snapshot removal + WAL checkpoint | 30 sec |
| Batched DELETE (408K messages) | 5 min |
| VACUUM | 20 min |
| Final WAL checkpoint | 5 sec |
| **Total** | **~25 min** |

## Post-Conditions

- state.db: 12GB (was 15GB)
- Messages: 249,130 (was 657,167)
- Disk usage: 66% (was 89%)
- WAL: <100KB
- `PRAGMA page_count`: 2,936,362 (was 3,665,100)

## Automation Recommendation

Add weekly cron job `custodian:prune-state-db`:
```bash
# Keep 14 days of messages (aligns with recovery contract 90-day error retention)
DELETE FROM messages WHERE timestamp < strftime('%s', 'now', '-14 days');
VACUUM;
PRAGMA wal_checkpoint(TRUNCATE);
```

Run during low-activity window (e.g., 03:00 UTC). Monitor disk usage before/after.