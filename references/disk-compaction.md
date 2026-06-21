# Disk Compaction — Safe Cleanup Procedures

## When to Run
- Disk usage exceeds 80% (`df -h /` shows Use% > 80%)
- User reports "disk full" or "running out of space"
- Proactive check during custodian deep scan

## Diagnostic Sequence

### 1. Overall usage
```bash
df -h /
```

### 2. Top-level breakdown
```bash
du -sh <hermes-root>/* | sort -rh | head -30
```

### 3. Drill into top consumers
```bash
# Sessions
du -sh <hermes-root>/sessions && find <hermes-root>/sessions -name "*.json" | wc -l

# State snapshots
du -sh <hermes-root>/state-snapshots && ls <hermes-root>/state-snapshots/

# Cron output
du -sh <hermes-root>/cron/output && find <hermes-root>/cron/output -type f | wc -l

# Logs
du -sh <hermes-root>/logs && find <hermes-root>/logs -name "*.log" -exec du -sh {} \;

# Backups
du -sh <hermes-root>/backups && ls -lh <hermes-root>/backups/
```

### 4. State DB analysis
```bash
# Quick size check
ls -lh <hermes-root>/state.db*

# Message count and age distribution
sqlite3 <hermes-root>/state.db "SELECT COUNT(*) FROM messages;"
sqlite3 <hermes-root>/state.db "SELECT COUNT(*) FROM messages WHERE timestamp < (strftime('%s', 'now') - 14*86400);"
sqlite3 <hermes-root>/state.db "SELECT COUNT(*) FROM sessions;"

# WAL size
ls -sh <hermes-root>/state.db-wal 2>/dev/null || echo "No WAL file"
```

**⚠️ IMPORTANT**: PRAGMA queries on 15GB state.db may take 30-60s each. Use `execute_code` with python3+sqlite3 for complex queries, not `terminal()` which times out at 30s.

## State DB Bloat — Message Pruning Procedure

When state.db exceeds expected size (>1GB per 100K messages is normal due to FTS trigram indexing, but >10GB warrants pruning):

### Step 1: Assess
```bash
# Check free disk space first — VACUUM needs ~2x DB size temporarily
df -h /
# If insufficient free space, skip VACUUM and do batch DELETE only
```

### Step 2: Batch delete old messages
Use batch deletion to avoid timeout on large tables:
```bash
# Delete in batches of 10,000
sqlite3 <hermes-root>/state.db "DELETE FROM messages WHERE timestamp < (strftime('%s', 'now') - 14*86400) AND rowid IN (SELECT rowid FROM messages WHERE timestamp < (strftime('%s', 'now') - 14*86400) LIMIT 10000);"
# Repeat until count returns 0
sqlite3 <hermes-root>/state.db "SELECT COUNT(*) FROM messages WHERE timestamp < (strftime('%s', 'now') - 14*86400);"
```

**Why batch?** A single `DELETE FROM messages WHERE timestamp < ...` on 115K+ rows times out in terminal. Batches of 10K complete in seconds.

### Step 3: VACUUM (only if disk space allows)
```bash
# VACUUM needs ~2x the DB size in free space temporarily
# On a 15GB DB with 28GB free, VACUUM will fail with "database or disk is full"
# Prune messages first (Step 2) until DB is small enough, or free disk space elsewhere
sqlite3 <hermes-root>/state.db "VACUUM;"
```

### Step 4: FTS rebuild (if VACUUM didn't shrink)
```bash
# FTS trigram indexes retain size even after VACUUM
sqlite3 <hermes-root>/state.db "INSERT INTO messages_fts(messages_fts) VALUES('rebuild');"
```

**⚠️ Reality check (2026-06-03, corrected 2026-06-08):** The FTS trigram theory was wrong. VACUUM on 2026-06-08 succeeded, reclaiming 6.96 GB (53% of the DB was freelist). The primary space consumer was dead rows in the sessions table (old system_prompt data = 4.82 GB), NOT FTS indexes. VACUUM feasibility: free_disk >= db_size is sufficient (not 2x). On a 13.1 GB DB with 17.1 GB free, VACUUM completed in 97 seconds.

## Safe Cleanup Actions (in confidence order)

### Tier 1 — Zero Risk (99% confidence)

**1. Delete pre-update backup zips** (saves ~3.5 GB each)
```bash
ls -lh <hermes-root>/backups/
# Remove .zip files from already-completed updates
rm -f <hermes-root>/backups/pre-update-*.zip
```
- Created by `custodian:update` before applying updates
- Safe to remove once update is confirmed working (verify no rollback needed)
- Files: `pre-update-YYYY-MM-DD-HHMMSS.zip`

**2. Delete old state snapshots** (saves ~14 GB per snapshot)
```bash
rm -rf <hermes-root>/state-snapshots/<old-snapshot-dir>
```
- The live state.db is the source of truth
- Snapshots are pre-update rollback copies, not read at runtime
- Check `manifest.json` inside snapshot to confirm it's a pre-update backup
- Keep the most recent snapshot only if desired

**3. Rotate logs** (saves ~15-20 MB)
```bash
# Compress logs >7 days
find <hermes-root>/logs -name "*.log" -mtime +7 -exec gzip {} \;
# Delete compressed logs >30 days
find <hermes-root>/logs -name "*.log.gz" -mtime +30 -delete
```

### Tier 2 — Low Risk (95% confidence)

**4. Compress cron output** (saves ~130 MB)
```bash
# Compress files >7 days
find <hermes-root>/cron/output -type f -mtime +7 -exec gzip {} \;
# Delete compressed files >30 days
find <hermes-root>/cron/output -type f.gz -mtime +30 -delete
```
- Cron jobs write new output per run; they never read old output
- Use `zgrep` if you need to search compressed files

### Tier 3 — Moderate Risk (70% confidence, verify first)

**5. Session JSON files** (saves ~9.5 GB)
- Location: `<hermes-root>/sessions/session_*.json` (15,000+ files)
- These are 1:1 duplicates of state.db rows (same session_id, same content)
- session_search tool reads from state.db, NOT from JSON files
- **Before deleting**: gzip first, wait a few days, confirm nothing breaks
```bash
# Compress older than 7 days
find <hermes-root>/sessions -name "*.json" -mtime +7 -exec gzip {} \;
# Delete .gz files after confirming system is stable (days later)
find <hermes-root>/sessions -name "*.json.gz" -mtime +14 -delete
```
- The `ghost_session_prune_v1` meta key in state.db suggests Hermes has a
  built-in pruning mechanism — the JSON files may be unintended duplicates
- 30% uncertainty: compiled hermes-agent source is unreadable; cannot confirm
  zero dependencies on these files

## Do NOT Touch Without Deeper Analysis

- **state.db migration/refactoring**: Touching the core session/message schema risks breaking session_search, finch, corvus, and all session-dependent skills. Requires testing against the actual compiled application.
- **node/ directory**: 2.0 GB including node_modules. Only remove if confirmed as a build artifact no longer needed at runtime.
- **checkpoints/**: 137 MB / 2,753 files. Retention policy needed; don't blindly delete.
- **commons/data**: 489 MB. Contains system data; review contents before any action.

## Proposal Format

When presenting disk cleanup options to the user:
1. Show current usage (df -h) and breakdown (du -sh)
2. List actions in confidence tiers (99% / 95% / 70%)
3. State savings per action
4. Let user decide which tier to execute
5. Default to "safe only" unless user explicitly asks for higher-risk options

## Known Disk Layout (June 2026)

| Path | Size | Notes |
|---|---|---|
| state.db | 6.2 GB | 12,479 sessions, 84,081 messages. VACUUM'd 2026-06-08 (reclaimed 6.96 GB). Main consumer: system_prompt column (4.82 GB). Old sessions >30 days: 4,196. Archived: 4,135. |
| sessions/ | 9.5 GB | 15,026 JSON files — redundant with state.db |
| state-snapshots/ | 1.6 GB | Single pre-update snapshot from Jun 8 |
| commons/ | 1.2 GB | journals, data |
| cron/output | ~167 MB | Never read after write |
| logs/ | ~45 MB | Rotate as needed |
