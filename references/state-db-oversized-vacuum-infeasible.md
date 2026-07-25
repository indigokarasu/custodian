# state.db Oversized with VACUUM Infeasible at >80% Disk

## Summary

When `state.db` grows beyond ~1GB AND root filesystem usage exceeds 80%, VACUUM becomes infeasible due to temporary space requirements (~2x DB size). The only viable remediation is **batched message pruning** of old cron sessions.

## Error Signature

- `state.db` size: >1GB (observed: 14GB)
- Disk usage: >80% (observed: 84%, 80G/96G)
- WAL size: ~27MB (normal)
- Message count: 656,465 (observed)
- Free disk: < DB size (observed: 17GB free vs 14GB DB)

## Classification

- **Fingerprint:** `oc_state_db_oversized`
- **Tier:** 2 (operational maintenance, requires scheduling)
- **Transient:** NO — grows monotonically without pruning
- **Escalation:** Required when disk >80% AND DB >1GB

## Root Cause

`state.db` stores all session messages indefinitely. Cron jobs (140+ jobs running every few minutes) generate massive message volume. No automatic pruning is configured.

## VACUUM Feasibility Calculation

| Metric | Value | Threshold |
|--------|-------|-----------|
| DB size | 14 GB | — |
| Temp space needed | ~28 GB | 2x DB size |
| Free disk | 17 GB | < 28 GB → **INFEASIBLE** |
| Disk usage | 84% | >80% → **CRITICAL** |

**Verdict:** VACUUM will fail with "database or disk is full" mid-operation.

## Remediation: Batched Message Pruning

```sql
-- Delete messages from sessions older than 90 days
-- Run in chunks to avoid lock contention
DELETE FROM messages
WHERE session_id IN (
  SELECT id FROM sessions
  WHERE started_at < datetime('now', '-90 days')
  LIMIT 10000
);
```

**Procedure:**
1. Run DELETE in batches of 10,000 sessions (not messages — sessions table is smaller)
2. After each batch, check `sqlite3 state.db "PRAGMA freelist_count"` 
3. When freelist stabilizes, consider VACUUM if disk <70%
4. Monitor `df -h` after each batch

## Detection During Scans

**Deep scan Step 2 (Collect):**
<<<<<<< Updated upstream
- `du -sh <hermes-home>/profiles/indigo/state.db`
=======
- `du -sh ~/.hermes/profiles/indigo/state.db`
>>>>>>> Stashed changes
- `df -h /root`
- `sqlite3 state.db "SELECT COUNT(*) FROM messages"`

**Thresholds:**
- Tier 2 flag: DB >1GB AND disk >80%
- Tier 3 escalation: DB >5GB AND disk >85% (imminent OOM risk)

## Clean Verdict Impact

**A clean verdict (all job errors transient) does NOT mean system is healthy if state.db is oversized.** The deep scan clean verdict shortcut (Step 3 → silent) must be gated by:
- state.db < 1GB OR disk < 80%
- If oversized, must produce report with `oc_state_db_oversized` escalation

## Prevention

- Add cron job: `custodian:prune-state-db` running weekly
- Configure SQLite `PRAGMA auto_vacuum = INCREMENTAL` (requires rebuild)
- Set `sessions.auto_prune: true` with `retention_days: 90` in config.yaml (already set, verify working)

## Related Patterns

- `disk-compaction.md` — general disk cleanup procedures
- `system-maintenance.md` — proactive monitoring thresholds
- `journal-path-format-inconsistency.md` — journal gaps can indicate cron not running, which also means pruning not running

## Confidence Model

- `confidence_score`: 0.9 (clear threshold, deterministic remediation)
- `recommended_tier`: 2 (scheduled maintenance, not emergency)
- Fix-loop risk: LOW if pruning is scheduled; HIGH if ignored

## Key Lesson

**VACUUM is a luxury of disk headroom.** At >80% disk, the only tool that fits is DELETE. Plan pruning before you hit the wall.