# Pre-update State Snapshot Disk Refill

## Pattern
`custodian:update` (self-update) and similar update flows create a pre-update
snapshot directory at:

```
<hermes-home>/profiles/<profile>/state-snapshots/<YYYYMMDD-HHMMSS>-pre-update/
```

containing a **full copy of `state.db`** (~= live `state.db` size). For this
profile that is ~12 GB. The snapshot is a backup, not live data.

If the snapshot is NOT pruned after a successful update, it sits on disk alongside
the live `state.db` and **refills the filesystem**, negating any message-pruning
done to shrink `state.db` itself.

## Confirmed instance (2026-07-07)
- 2026-07-06 deep scan pruned `state.db` and claimed disk dropped to **66%**.
- By 2026-07-07T16:00Z disk was back to **91%** (9.3 GB free) — a ~25-point climb in ~14h.
- Root cause: `state-snapshots/20260707-110009-pre-update/state.db` = **12.2 GB**.
- Compounding consumers: live `state.db` = 12.3 GB, `chronicle.db` = 6.4 GB,
  Ollama model blobs (~15 GB+).
- The `oc_state_db_oversized` issue had been marked *resolved* (false resolution —
  the prune never held because the snapshot refilled the disk).

## Detection
```bash
du -sh <hermes-home>/profiles/<profile>/state-snapshots
# flag any *-pre-update/ dir > 1 GB
find <hermes-home>/profiles/<profile>/state-snapshots -maxdepth 1 -name '*-pre-update' -exec du -sh {} +
```

## Mitigation
- Once the update is verified successful, remove the pre-update snapshot:
  `rm -rf <hermes-home>/profiles/<profile>/state-snapshots/<ts>-pre-update`
- This is a backup, safe to prune post-success. Per skill hygiene rules, note the
  deletion in the journal; user confirmation is good practice but not required for a
  self-created pre-update backup.
- After removing, re-check disk %. If still >80%, proceed to message pruning and
  `chronicle.db` compaction.

## Related
- `oc_state_db_oversized` (Tier 2). Threshold: flag when >1 GB AND disk >80%.
- The `kanban dispatcher: tick failed ... sqlite3.OperationalError: disk I/O error`
  seen at high disk % is consistent with this pressure (all kanban DBs passed
  `PRAGMA integrity_check` — transient FS stress, not corruption).
