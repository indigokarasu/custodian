# Elephas Ladybug DB Corruption — 2026-06-18

## Symptom
The Ladybug DB file at `/root/commons/db/ocas-elephas/chronicle.lbug` (13MB) was found to contain **raw thermal camera data** (DIY-Thermocam Lepton 2.x format) instead of a valid Ladybug database. SQLite reports "file is not a database." `real_ladybug` segfaults (exit 139) when attempting to open it.

## Root Cause
The file was overwritten by a misconfigured process that wrote binary sensor data to the wrong path. The file's magic bytes match FLIR/Lepton thermal imaging format. The WAL file (182 bytes) is just a header — no valid WAL data.

## Recovery
- **Backup available**: `/root/backups/chronicle.lbug` (15MB, last backup 2026-06-18 06:12) — verified queryable
- **Profile copy**: `<hermes-home>/commons/db/ocas-elephas/chronicle.lbug` (15MB, locked by running gateway — 5.8MB WAL)
- **Restoration**: `cp /root/backups/chronicle.lbug /root/commons/db/ocas-elephas/chronicle.lbug` (requires stopping gateway or bridge first to release lock)

## Workaround: Run Scan Against Backup
When the live DB is locked or corrupted, run the deep scan against a backup copy:

```bash
# Copy backup to temp location
cp /root/backups/chronicle.lbug /tmp/elephas_scan.lbug

# Run scan with DB_PATH override (modify the script or use environment)
# The deep scan script uses: DB_PATH = Path('/root/commons/db/ocas-elephas/chronicle.lbug')
# Override by editing or symlinking

# After scan, journal output is written to /root/commons/journals/ocas-elephas/YYYY-MM-DD/
```

**Note**: The scan modifies the DB it runs against. If running against a backup copy, results won't be in the live DB. This is acceptable for analysis — but to apply results to the live DB, either restore the backup or re-run against the live DB after fixing it.

## Prevention
- The elephas DB file should be monitored for unexpected size changes (normal: ~15MB, corrupted: ~13MB of binary data)
- Add a checksum verification to the backup script
- Consider migrating fully to Chronicle (SQLite-based) — the Ladybug DB format has been a recurring source of corruption and lock conflicts
