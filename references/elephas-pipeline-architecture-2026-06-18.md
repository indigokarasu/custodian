# Elephas Pipeline Architecture & Cron Execution — 2026-06-18

## Two Implementations

There are **two distinct elephas pipeline implementations**. Know which one you're using.

### 1. `elephas_cron_run.py` (USE THIS FOR CRON)
- **Location**: `/root/indigo-repo/commons/db/ocas-elephas/elephas_cron_run.py`
- **Also copied to**: `/root/indigo/commons/db/ocas-elephas/elephas_cron_run.py` (if indigo symlink/dir exists — verify with `ls -la /root/indigo/`)
- **⚠️ Journal output path**: Writes journals to `/root/commons/journals/ocas-elephas/` (NOT `<hermes-root>/commons/journals/`). Lucid scans the `.hermes` path, so elephas journals won't be re-processed by Lucid — but also won't appear in Lucid's cursor. This is intentional (elephas journals are self-contained run records, not Lucid input).
- **Transport**: HTTP bridge on port 9192 via `urllib.request`
- **Dedup**: Yes — uses `ingestion_log.jsonl` to skip already-processed files
- **Phases**: 1a (journal ingestion) → 1b (weave enrichment) → 2 (immediate consolidation) → 3 (memory ingestion) → 4 (session ingestion) → 5 (deep consolidation)
- **Run**: `cd /root/indigo/commons/db/ocas-elephas && python3 elephas_cron_run.py`

### 2. `elephas_bridge.py` (Chronicle-core based — DO NOT USE FOR CRON)
- **Location**: `<hermes-home>/scripts/elephas_bridge.py`
- **Transport**: Direct Chronicle core (`engine.core.ChronicleCore`)
- **Dedup**: **None** — re-scans ALL journal files every run
- **Performance**: **Times out** on 7745+ files (never finishes)
- **Status**: Broken for production use. Do not invoke from cron.

### 3. `elephas_cron_pipeline.py` (ACTIVE — USE FOR CRON)
- **Location**: `<hermes-home>/commons/db/ocas-elephas/elephas_cron_pipeline.py`
- **Also at**: `<hermes-root>/commons/db/ocas-elephas/elephas_cron_pipeline.py` (same file, same inode — symlinked)
- **Transport**: HTTP bridge on port 9192 via `ladybug_client` (`lb.configure("chronicle")`)
- **Dedup**: Yes — uses `ingestion_log.jsonl` to skip already-processed files
- **Phases**: 0 (clean stale) → 1 (find unprocessed) → 2 (open DB) → 3 (ingest journals → signals/candidates) → 4 (immediate consolidation) → 5 (verify + write journal)
- **Run**: `cd <hermes-home>/commons/db/ocas-elephas && python3 elephas_cron_pipeline.py`
- **⚠️ Do NOT invoke via `exec()`** — causes double-run (the `if __name__ == "__main__":` block fires both during exec and after). Use subprocess or import `run_pipeline` without exec.
- **Verified working**: 2026-06-19 02:06 UTC — bridge healthy, pipeline completed, 0 pending candidates after consolidation.

### 4. `elephas_ingest_wrapper.sh` (DEPRECATED — DO NOT USE)
- **Location**: `<hermes-home>/scripts/elephas_ingest_wrapper.sh`
- **Problem**: Stops bridge via `fuser`+`kill`, runs pipeline, restarts bridge. Race-prone.
- **Status**: Superseded by `elephas_cron_pipeline.py`.

## Pre-flight: Check Bridge Health
Before running the pipeline, verify the bridge is up:
```bash
curl -sf http://127.0.0.1:9192/health
```
If down, see `elephas-bridge-recovery-2026-06-11.md` for recovery procedure.

**Required env var**: `LBUG_C_API_LIB_PATH=/tmp/liblbug-v0171/liblbug.so.0.17.1` — the bridge exits immediately without it. The `liblbug.so` symlink in `real_ladybug/` points to the Python C extension (not the C library) and won't work. The actual C library is at `/tmp/liblbug-v0171/liblbug.so.0.17.1`. See `elephas-bridge-recovery-2026-06-18.md` for the full recovery procedure including pkill + restart.

## Malformed Journal Files Pattern

**Symptom**: Pipeline logs multiple `SKIP` lines with JSON parse errors.

**Root cause**: OCAS skill journals are written as append-only JSON. If a skill run is interrupted (OOM, gateway restart, SIGKILL), the JSON is truncated mid-write. The file is left in a permanently corrupt state.

**Affected skills**: Primarily `ocas-mentor` (high frequency, many small writes).

**Remediation**: Corrupt files should be moved to `.archive/` so they stop blocking the scan:
```bash
cd <hermes-root>/commons/journals
find . -name "*.json" -not -path "./.archive/*" -exec sh -c 'python3 -c "import json; json.load(open(\"$1\"))" 2>/dev/null || echo "$1"' _ {} \;
```

**Empty filename bug**: Files named literally `.json` (empty basename) in `ocas-mentor/`. Path generation bug. Investigate and remove.

## Consolidation State (as of 2026-06-19 02:06 UTC)
- 90 Entity, 142 Place, 135 Concept, 17 Thing nodes (237 total entity-type nodes)
- 19,655 signals, 11,452 candidates (all 11,452 promoted — 0 pending)
- 20,039 relationships
- 0 orphan signals
- 0 pending user-relevant candidates
- **Status: FULLY CONSOLIDATED**
- Chronicle SQLite: 119,153 events, 0 pending curation jobs
- Ladybug DB: ~23MB

## ⚠️ DB Corruption Incident (2026-06-18)

The live Ladybug DB at `/root/commons/db/ocas-elephas/chronicle.lbug` was found **corrupted** — overwritten with 13MB of raw thermal camera data (DIY-Thermocam Lepton 2.x format). SQLite reports "file is not a database." `real_ladybug` segfaults.

**Impact**: All elephas cron runs since ~01:35 UTC silently failed (0 signals, 0 candidates across 20+ runs).

**Recovery**: Backup at `/root/backups/chronicle.lbug` (15MB, 06:12 UTC) verified queryable. Profile copy at `<hermes-home>/commons/db/ocas-elephas/chronicle.lbug` (15MB, locked by gateway).

**Deep scan workaround**: Copied backup to `/tmp/elephas_scan.lbug`, ran `elephas_deep_run.py` with DB_PATH override against the copy. Journal written to `/root/commons/journals/ocas-elephas/2026-06-18/deep_5d6db6fc.json`.

**Root cause**: Unknown process wrote binary sensor data to the DB file path.

**See**: `references/elephas-db-corruption-2026-06-18.md` for full details.

## Cron Invocation Gotchas (2026-06-18)

### Double-Run via `exec()`

**Never** invoke `elephas_cron_pipeline.py` via Python `exec()` when the script has an `if __name__ == "__main__":` guard. The `exec()` body runs the entire script (including the `if __main__` block), and then the `if __name__` check also fires — the pipeline runs **twice**.

**Wrong** (causes double-run):
```python
exec(open('elephas_cron_pipeline.py').read())
run_pipeline()  # This won't even be reached; the exec already ran everything
```

**Correct** (subprocess):
```bash
python3 <hermes-home>/commons/db/ocas-elephas/elephas_cron_pipeline.py
```

**Correct** (import + call, no `exec`):
```python
import sys
sys.path.insert(0, '<hermes-home>/commons/db/ocas-elephas')
from elephas_cron_pipeline import run_pipeline
run_pipeline()
```

### Entity-Free Journal Batches Are Normal

A run that processes 25+ files but creates 0 signals/candidates is **not an error**. Scan/sweep journals (`ocas-spot`, `ocas-finch`, `ocas-forge` scan files), dream journals (`ocas-lucid`), praxis reviews, and dispatch records typically carry no `entities_observed`. They are logged as `no_entities` in the ingestion log and should be skipped silently. The pipeline is working correctly when it processes these without error.

### Corrupt `ocas-mentor` Files Recur

The `ocas-mentor` skill produces truncated JSON files when runs are interrupted (OOM, gateway restart, SIGKILL). Files named literally `.json` (empty basename) are a path generation bug. These files are permanently corrupt and should be moved to `.archive/` to stop blocking the scan. As of 2026-06-18, 12+ such files accumulate per week.
