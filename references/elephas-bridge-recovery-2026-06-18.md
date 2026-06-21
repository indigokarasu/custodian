# Elephas Bridge Recovery — 2026-06-18

## Symptom
LadybugDB bridge on port 9192 fails to start with:
```
Could not find lbug C API shared library. Set LBUG_C_API_LIB_PATH...
```

## Root Cause
The `real_ladybug` v0.15.3 C extension (`_lbug.cpython-314-x86_64-linux-gnu.so`) needs the `ArrowQueryResult` symbol from `liblbug.so`. The `liblbug.so` symlink in `real_ladybug/` points to the Python C extension itself (not the C library), and the v0.17.1 `liblbug.so` in `/tmp/liblbug-v0171/` doesn't export the required symbols.

The `ladybug` module from the **3.13 site-packages** (`<hermes-install>/.venv/lib/python3.13/site-packages/ladybug/`) works correctly when given `LBUG_C_API_LIB_PATH` pointing to the v0.17.1 library.

## Fix

### 1. Set environment variable
```bash
export LBUG_C_API_LIB_PATH=/tmp/liblbug-v0171/liblbug.so.0.17.1
```

### 2. Kill any stale bridge process
```bash
pkill -f "ladybug_bridge" 2>/dev/null
sleep 2
# Verify lock released
fuser <hermes-root>/commons/db/ocas-elephas/chronicle.lbug 2>/dev/null && echo "Lock still held" || echo "Lock released"
```

### 3. Start the bridge
```bash
LBUG_C_API_LIB_PATH=/tmp/liblbug-v0171/liblbug.so.0.17.1 \
  nohup python3 <hermes-home>/scripts/ladybug_bridge.py \
  --db <hermes-root>/commons/db/ocas-elephas/chronicle.lbug \
  --port 9192 \
  > /tmp/ladybug_bridge_elephas.log 2>&1 &
```

### 4. Verify
```bash
sleep 4 && curl -s http://127.0.0.1:9192/health
```

### 5. Run the pipeline
```bash
python3 <hermes-root>/commons/db/ocas-elephas/elephas_cron_pipeline.py
```

## Key Files
- Bridge script: `<hermes-home>/scripts/ladybug_bridge.py`
- Pipeline script: `<hermes-root>/commons/db/ocas-elephas/elephas_cron_pipeline.py`
- Working liblbug.so: `/tmp/liblbug-v0171/liblbug.so.0.17.1`
- Ladybug module (3.13): `<hermes-install>/.venv/lib/python3.13/site-packages/ladybug/`
- Chronicle DB: `<hermes-root>/commons/db/ocas-elephas/chronicle.lbug`
- Bridge log: `/tmp/ladybug_bridge_elephas.log`

## DB File Segfault Workaround (2026-06-18)

When the bridge still segfaults (exit code 255) after setting `LBUG_C_API_LIB_PATH`, the cause is a corrupt or incompatible `chronicle.lbug` file. `real_ladybug` opens a fresh test DB fine but crashes on the production DB.

**Workaround: Copy DB to /tmp, bridge opens the copy, sync back after.**

```bash
# 1. Copy DB and remove WAL (WAL can cause issues on copy)
cp /root/commons/db/ocas-elephas/chronicle.lbug /tmp/chronicle_copy.lbug
rm -f /tmp/chronicle_copy.lbug.wal

# 2. Start bridge on the copy
cd <hermes-root>/scripts && python3 ladybug_bridge.py --db /tmp/chronicle_copy.lbug --port 9192

# 3. Verify
curl -s http://localhost:9192/health

# 4. Run pipeline (elephas_cron_run.py uses port 9192)
cd /root/indigo/commons/db/ocas-elephas && python3 elephas_cron_run.py

# 5. Sync changes back to main DB
cp /tmp/chronicle_copy.lbug /root/commons/db/ocas-elephas/chronicle.lbug
```

**Important**: The bridge writes to the copy, not the original. You MUST sync back after the run or changes are lost next time the bridge restarts from the original file.

## Notes
- The bridge's `open_connection()` adds 3.13 site-packages to sys.path and imports `ladybug` — this works with the env var
- The pipeline uses `ladybug_client` which routes ALL queries through the HTTP bridge (port 9192)
- The pipeline does NOT manage bridge lifecycle — bridge must be running before pipeline starts
- Do NOT use `fuser` + `kill -9` to stop the bridge — use `pkill -f "ladybug_bridge"` then verify with `fuser`
