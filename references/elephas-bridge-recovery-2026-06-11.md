# Elephas Bridge Recovery — 2026-06-11

## Problem
`ladybug-bridge-chronicle.service` in failed state (restart counter 5). Root cause: `<hermes-root>/commons/db/ocas-elephas/` directory did not exist. The bridge service unit file points `--db <hermes-root>/commons/db/ocas-elephas/chronicle.lbug` but the directory was never created (or was deleted).

The profile-local DB exists at `<hermes-home>/commons/db/ocas-elephas/chronicle.lbug` and is the authoritative copy.

## Fix
```bash
# 1. Create commons directory
mkdir -p <hermes-root>/commons/db/ocas-elephas

# 2. Symlink profile-local DB and metadata files
ln -sf <hermes-home>/commons/db/ocas-elephas/chronicle.lbug <hermes-root>/commons/db/ocas-elephas/chronicle.lbug
ln -sf <hermes-home>/commons/db/ocas-elephas/config.json <hermes-root>/commons/db/ocas-elephas/config.json
ln -sf <hermes-home>/commons/db/ocas-elephas/ingestion_log.jsonl <hermes-root>/commons/db/ocas-elephas/ingestion_log.jsonl
ln -sf <hermes-home>/commons/db/ocas-elephas/evidence.jsonl <hermes-root>/commons/db/ocas-elephas/evidence.jsonl

# 3. Copy pipeline script (not symlinked — may need profile-local edits)
cp <hermes-home>/commons/db/ocas-elephas/elephas_cron_pipeline.py <hermes-root>/commons/db/ocas-elephas/

# 4. Restart bridge via systemd (never kill -9 — causes respawn loop)
systemctl stop ladybug-bridge-chronicle.service
systemctl start ladybug-bridge-chronicle.service

# 5. Verify
curl -sf localhost:9192/health
```

## LBUG_C_API_LIB_PATH Requirement

The bridge **requires** `LBUG_C_API_LIB_PATH` to be set, or it exits immediately with "Could not find lbug C API shared library."

```bash
export LBUG_C_API_LIB_PATH=/tmp/liblbug-v0171/liblbug.so
```

The shared library is at `/tmp/liblbug-v0171/liblbug.so` (version 0.17.1). The Python `.so` wrapper is at `<hermes-install>/.venv/lib/python3.13/site-packages/ladybug/_lbug.cpython-313-x86_64-linux-gnu.so` but it cannot find the C library without the env var.

**If the bridge exits immediately after startup**, check logs for "Could not find lbug C API shared library" — set `LBUG_C_API_LIB_PATH` and restart.

## Key Rules
- **Always use `systemctl stop`/`start`** — never `kill -9` on the bridge PID. The service has `Restart=on-failure` and will respawn.
- **Symlink, don't copy the DB** — the `.lbug` file must remain a symlink to the profile-local copy so both paths see the same data.
- **The pipeline script uses `ladybug_client` (HTTP bridge)** — it does NOT need `real-ladybug` or direct DB access. The `LADYBUG_DB=chronicle` env var routes queries to port 9192.
- **If the bridge serves the wrong DB path**, check `ps aux | grep ladybug_bridge` to see which `--db` path it was started with. The service unit file hardcodes the path.
- **Set `LBUG_C_API_LIB_PATH=/tmp/liblbug-v0171/liblbug.so`** when starting the bridge manually. The systemd service unit file must also include this env var.
