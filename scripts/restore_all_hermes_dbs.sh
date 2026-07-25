#!/bin/bash
# restore_all_hermes_dbs.sh — Restore Hermes databases from backup
#
# Usage:
#   restore_all_hermes_dbs.sh [--backup-dir <dir>] [--dry-run]
#
# Defaults:
#   BACKUP_DIR="<home-dir>/backup"
#
# Restores: state.db, state-snapshots, Chronicle (Elephas), Weave (social graph)
# Exit codes: 0=success, 1=backup dir missing, 2=copy failure

set -euo pipefail

BACKUP_DIR="<home-dir>/backup"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --backup-dir) BACKUP_DIR="$2"; shift 2 ;;
        --dry-run)    DRY_RUN=1; shift ;;
        --help)
            sed -n '2,15p' "$0"
            exit 0
            ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

if [ ! -d "$BACKUP_DIR" ]; then
    echo "ERROR: Backup directory not found: $BACKUP_DIR" >&2
    exit 1
fi

restore() {
    local src="$1" dst="$2"
    if [ ! -e "$src" ]; then
        echo "SKIP: $src not found in backup"
        return 0
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "DRY-RUN: would copy $src -> $dst"
    else
        cp "$src" "$dst"
        echo "OK: $dst"
    fi
}

echo "[*] Restoring Hermes databases from $BACKUP_DIR..."

restore "$BACKUP_DIR"/hermes_state_*.db ~/.hermes/state.db

if [ -d "$BACKUP_DIR"/hermes_state_snapshots_* ] 2>/dev/null; then
    restore "$BACKUP_DIR"/hermes_state_snapshots_* ~/.hermes/state-snapshots/
fi

restore "$BACKUP_DIR"/chronicle_lbug_* ~/.hermes/commons/db/ocas-elephas/chronicle.lbug
restore "$BACKUP_DIR"/chronicle_lbug_backup_* ~/.hermes/prep_preservation/chronicle.lbug
restore "$BACKUP_DIR"/weave_lbug_* <repo-root>/commons/db/ocas-weave/weave.lbug
restore "$BACKUP_DIR"/weave_lbug_backup_* ~/.hermes/prep_preservation/chronicle.lbug
restore "$BACKUP_DIR"/weave_lbug_data_* ~/.hermes/data/hermes-weave/weave.lbug

echo "[*] Done."