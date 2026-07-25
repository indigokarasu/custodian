#!/bin/bash
# Hermes System Backup
# 1 local backup + 1 versioned backup on GitHub LFS
# Only canonical sources — no duplicates
#
# Usage:
#   backup_system.sh [--full|--incremental] [--dry-run]
#
# Exit codes: 0=success, 1=backup failure, 2=push failure

set -euo pipefail

MODE="full"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --full)        MODE="full"; shift ;;
        --incremental) MODE="incremental"; shift ;;
        --dry-run)     DRY_RUN=1; shift ;;
        --help)
            sed -n '2,14p' "$0"
            exit 0
            ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

LOCAL_BACKUP="/root/backups"
GITHUB_REPO="/root/indigo"
DATE=$(date +'%Y%m%d_%H%M%S')

mkdir -p "$LOCAL_BACKUP"

echo "[*] Backing up canonical databases (mode: $MODE)..."

do_backup() {
    local src="$1" dst="$2"
    if [ ! -e "$src" ]; then
        echo "SKIP: $src not found"
        return 0
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "DRY-RUN: would copy $src -> $dst"
    else
        cp "$src" "$dst"
    fi
}

# Chronicle (Elephas)
do_backup ~/.hermes/commons/db/ocas-elephas/chronicle.lbug "$LOCAL_BACKUP/chronicle.lbug"

# Weave
do_backup ~/.hermes/commons/db/ocas-weave/weave.lbug "$LOCAL_BACKUP/weave.lbug"

# Styx + Transactions
do_backup ~/.hermes/data/styx.db "$LOCAL_BACKUP/styx.db"
do_backup ~/.hermes/data/transactions.db "$LOCAL_BACKUP/transactions.db"

# Hermes State
do_backup ~/.hermes/state.db "$LOCAL_BACKUP/state.db"

# Sessions
do_backup ~/.hermes/sessions "$LOCAL_BACKUP/sessions"

# MemPalace
if [ -d /root/.mempalace ] && [ "$DRY_RUN" -eq 0 ]; then
    tar -czf "$LOCAL_BACKUP/mempalace.tar.gz" -C /root/.mempalace .
elif [ "$DRY_RUN" -eq 1 ]; then
    echo "DRY-RUN: would tar /root/.mempalace"
fi

echo "[*] Local backup complete. Pushing to GitHub LFS..."

# Copy to GitHub repo backup directory
mkdir -p "$GITHUB_REPO/backups"
cp "$LOCAL_BACKUP"/* "$GITHUB_REPO/backups/"

# Push to GitHub
cd "$GITHUB_REPO"
git add backups/
git commit -m "Backup $DATE" || echo "No changes to commit"
git push origin main

echo "[*] Backup complete: $DATE"
