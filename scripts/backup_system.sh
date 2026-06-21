#!/bin/bash
# Hermes System Backup
# 1 local backup + 1 versioned backup on GitHub LFS
# Only canonical sources — no duplicates

set -e

LOCAL_BACKUP="/root/backups"
GITHUB_REPO="/root/indigo"
DATE=$(date +'%Y%m%d_%H%M%S')

mkdir -p "$LOCAL_BACKUP"

echo "[*] Backing up canonical databases..."

# Chronicle (Elephas)
cp <hermes-root>/commons/db/ocas-elephas/chronicle.lbug "$LOCAL_BACKUP/chronicle.lbug"

# Weave
cp <hermes-root>/commons/db/ocas-weave/weave.lbug "$LOCAL_BACKUP/weave.lbug"

# Styx + Transactions
cp <hermes-root>/data/styx.db "$LOCAL_BACKUP/styx.db"
cp <hermes-root>/data/transactions.db "$LOCAL_BACKUP/transactions.db"

# Hermes State
cp <hermes-root>/state.db "$LOCAL_BACKUP/state.db"

# Sessions
cp -r <hermes-root>/sessions "$LOCAL_BACKUP/sessions"

# MemPalace
tar -czf "$LOCAL_BACKUP/mempalace.tar.gz" -C /root/.mempalace .

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
