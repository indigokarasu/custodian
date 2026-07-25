#!/bin/bash
# backup_all_hermes_data.sh — Backs up all Hermes data to GitHub LFS repo + local
# This is a thin wrapper; the canonical script is in indigo-repo.
#
# Usage:
#   backup_all_hermes_data.sh [--dry-run]
#
# Exit codes: 0=success, 1=backup failure, 2=push failure

set -euo pipefail

DRY_RUN=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --help)
            sed -n '2,11p' "$0"
            exit 0
            ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

SCRIPT="<repo-root>/scripts/backup_all_hermes_data.sh"
if [ ! -f "$SCRIPT" ]; then
    echo "ERROR: canonical backup script not found at $SCRIPT" >&2
    exit 1
fi

if [ "$DRY_RUN" -eq 1 ]; then
    echo "DRY-RUN: would run $SCRIPT"
else
    exec bash "$SCRIPT" "$@"
fi