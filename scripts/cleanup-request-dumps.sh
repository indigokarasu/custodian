#!/bin/bash
# cleanup-request-dumps.sh — Remove request_dump files older than 24 hours
#
# Usage:
#   cleanup-request-dumps.sh [--max-age <hours>] [--dry-run]
#
# Defaults:
#   SESSIONS_DIR="$HOME/.hermes/sessions"
#   MAX_AGE_HOURS=24
#
# These are debug artifacts from failed API calls and contain partial API key material.
# Runs hourly via cron.
# Exit codes: 0=success, 1=sessions dir not found (non-fatal)

set -euo pipefail

SESSIONS_DIR="${HOME}/.hermes/sessions"
PATTERN="request_dump_*.json"
MAX_AGE_HOURS=24
DRY_RUN=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-age)  MAX_AGE_HOURS="$2"; shift 2 ;;
        --dry-run)  DRY_RUN=1; shift ;;
        --help)
            sed -n '2,15p' "$0"
            exit 0
            ;;
        *) echo "Unknown arg: $1" >&2; exit 1 ;;
    esac
done

if [ ! -d "$SESSIONS_DIR" ]; then
    echo "Sessions directory not found: $SESSIONS_DIR"
    exit 0
fi

count=0
freed=0

while IFS= read -r -d '' file; do
    size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo 0)
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "DRY-RUN: would remove $file (${size} bytes)"
    else
        rm -f "$file"
    fi
    count=$((count + 1))
    freed=$((freed + size))
done < <(find "$SESSIONS_DIR" -name "$PATTERN" -mtime +0 -print0 2>/dev/null)

if [ "$DRY_RUN" -eq 1 ]; then
    echo "[*] DRY-RUN: would remove $count files, freeing ~$((freed / 1024)) KB"
else
    echo "[*] Removed $count request_dump files, freed ~$((freed / 1024)) KB"
fi
