#!/bin/bash
# Cleanup request_dump files older than 24 hours
# These are debug artifacts from failed API calls and contain partial API key material
# Runs hourly via cron

SESSIONS_DIR="$HOME/.hermes/sessions"
PATTERN="request_dump_*.json"
MAX_AGE_HOURS=24

if [ ! -d "$SESSIONS_DIR" ]; then
    echo "Sessions directory not found: $SESSIONS_DIR"
    exit 0
fi

# Find and delete files older than MAX_AGE_HOURS
count=0
freed=0
while IFS= read -r -d '' file; do
    size=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo 0)
    rm -f "$file"
    count=$((count + 1))
    freed=$((freed + size))
done < <(find "$SESSIONS_DIR" -name "$PATTERN" -mmin +$((MAX_AGE_HOURS * 60)) -print0 2>/dev/null)

if [ "$count" -gt 0 ]; then
    freed_mb=$(echo "scale=1; $freed / 1024 / 1024" | bc 2>/dev/null || echo "?")
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] Cleaned up $count request_dump files, freed ${freed_mb}MB"
else
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] No request_dump files older than ${MAX_AGE_HOURS}h found"
fi
