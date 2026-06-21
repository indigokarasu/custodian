# memory_guard failure pattern

## What to check

The `finch:memory-guard-floor` cron job (runs every 6h) enforces the hard char-cap on MEMORY.md.
Check its output for failures:

1. **Job not running**: If `finch:memory-guard-floor` has no `last_run_at` or gap > 8h, the guard floor is not active.
2. **STILL OVER CAP warning**: If the guard's DecisionRecord shows `over_cap_after=true`, directives alone exceed the cap — needs LLM consolidation or human review.
3. **INTEGRITY violation**: If the guard refuses to apply because directives would be lost, flag for immediate review.
4. **Lock contention**: If the guard reports "MEMORY.md locked by a live process" repeatedly, something is holding the lock.

## How to check

```bash
# Check last run
hermes cron list 2>/dev/null | grep -A5 "memory-guard-floor"

# Check DecisionRecords for failures
tail -20 <hermes-home>/commons/data/ocas-finch/decisions.jsonl | python3 -c "
import sys, json
for line in sys.stdin:
    d = json.loads(line)
    if d.get('over_cap_after') or d.get('outcome') == 'error':
        print(json.dumps(d, indent=2))
"

# Check MEMORY.md current size
wc -c <hermes-home>/MEMORY.md
```

## Severity

- **STILL OVER CAP**: Tier 2 — flag in next custodian report. Not urgent but needs attention within 24h.
- **INTEGRITY violation**: Tier 1 — immediate escalation. Directives are at risk.
- **Job not running**: Tier 2 — check if cron job is enabled; restart if needed.
