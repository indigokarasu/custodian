# fix_effectiveness.jsonl Schema Contamination

## Pattern

The `ConfidenceModel` in `classifier.py` reads `fix_effectiveness.jsonl` line-by-line on init, storing each record by `fingerprint` into `self._effectiveness`. The expected schema is a confidence record:

```json
{
  "fingerprint": "...",
  "attempts": 3,
  "successes": 2,
  "failures": 1,
  "success_rate": 0.6667,
  "confidence_score": 0.4,
  "recommended_tier": 1
}
```

But the file can accumulate raw fix log entries that have a different schema:

```json
{
  "fingerprint": "...",
  "fix_id": "fix-20260604-001",
  "target": "9 stale jobs",
  "outcome": "reset_scheduler_state",
  "timestamp": "2026-06-04T20:30:00-07:00"
}
```

These raw entries **lack the `attempts` key**. When `should_escalate()` accesses `rec["attempts"]` on one of these records, it raises `KeyError`, which crashes `custodian_status`, which gets logged as an error — creating a self-inflicted crash loop.

## Detection

```bash
python3 -c "
import json
path = '<hermes-home>/commons/data/ocas-custodian/fix_effectiveness.jsonl'
with open(path) as f:
    for i, line in enumerate(f):
        line = line.strip()
        if not line: continue
        r = json.loads(line)
        if 'attempts' not in r:
            print(f'Line {i}: MISSING attempts. Keys: {list(r.keys())}')
"
```

Clean up malformed entries:

```bash
python3 -c "
import json
path = '<hermes-home>/commons/data/ocas-custodian/fix_effectiveness.jsonl'
valid = []
with open(path) as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if 'attempts' in r:
            valid.append(r)
with open(path, 'w') as f:
    for r in valid:
        f.write(json.dumps(r, default=str) + '\n')
print(f'Kept {len(valid)} valid records')
"
```

## Permanent Fix (in classifier.py)

Two changes needed:

1. **`_load()` method** — skip records without the `attempts` key:
```python
def _load(self) -> None:
    records = _load_jsonl(self._effectiveness_path())
    if records:
        for r in records:
            fp = r.get("fingerprint", "unknown")
            if "attempts" not in r:
                logger.warning("Skipping malformed effectiveness record for %s", fp)
                continue
            self._effectiveness[fp] = r
    else:
        self._backfill_from_fixes()
```

2. **`should_escalate()` method** — use `.get()` for defense in depth:
```python
def should_escalate(self, fingerprint: str) -> bool:
    rec = self._effectiveness.get(fingerprint)
    if rec is None:
        return True
    attempts = rec.get("attempts", 0)
    if attempts >= 3 and rec.get("success_rate", 1.0) < 0.2:
        return True
    if attempts >= 2 and rec.get("success_rate", 1.0) < 0.5:
        return True
    return False
```

## How Contamination Happens

The `_backfill_from_fixes()` method aggregates from `fixes.jsonl` and writes proper confidence records. But if `fixes.jsonl` entries are later directly appended to `fix_effectiveness.jsonl` (e.g., by a script or manual edit), or if the file is written by code that doesn't use `_compute_record()`, raw entries accumulate.

Prevent by always writing effectiveness records through `_compute_record()` or `record_outcome()`, never by direct file append.
