# Escalation Runner 2026-06-24 21:08 — Timing Gap + YAML Null-Key Detection Pitfall

## Outcome
- 1 Tier 3 escalation found (deep-scan-20260624T210417Z-escalation.json) but correctly NOT fixed (fix-loop 5th occurrence)
- RCA record created: `rca-oc_config_empty_section_fix_loop-20260624`
- No other open issues or proposals
- `[SILENT]` returned after journal

## Timing Gap: Deep Scan Escalation After Last Esc-Run

**Problem**: The esc-run at 20:32 found 0 issues. The deep scan at 21:04 produced an escalation journal (`deep-scan-20260624T210417Z-escalation.json`) flagging the config empty section fix-loop 5th occurrence. No esc-run happened after 21:04 to pick it up.

**Root cause**: The escalation runner cron (`*/30 9-17 * * 1-5`) may run before a deep scan's escalation journal is written. If the deep scan runs at 21:00 and the next esc-run is at 21:30, the issue sits for 30 minutes. But if the deep scan's escalation journal is written AFTER the last esc-run of the day (e.g., esc-run at 20:32, deep scan at 21:04), the issue is never processed until the next day's first esc-run.

**Lesson**: When an esc-run finishes, check whether ANY deep scan ran since and produced an escalation journal. Before returning `[SILENT]`, scan for `deep-scan-*-escalation.json` files with mtime > last esc-run journal timestamp.

**Detection pattern**:
```bash
# After esc-run journal written, check for pending deep-scan escalations
LAST_ESC_RUN=$(stat -c %Y /path/to/latest/esc-run-*.json 2>/dev/null)
find <hermes-home> -name "deep-scan-*-escalation.json" -newer /path/to/latest/esc-run-*.json 2>/dev/null
# If found, process them before returning silent
```

## YAML Null-Key Representation Ambiguity

**Problem**: `grep -n ': null$' <hermes-home>/config.yaml` found 4 lines (literal `null` text). But the actual config had the literal `null` text removed — keys were just `key:` (empty after colon). PyYAML parses both as None, but the grep pattern only catches one form.

**Two equivalent YAML null representations**:
```yaml
# Form 1: literal null text (caught by grep ': null$')
max_concurrent_sessions: null

# Form 2: empty value (NOT caught by grep ': null$')
max_concurrent_sessions:
```

Both parse as `None` in Python. The config was modified between the deep scan (14:04 PDT) and the esc-run (14:08 PDT) — the literal `null` text was removed but the keys still had null values (Form 2).

**Correct detection**: Use PyYAML to check, not grep:
```python
import yaml
with open('<hermes-home>/config.yaml') as f:
    config = yaml.safe_load(f)
null_keys = [k for k, v in config.items() if v is None]
```

**Or grep both forms**:
```bash
grep -nE ': $|^[a-z_]+:$' <hermes-home>/config.yaml  # catches "key:" with nothing after
grep -n ': null$' <hermes-home>/config.yaml           # catches "key: null"
```

**Lesson**: When checking for null config keys, use PyYAML or grep BOTH patterns. A key with nothing after the colon is still null-valued.

## Fix-Loop 5th Occurrence — Correct Handling

Per the fix-loop detection rule, the 5th occurrence of `oc_config_empty_section` must NOT be fixed. The correct action is:
1. Create an RCA record with the full occurrence chain
2. Note the architectural fix direction (config validation hook or startup script)
3. Do NOT apply the fix again
4. Write esc-run journal + return `[SILENT]`

The RCA record was successfully created at:
`<hermes-home>/profiles/indigo/commons/data/osas-custodian/rca.jsonl`

## Journal Sync Pattern: shutil.copy2

For syncing individual journal files to commons, `shutil.copy2` preserves metadata:
```python
import shutil
shutil.copy2(src_journal_path, dst_journal_path)
```

This is more reliable than `cp -f` for preserving mtime (which matters for future scans).