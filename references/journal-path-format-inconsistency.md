# Journal Path Format Inconsistency Pattern

Confirmed 2026-07-01 during deep scan.

## The Problem

Custodian session runs do NOT consistently write journals to the canonical path format specified in `references/observation-journal-schema.md`:

```
Canonical: {agent_root}/commons/journals/ocas-custodian/{YYYY-MM-DD}/{run_id}.json
```

Instead, three different formats coexist in the journals directory:

| Format | Example Path | Example File |
|--------|-------------|--------------|
| **Canonical** (with hyphens) | `.../ocas-custodian/2026-07-01/` | `deep-scan-20260701T090716Z.json` |
| **Compact** (no hyphens) | `.../ocas-custodian/20260701/` | `20260701T080610Z.json` |
| **Loose** (no date dir) | `.../ocas-custodian/` | `light-scan-2026-06-21-201000.json` |

## Impact on Gap Detection

The "Journal gap detection" gotcha in SKILL.md says:

> Check the latest journal in `{agent_root}/commons/journals/ocas-custodian/YYYY-MM-DD/` — if the most recent file's date is >3 days ago, the cron may not be firing

This ONLY checks the `YYYY-MM-DD/` directory. If recent journals exist in the Compact format directory (`YYYYMMDD/`) or as Loose files, gap detection falsely reports a gap.

## Root Cause

Multiple code paths write journals with different path construction:
1. **Light scan scripts** — may use `datetime.now().strftime("%Y%m%d")` (no hyphens) for the directory
2. **Deep scan scripts** — may use `datetime.now().strftime("%Y-%m-%d")` (with hyphens) for the directory
3. **Loose files** — older or fallback code paths write directly to the journals root without a subdirectory
4. **Escalation runner** — may use still another format

## Fix Direction

1. **Short-term (gap detection):** When checking for journal gaps, search ALL formats:
   - Check `YYYY-MM-DD/` (canonical)
   - Check `YYYYMMDD/` (compact variant)
   - Check loose files in root dir for files within 3 days
   - Use `find {journals_dir} -name "*.json" -mtime -3` as a robust fallback

2. **Long-term (consistency):** Unify all journal-writing code paths to use the canonical `YYYY-MM-DD/` format. The `observation-journal-schema.md` already specifies this format — the bug is that not all writers follow it.

## Diagnosis Steps

```python
import os, glob
journals_dir = "<hermes-home>/commons/journals/ocas-custodian"

# Check all three locations
canonical_dirs = sorted(glob.glob(f"{journals_dir}/2026-??-??/"))
compact_dirs = sorted(glob.glob(f"{journals_dir}/2026????/"))
loose_files = sorted(glob.glob(f"{journals_dir}/*.json"))

print(f"Canonical dirs: {canonical_dirs[-3:] if canonical_dirs else 'none'}")
print(f"Compact dirs: {compact_dirs[-3:] if compact_dirs else 'none'}")
print(f"Loose files: {loose_files[-3:] if loose_files else 'none'}")
```

## Occurrences

| Date | Scan Type | Format Used |
|------|-----------|-------------|
| 2026-06-16 | Light scan | Loose |
| 2026-06-17 | Light scan | Loose |
| 2026-06-21 | Light scan | Loose |
| 2026-07-01 | Light scan | Canonical (`2026-07-01/`) |
| 2026-07-01 | Unknown | Compact (`20260701/`) |
| 2026-07-01 | Deep scan | Canonical (`2026-07-01/`) |