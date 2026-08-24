# fos→os NameError Pattern

## Symptom

All scripts in `$HERMES_HOME/../<profile>/skills/ocas-custodian/scripts/` that use `fos.path.expanduser(...)` crash with `NameError: name 'fos' is not defined` at import/exec time.

## Scope (confirmed 2026-07-27)

The typo is a copy-paste error across **8 files** in the custodian scripts directory:

| File | Line(s) | Blocks |
|------|---------|--------|
| `classify_error_jobs.py` | 20, 90 | Step 6 de-aggregation of exit-1 wrapper jobs |
| `confirm_provider_recovery.py` | 134 | Provider recovery verification |
| `verify_provider_recovery.py` | 24, 40 | Provider recovery verification |
| `verify_escalation_state.py` | 161 | Escalation-state bidirectional verification |
| `find_missed_user_gated_jobs.py` | 187 | Missed enrollment probe |
| `bucket_error_jobs.py` | 138 | Error-job fingerprint bucketing |
| `classify_llm_necessity.py` | 27 | LLM-necessity classification (standalone) |
| `classify_llm_necessity_integration.py` | 24, 25, 273 | Step 10b LLM-necessity integration into light scan |

## Fix

```bash
# Fix all occurrences in one pass (confirmed 8 files affected)
grep -rln 'fos\.path\.expanduser' scripts/ | xargs sed -i 's/fos\.path\.expanduser/os.path.expanduser/g'
```

Run across ALL `.py` files in the scripts directory. The typo is a copy-paste error — if one occurrence is fixed, check ALL files in the same module.

## Escalation Runner Auto-Apply

When the escalation runner encounters a `NameError: name 'fos' is not defined` in any custodian script output during a scan, apply the fix immediately rather than escalating the symptom. This unblocks Step 6 (exit-1 de-aggregation) and Step 10b (LLM-necessity integration).

1. Run `s/fos\.path\.expanduser/os.path.expanduser/g` across ALL `.py` files in the skill's `scripts/` directory.
2. Verify by running both `classify_error_jobs.py` and `classify_llm_necessity_integration.py` — both should exit 0 with no traceback.
3. Confirm no remaining `fos.path` occurrences with `grep -rn 'fos\.path' scripts/`.

## Post-Fix Verification 2026-07-28

Both custodian scripts now run clean (exit 0). Nous provider partially recovered (9/16 jobs OK), but 7 jobs still returning HTTP 503 upstream capacity. The fos→os fix itself did not cause the Nous recovery — the provider partially stabilized independently.

## Escalation Runner Auto-Apply

When the escalation runner encounters a `NameError: name 'fos' is not defined` in any custodian script output during a scan, apply the fix immediately rather than escalating the symptom. This unblocks Step 6 (exit-1 de-aggregation) and Step 10b (LLM-necessity integration).

1. Run `s/fos\.path\.expanduser/os.path.expanduser/g` across ALL `.py` files in the skill's `scripts/` directory.
2. Verify by running both `classify_error_jobs.py` and `classify_llm_necessity_integration.py` — both should exit 0 with no traceback.
3. Confirm no remaining `fos.path` occurrences with `grep -rn 'fos\.path' scripts/`.

## Post-Fix Verification 2026-07-28

Confirmed that 9 of 16 Nous-503 jobs recovered after the fix was applied (the fix itself did not cause the recovery — the provider partially stabilized). The 7 remaining 503 jobs are genuine upstream capacity issues. Both `classify_error_jobs.py` and `classify_llm_necessity_integration.py` now run clean (exit 0).