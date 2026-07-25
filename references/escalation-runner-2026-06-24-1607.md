# Escalation Runner 2026-06-24 16:07 — Evidence Record

**Verdict:** No escalated issues found. System healthy.

## Checks Performed
- Commons issues.jsonl: all entries resolved/superseded
- Profile issues.jsonl: all entries resolved/superseded/monitoring
- Commons journals (24h): 2 entries, all clean
- Profile journals (24h): 18 entries, all clean
- Proposals dir: empty
- Known fix-loop: `oc_config_empty_section` in main config.yaml (already Tier 3 tracked in deep-scan-2026-06-24-0550)

## Key Lesson: Config Empty Section Fix-Loop Involves TWO Files

### Problem
The `oc_config_empty_section` fix-loop (4th occurrence as of 2026-06-24) was previously "fixed" on 2026-06-17, 06-18, and 06-23 — but null keys kept reappearing. Investigation revealed:

- **Profile config** (`<hermes-home>/profiles/indigo/config.yaml`): CLEAN — `context_file_max_chars: 10000` (proper value)
- **Main config** (`<hermes-home>/config.yaml`): STILL HAS NULLS — `max_concurrent_sessions: null` (line 11), `context_file_max_chars: null` (line 89), `max_in_progress_per_profile: null` (line 125), `max_parallel_jobs: null` (line 495)

### Root Cause
Previous fixes only addressed the profile config. The main config (`<hermes-home>/config.yaml`) retained null keys. Both files are loaded by the gateway — null keys in either generate TUI warnings and can trigger `oc_config_empty_section` fingerprint.

### Diagnostic Pattern
When investigating config empty section fix-loops:
```bash
# Check BOTH config files for null keys
grep -n ': null$' <hermes-home>/config.yaml <hermes-home>/profiles/*/config.yaml
```

### Fix Pattern
Apply null-key removal to BOTH files, not just the profile config. Use PyYAML to properly delete keys rather than setting to `null`.

### Why Not Auto-Fixed This Session
The null keys in the main config are cosmetic TUI warnings — they don't cause operational failures. The deep scan already documented this as Tier 3 escalation `oc_config_empty_section_fixloop_20260624` with confidence 0.95. The root cause ("gateway config migration regenerates null keys from template") requires a code-level fix (post-startup hook) that is beyond the escalation runner's auto-fix scope. The fix-loop is tracked and documented but not auto-fixable.
