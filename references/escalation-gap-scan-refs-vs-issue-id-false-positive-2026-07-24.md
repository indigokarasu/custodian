# Gap-Scan False Positive: journal `escalation_refs` vs issue `issue_id` mismatch

**Confirmed:** 2026-07-24 escalation execution loop.

## Symptom
`scripts/scan_escalation_journal_gaps.py --hours 24` reported multiple "GAP (escalation_needed but no matching open issue)" entries, naming issue-style ids that look real (e.g. `oc_taste_spotify_token_missing_20260713`, `oc_chronicle_event_actor_check_constraint_20260722`). On its face this means Custodian flagged issues that were never persisted to `issues.jsonl` — a Step 8b silent-drop.

## Actual root cause
The scan cross-references each journal's `escalation_refs` (which are **skill/job NAMES** like `ocas-autobio-observe`) against the OPEN issues' `issue_id` field (e.g. `oc_autobio_content_policy_blocked_20260723T0505Z`). A name can never equal an id, so the matcher reports the underlying open issue as "missing" — even though it is present and open.

## Verification procedure (run BEFORE any `--write`)
1. `python3 scripts/parse_issues_jsonl.py` — source of truth for open count.
2. `python3 scripts/verify_escalation_state.py` — confirms live job/issue state.
3. For each cited "missing" id, grep the **authoritative** data-path file directly:
   ```python
   python3 -c "d=open('~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl').read(); print('PRESENT' if '<id>' in d else 'ABSENT')"
   ```
   If PRESENT → it is a FALSE POSITIVE.
4. **Do NOT pass `--write`** to the gap-scan. Writing would re-persist existing open issues as duplicate escalations.

## 2026-07-24 concrete case
Gap-scan reported 5 "missing" ids. All 5 were confirmed PRESENT + `status: open` in the authoritative `issues.jsonl`:
- `oc_taste_spotify_token_missing_20260713`
- `oc_chronicle_event_actor_check_constraint_20260722`
- `oc_state_db_oversized_20260722T0205Z`
- `oc_autobio_content_policy_blocked_20260723T0505Z` (later resolved as a false escalation via live re-run)
- `oc_cron_config_drift_unpinned_rally_sift_20260722`

No issues were actually missing. The scan output was 100% false positive.

## Contrast with the 2026-07-15 guard (escalation-loop-pitfalls §1a)
The 2026-07-15 FALSE-POSITIVE GUARD covers journals whose referenced issue is already `resolved`/`duplicate`. This 2026-07-24 variant (§1b) covers the OPPOSITE: the referenced issue is OPEN and present, but the scan still reports it missing due to name↔id mismatch. Both require the same fix: verify against the authoritative file, never blindly `--write`.
