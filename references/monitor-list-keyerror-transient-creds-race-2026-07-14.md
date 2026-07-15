# monitor:list / tasks_monitor.py `KeyError: 'access_token'` — transient credential-refresh race (NOT a persistent missing-token defect)

**Confirmed:** 2026-07-14 (escalation execution loop)

## Symptom
A live re-run of `tasks_monitor.py --mode check` raises:
```
KeyError: 'access_token'
  File ".../ocas-tasks/scripts/tasks_monitor.py", line 88, in get_access_token
    return creds["access_token"]
```
The no_agent wrapper `monitor_list.py` masks this as bare `Script exited with code 1` (no stderr forwarded). This is the **same signature** the `oc_google_tasks_access_token_missing` fingerprint is built from.

## The trap
This signature looks identical to a *persistent* missing-token defect (the documented mask-gap case, `references/monitor-list-exit1-mask-gap.md`, where the live signature differs from already-resolved covering issues and you persist a NEW issue). But here it was **transient**.

## Why it was transient (root cause)
The credential file was being **rewritten by a concurrent refresh** at the moment of the probe:
- File: `/root/.google_workspace_mcp/credentials/google-workspace-user.json`
- `load_creds()` read a mid-write / just-before-refresh snapshot that lacked `access_token`.
- The file's mtime coincided exactly with the probe timestamp.

## Discriminating test — run BEFORE persisting `oc_google_tasks_access_token_missing`
1. **Inspect the creds file directly.** If it contains a valid `access_token` (and `refresh_token`), the failure was a race, NOT a persistently missing token:
   ```bash
   F=/root/.google_workspace_mcp/credentials/google-workspace-user.json
   <hermes-install>/.venv/bin/python -c "import json; d=json.load(open('$F')); print('access_token' in d, 'refresh_token' in d, 'expiry' in d)"
   ```
2. **Re-run the wrapper** `monitor_list.py` (and/or `hermes cron run <id>`) 1–2 more times. It succeeds once the refresh completes.
3. **Only persist `oc_google_tasks_access_token_missing`** if the token is GENUINELY absent from the file AND the failure reproduces across re-runs. A one-shot failure with a valid token in the file is a false-escalation candidate.

## Outcome this session
- Attempt 1 (direct `tasks_monitor.py`): `KeyError: 'access_token'` (mid-refresh).
- Creds-file inspection: valid `access_token` + `refresh_token` present.
- Attempt 2 (`monitor_list.py` wrapper): exit 0.
- Attempt 3 (`hermes cron run 39b7edc44b35`): exited 0.

Conclusion: transient race, **not** a new defect. No issue persisted; no escalation created.

## 2026-07-15 FOLLOW-UP: the FALSE RE-OPEN and its resolution
The race recurred — but this time a prior light scan had ALREADY persisted/re-opened `oc_google_tasks_access_token_missing_20260714` (status=user_gated, escalation_needed=true) on the stale premise that the KeyError was a *genuine* recurrence. That re-open was the documented **inverse false-recovered-note trap**: it saw `monitor:list` `status=error` with an OLD `last_run_at` (23:34Z) and declared live failure WITHOUT re-confirming the credential file had recovered. The file HAD recovered. Resolving the false escalation required live verification, not just not-persisting.

**Resolution procedure (executed 2026-07-15, worked):**
1. **Inspect creds file keys** — valid `access_token` + unexpired `expiry` present → race, not defect.
2. **Re-run wrapped worker** `tasks_monitor.py --mode check` → exit 0, no KeyError.
3. **Re-run no_agent wrapper** `monitor_list.py` → exit 0.
4. **Trigger the actual job** `hermes cron run 39b7edc44b35` → `succeeded`. (Authoritative live-verification primitive — a `hermes chat` pong probe is NOT sufficient; re-running the affected JOB proves recovery. A `status=error` + old `last_run_at` proves nothing about the current run.)
5. **Confirm jobs.json** `monitor:list` → `last_status=ok`, `last_error=None`.
6. **Resolve the false escalation** with `scripts/race_safe_issue_patch.py --issue-id oc_google_tasks_access_token_missing_20260714 --require-status user_gated --set status=resolved --set user_gated=false --set escalation_needed=false --set resolved_at=... --set resolved_note='...' --set false_escalation=true`. The `--require-status user_gated` guard prevents clobbering a sibling's concurrent mutation; the patcher re-reads to confirm persistence.
7. **Write an action journal** to `commons/journals/ocas-custodian/YYYY-MM-DD/esc-loop-<ts>.json` (json.dump, unique filename) recording the resolution + live evidence.

**Key lesson:** a `user_gated`/`escalation_needed` issue for this fingerprint is NOT automatically actionable just because a prior scan re-opened it. The re-open itself may have been the trap. Always re-derive live state (creds file + `hermes cron run`) before either persisting OR resolving. If live state is healthy, RESOLVE as a false escalation — do not leave it open (leaving it open perpetuates the noise every future scan). Distinguished from the genuine-defect case: there, the token is GENUINELY absent from the file across re-runs and the issue correctly stays open/user_gated until owner re-auths.

## Relationship to other gotchas
- **INVERSE of mask-gap** (`references/monitor-list-exit1-mask-gap.md`): there, the KeyError was persistent → persist new issue. Here, it was a refresh race → do NOT persist.
- Same family as `references/escalation-false-recovered-note-trap.md` (verify live before overwriting a "recovered" note) and the stale-premise guard (Step 8b): re-derive live state, don't trust a single probe.
- The generic rule "re-run the actual script to confirm stale-vs-active" (deep-scan classification) applies — but the *file-has-valid-token* check is the specific discriminator that separates a refresh race from a true missing-token defect.
