---
name: escalation-gap-taxonomy-token-and-oauth-discriminator
license: MIT
description: Two escalation-loop false-signal traps confirmed 2026-07-16 — (1) journal→issues gap scans mis-flag transient pattern-taxonomy tokens as missing escalations; (2) OAuth user-gated classification must check for any non-interactively recoverable stored credential. Worked example: oc_taste_spotify_token_missing_20260713.
---

# Escalation-loop gap + OAuth user-gated discrimination (2026-07-16)

## Trap 1 — transient pattern-taxonomy tokens are NOT escalation gaps

When you run a journal→issues gap check (Step 8b / 8b-variant / `scan_escalation_journal_gaps.py`), you collect every fingerprint-like token (`oc_[a-z0-9_]+`) from journals flagged `escalation_needed:true` in the window, then report any token with NO corresponding `issues.jsonl` entry as a "gap" to persist.

This regex-over-journals approach has a blind spot: it also captures **pattern-classification taxonomy names** that appear inside `error_job_detail[]` blocks but were classified `transient` (cf=0, `action:transient`) and therefore never escalated. They correctly have NO `issues.jsonl` entry. Persisting them creates a **phantom escalation**.

Confirmed tokens (2026-07-16 light/deep scans):
- `oc_gateway_interpreter_shutdown_transient` — appears in `error_job_detail` with `"cf": 0`, `"action": "tran..."`. Gateway SIGTERM/futures-shutdown class. Transient, not escalated.
- `oc_cron_no_agent_exit_1_noop` — named Tier-2 surface-only pattern (no-op exit 1). Never escalated.

**Guard:** Before persisting any "missing" token as a gap, verify it is a real escalated fingerprint, not a taxonomy name. Heuristics:
- If the token appears only inside an `error_job_detail` / `previous_scan_delta` block with `cf:0` or `action:transient` → it is a classification label, NOT an escalation. Skip.
- Cross-check against the known transient-pattern table in SKILL.md (Non-Fatal Error Patterns). If the token names a known-transient pattern, skip.
- Only persist when the journal's top-level `escalation_needed:true` is attached to a concrete job failure with a live signature AND no issue entry exists for that fingerprint.

This is distinct from (and sits on top of) the `scan_escalation_journal_gaps.py` FALSE-POSITIVE GUARD, which already filters already-`resolved`/`duplicate` issues. Add the taxonomy-token filter on top.

## Trap 2 — OAuth user-gated discriminator: stored-recoverable vs truly-interactive

When an OAuth/token fingerprint is flagged, the naive conclusion is "requires <operator> interactive auth → user-gated." That is correct ONLY when no stored credential permits non-interactive recovery. Always check the recovery path before classifying:

**Recoverable non-interactively (do NOT leave user-gated):** the credential store holds a valid `refresh_token` (or equivalent) that a script can exchange for a fresh access token without <operator>. Example: Google `tasks_monitor.py` `KeyError: 'access_token'` case (b) — creds file has `access_token` ABSENT but a valid `refresh_token` → `refresh_token()` recovers non-interactively; apply the durable code fix. (See `references/monitor-list-access-token-recurrence-durable-fix-2026-07-15.md`.)

**Truly user-gated (leave open, do not mark resolved):** NO stored credential exists anywhere that permits recovery. The only path is the interactive Authorization Code flow (browser + localhost callback) that requires <operator>.

### Worked example: `oc_taste_spotify_token_missing_20260713` (confirmed 2026-07-16)

- Issue: `taste:sync-spotify` (job `e0a126b6c9f7`, `no_agent`, `enabled:false`) fails — `spotify_history_puller.py` raises `Missing Spotify credentials: SPOTIFY_REFRESH_TOKEN (present: SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET)`.
- Mechanism: `spotify_history_puller.py` reads ONLY `SPOTIFY_REFRESH_TOKEN` from the env (`.env`). There is a SEPARATE interactive helper, `spotify_auth_helper.py`, that performs the browser OAuth flow and writes tokens to `commons/data/ocas-taste/music/spotify_token.json` and `~/.cache-spotify-taste` — but the cron wrapper `rr_taste_sync_spotify.sh` does NOT read those files. So even after <operator> runs the helper once, the cron job still won't have the env token unless `.env` is updated.
- Verification performed (live):
<<<<<<< Updated upstream
  - `SPOTIFY_REFRESH_TOKEN` ABSENT from `<hermes-home>/profiles/indigo/.env` (only CLIENT_ID / CLIENT_SECRET / REDIRECT_URI present).
=======
  - `SPOTIFY_REFRESH_TOKEN` ABSENT from `~/.hermes/profiles/indigo/.env` (only CLIENT_ID / CLIENT_SECRET / REDIRECT_URI present).
>>>>>>> Stashed changes
  - `spotify_token.json` ABSENT; `~/.cache-spotify-taste` ABSENT.
  - No `SPOTIFY_REFRESH_TOKEN` anywhere under the profile.
- Conclusion: **truly user-gated.** No stored credential to refresh non-interactively. Requires <operator> to complete the interactive Spotify OAuth flow and populate `SPOTIFY_REFRESH_TOKEN` in `.env`. The job is already disabled. Per the honesty rule, do NOT mark the issue `resolved` — it stays `user_gated` + `escalation_needed:true` until <operator> acts.
- Note for future loops: this issue recurs under TWO naming variants — `oc_spotify_token_missing` and `oc_taste_spotify_token_missing_20260713` — both point to the same `user_gated` entry. When gap-matching, treat them as the same fingerprint; do not create a duplicate.

## Escalation-loop output for this run
- 1 open issue (`oc_taste_spotify_token_missing_20260713`) — verified still user-gated, no fix.
- 0 true journal→issues gaps (the 2 "missing" tokens were taxonomy false-positives).
- 0 unresolved proposals.
- Action: wrote evidence journal, returned `[SILENT]`. No issue state changed.