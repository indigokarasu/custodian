# Escalation / Deep-Scan Pitfalls — learned 2026-07-13 deep scan

Operational pitfalls surfaced during a deep scan that the SKILL.md body could not absorb
(SKILL.md is over the 100K hard limit). These are durable; fold into SKILL.md Gotchas once it is
split. All confirmed against the 2026-07-13 run (66 enabled error jobs, profile `indigo`).

## 1. `find_missed_user_gated_jobs.py` "ACTION: pause" is STALE guidance — do NOT follow it for provider outages
The script ends with `ACTION: pause each MISSED job (enabled=false, state=paused) and append its id
to the recommended_issue's jobs_paused.` This predates the **2026-07-11 correction** that
provider/auth/credit/endpoint outages must NOT pause recurring jobs.
- When the MISSED jobs carry provider-outage fingerprints (Nous 401, OpenRouter 402, owl-alpha 404,
  Google 403/401), treat the script's "missed" list as an **enrollment/tracking signal only** — do
  NOT auto-pause.
- Only pause MISSED jobs that are genuinely futile (deterministic code bug, missing script).
- Confirmed 2026-07-13: the script flagged 14 `token_expired` jobs as MISSED and printed "pause",
  but all were already covered by the `user_gated` issue
  `oc_provider_auth_token_expired_20260712T040120` (jobs_paused: 0, correctly left running); pausing
  would have violated the correction.

## 2. `parse_issues_jsonl.py` reads the SYNC COPY, not the authoritative `issues.jsonl`
- `parse_issues_jsonl.py` reads
  `<hermes-home>/profiles/<profile>/commons/journals/ocas-custodian/issues.jsonl` (a lagging sync
  target).
- `verify_escalation_state.py` reads
  `<hermes-home>/profiles/<profile>/commons/data/ocas-custodian/issues.jsonl` (authoritative).
- Counts differ (e.g. 22 vs 6 open). For reconciliation AND writes, ALWAYS use the `commons/data/`
  path. Never treat the journals-copy count as ground truth.

## 3. Live-vs-stale test for provider-auth error storms (decisive)
When many jobs 401/402/404, settle live-vs-stale with:
1. **You are proof.** The Custodian scan you're running executes on the default provider/model. If
   you're producing output, the token/route is not globally dead (caveat: a fallback route may
   differ — corroborate with #2).
2. **Scan the live gateway log for successful completions in the last hour:**
   `grep -nE "response ready|Sending response" <hermes-home>/profiles/<profile>/logs/gateway.log | tail`
   (NOT `<hermes-home>/logs/gateway.log` — stale/rotated; find the live one with
   `find <hermes-home>/profiles/<profile>/logs -name "*.log" -mmin -90`).
   If real completions landed recently, the route works.
3. **Error age from `last_run_at`** (parse with `datetime.fromisoformat(s).astimezone(timezone.utc)`;
   never compare the raw offset string to a UTC `now` — see
   `references/jobs-json-timestamp-offset-misread-pitfall.md`).
- If the provider is live → errors are stale → **leave jobs running, do NOT pause.** Full recipe:
  `references/live-vs-stale-provider-error-recipe.md`.
- Confirmed 2026-07-13: 66 error jobs while the gateway served Telegram LLM responses 00:xx–01:20 PDT
  and the deep scan ran fine on nous/hy3:free — all provider-cluster errors stale, correctly left
  running.

## 4. no_agent exit-1 with a DOUBLED path (`<HERMES_HOME>/home/.hermes/...`) = skill-code `Path.home()` bug
- A no_agent script failing with `FileNotFoundError: '.../home/.hermes/commons/...'` (note the
  doubled `home/.hermes` segment) has a `Path.home()`/`expanduser` resolution bug inside the SKILL
  code — it prepends `HERMES_HOME` to a `~/`-style path.
- Custodian MUST NOT edit skill package files, so this cannot be auto-fixed.
- **Resolution:** pause the job (valid narrow-pause — deterministic/futile retry) + escalate the
  code bug to the skill maintainer. Recommended fix: build paths from `HERMES_HOME`, already exported
  by the `rr_*.sh` wrappers.
- Confirmed 2026-07-13: `weave:sync-contacts` + `weave:enrichability-recalc` failed this way
  (`google_sync.py` / `recalculate_enrichability.py`); paused + escalated as
  `oc_weave_home_path_bug_20260713T091255Z`.

## SKILL.md size note
SKILL.md is 105,132 chars (> 100K hard limit). New operational pitfalls MUST go in `references/`
until SKILL.md is split (the skill's own `references/` directory is the intended home for detailed
pitfalls). A future `custodian.update` from the plugin repo will NOT carry local reference edits —
treat these files as local operational knowledge.
