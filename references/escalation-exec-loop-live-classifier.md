# Escalation Execution Loop — Live Classifier Recipe

Companion to `scripts/escalation_exec_pause_reconcile.py` and
`references/escalation-execution-loop.md`. Captured 2026-07-10 during a real escalation loop
that found 5 open user-gated issues, 80 of them inverse-gotcha (issues.jsonl `jobs_paused`
populated but jobs.json showed the jobs still `enabled+erroring`).

## The core lesson

When executing the escalation loop's fix step, **classify and pause from the LIVE `jobs.json`
`last_error`, NOT from the issue's `jobs_paused` list or its assigned `fingerprint`.** The issue
metadata is unreliable: prior passes wrote `jobs_paused` claiming the jobs were paused, but never
actually edited `jobs.json`. Trust `jobs.json` as the source of truth.

## Execution recipe (also encoded in the script)

1. Load `jobs.json`. For every `enabled` job with `last_status=error` or non-empty `last_error`:
2. Classify by `last_error` keywords into buckets: `nous` (401/key/token_expired), `openrouter`
   (402/credits), `google403` (403/forbidden), `owl` (404/owl), `transient` (Nvidia
   `ResourceExhausted` rate-limit).
3. **Pause** every user-gated bucket job (`enabled=false, state=paused, paused_at, pause_reason`).
4. **Leave running** the `transient` bucket (rate-limit self-clears) — do NOT pause.
5. Reconcile `issues.jsonl`: for each open issue, set `jobs_paused` to the ACTUAL paused ids whose
   live error matches its bucket; keep `status=user_gated, escalation_needed=true`; never set
   `resolved` (pausing is mitigation, not a fix).
6. Flag duplicate issues (same bucket, overlapping jobs) for fold — do not double-pause.

## Three traps that bit this session

### (a) Masked subprocess-wrapper errors
A `no_agent` monitor that wraps a subprocess shows only `Script exited with code 1` in
`last_error` — the REAL error (e.g. Google Tasks 403 OAuth) is hidden inside the wrapper.
**Special-case by job name**: `monitor:list` is a Google 403 failure even though its `last_error`
has no `403` keyword. A naive keyword classifier leaves it running as "unknown" — wrong.

### (b) Error fingerprint DRIFT
A job filed under `oc_http_404_model_deprecated` may, later, fail on Nous 401 or OpenRouter 402
(the owl model got fixed/updated, but the job now dies on provider auth). Re-derive the bucket
from the LIVE error every run — don't trust the issue's historical `fingerprint`. If the 404 is
gone but the job is still paused for a different reason, keep the issue open with a note to verify
the original root cause after the provider recovers.

### (c) Duplicate issues, same provider
`oc_default_provider_token_expired` was a DUPLICATE of `oc_nous_401_key_invalid` — both route
through `provider=nous, model=tencent/hy3:free`; most of its jobs were Nous 401s. Fold duplicates
into one issue (root cause stays open under the surviving issue); resolve the duplicate with a
cross-reference note. Also: some jobs listed in an issue were `enabled` with EMPTY `last_error`
(healthy) — remove them; do not pause healthy jobs.

## Resume-vs-pause caution
A prior pass had RESUMED these jobs earlier the same day (backup `jobs.json.bak-resume-provider-
recovered-...`) believing the provider recovered — but they failed again. The verify-live-first
rule cuts both ways: only resume on a CONFIRMED live probe; if a job is currently failing, pause
it. Pausing a burning job is the correct mitigation; leaving it running just re-floods logs.

## What the user must still do (re-enable on recovery)
Nous: add credits / rotate key (portal.nousresearch.com). OpenRouter: top up credits.
Google Tasks 403: re-auth OAuth/scope. Owl-404: verify model after provider recovery.
