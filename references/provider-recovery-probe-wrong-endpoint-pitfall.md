# Provider-Recovery Probe Must Hit the Same Credential the Failing Job Used

**Fingerprint:** refinement of `oc_nous_api_key_invalid` / `oc_provider_auth_token_expired` resolution — a FALSE "recovered" verdict.

**Date confirmed:** 2026-07-13.

## The trap
A cron scan declares a provider/auth issue "resolved / forward-stale" after a live probe returns green. But the probe often hits the **gateway default** (`model.default`, e.g. `tencent/hy3:free`), which is a *different credential* than the one the failing job actually uses.

A single profile config carries MULTIPLE distinct provider credentials:
- top-level `model:` block — `provider: nous`, `base_url: https://chatgpt.com/backend-api/codex`, a `sk-or-v1…` key
- `providers:` dict (e.g. `aion_labs`)
- `auxiliary.*` blocks (vision, compression, tts, …) — each can have its own `base_url`/`api_key`
- `fallback_model` (if present)

A green probe on the default proves nothing about the dead one.

## Worked incident (2026-07-13)
- `weave:overnight-enrichment` (id `df65af7c019d`) FAILED **today** 2026-07-13T02:18 PDT with:
  `RuntimeError: Error code: 401 - ... 'Your API key is invalid, blocked or out of funds. Please go visit the portal to sort that out: https://portal.nousresearch.com'`
- The 16:10 light scan had marked `oc_nous_api_key_invalid_20260712T040120` **resolved / forward-stale** after probing only `tencent/hy3:free` (the working default) and seeing 78+ jobs OK.
- That probe NEVER exercised the `model:`-block Nous key. Live verification at 17:35Z:
  `GET https://chatgpt.com/backend-api/codex/models` with the `sk-or-v1-dc0…` key → **HTTP 401 "Incorrect API key provided"**.
- Result: the job kept failing, and with `next_run_at` next day (2026-07-14T02:00 PDT) and `cf=None`, it would NOT self-clear. The "clean verdict" was wrong for this credential path.

## Rule
Before marking ANY provider/auth issue `resolved` or `forward-stale`:
1. Identify which credential the failing job uses (read the job's `skill`/`script`, and the `model:`/`providers:`/`auxiliary.*` config the skill/script calls).
2. Probe THAT credential's `base_url` with THAT `api_key` directly (`GET <base_url>/models` with `Authorization: Bearer <key>`).
3. Only if the exact failing credential is green is "recovered" valid.

Use `scripts/verify_all_provider_credentials.py` to probe every configured credential at once — it enumerates `model:` block, `providers:`, `auxiliary.*`, and `fallback_model` and reports LIVE/DEAD per distinct (base_url, key) pair.

This is the **Step 8c / Step 9** verify-before-accepting-self-resolved pitfall applied to provider credentials (the skill's existing self-resolved guidance only covers `ModuleNotFoundError` imports).