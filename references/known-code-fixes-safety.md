# Known Code Fixes, Safety & Code Surface

## Tier 4 Code Fixes + MCP Cascade

- Tier 4 code fixes + MCP cascade triage: `references/known-code-fixes-and-cascade.md`.
  Secret-redaction pattern that corrupts skill source — fix directly, do NOT leave
  user-gated: `references/redaction-placeholder-source-corruption.md`.

## Safety Envelope + Registry

- Safety envelope + Tier 1 auto-fix registry: `references/fix-safety.md`. Background-task
  conformance + registry health: `references/conformance.md`. Activity model (rebuilt each
  deep scan from a 14-day window) + schedule optimization:
  `references/schedule-optimization.md`. Storage/platform:
  `references/background-tasks.md`, `references/platform-compatibility.md`.

## Script Path Security

- Script path rejected under a profile → scripts must live at
  `<hermes-home>/profiles/<profile>/scripts/<basename>`:
  `references/script-path-security-block-pattern.md`.

## Google OAuth

- `oc_google_oauth_client_deleted` (client deleted in the Cloud Console) and
  `oc_google_oauth_token_revoked` (refresh token expired, `invalid_grant`), affecting only
  direct-credential jobs `email:check` and `monitor:list` —
  `references/google-oauth-client-deleted-pattern.md`. **Sequential rule:** after fixing a
  `googleapiclient` `ModuleNotFoundError`, immediately re-check for token revocation
  (Tier 1 → Tier 3). Wrapper-cascade masking:
  `references/subprocess-cascade-oauth-masking.md`.

## Env-Sync Gate

- A Tier 1 fix that looked revalidated did not stop the next occurrence —
  an unguarded loop fall-through plus an urgency guard that measured the token the script
  had just replaced. `references/envsync-budget-fallthrough-and-inoperable-urgency-guard-2026-09-29.md`.

## Naming Trap

- `hello_operator/server.py` names the class serving `/v1/chat/completions`
  `Router`, so `ps | grep router` cannot see it and "the gateway never restarted" is
  evidence about a *different* process than a `hello-operator.service` restart.
  `references/hello-operator-envsync-restart-drops-inflight.md`; for unexplained unit
  restarts read `/root/.hello-operator/stop-forensics.log` first.
