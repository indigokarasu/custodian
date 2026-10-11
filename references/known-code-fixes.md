# Known Code Fixes — Detail

Extracted from SKILL.md 2026-10-11 (D3 token-budget). One-line index lives in
SKILL.md under `## Known Code Fixes`. Read this file **before applying a fix whose
signature matches an entry below** — each entry is a defect seen live, with the
exact file, the mechanism, and the correct handling.

Full Tier-4 code-fix recipes + MCP cascade triage: `references/known-code-fixes-and-cascade.md`.

## Template / path / manifest

- **`send_email.py` template mismatch** (`oc_vesper_template_missing`): only `job_search`
  is a valid template; a caller passing another name raises. Full diagnosis:
  `references/send-email-template-mismatch.md`.
- **Wrong path prefix**: commons paths are `$AGENT_ROOT/profiles/<profile>/commons/...`,
  NOT `$AGENT_ROOT/commons/...`. `references/wrong-path-prefix-in-skill-scripts.md`.
- **Plugin manifest parse spam** (`Failed to parse .../plugin.yaml: expected '<document start>'`):
  stray `...` YAML document-end markers between mapping keys; the plugin never loads.
  Check upstream first (the checkout may just be behind).
  `references/plugin-manifest-yaml-document-marker-corruption.md`.
- **Script-path rejection under a profile**: scripts must live at
  `<hermes-home>/profiles/<profile>/scripts/<basename>`, not inside the skill package.
  `references/script-path-security-block-pattern.md`.

## Security / redaction

- **Secret-redaction pattern that corrupts skill source**: fix directly, do NOT leave
  user-gated — the redaction writes a placeholder over the source and the skill stops
  working. `references/redaction-placeholder-source-corruption.md`.

## OAuth / credentials

- **Google OAuth** `oc_google_oauth_client_deleted` (client deleted in the Cloud Console)
  and `oc_google_oauth_token_revoked` (refresh token expired, `invalid_grant`), affecting
  only direct-credential jobs `email:check` and `monitor:list` —
  `references/google-oauth-client-deleted-pattern.md`.
  **Sequential rule:** after fixing a `googleapiclient` `ModuleNotFoundError`,
  immediately re-check for token revocation (Tier 1 → Tier 3). Wrapper-cascade masking:
  `references/subprocess-cascade-oauth-masking.md`.

## Env-sync / restart traps

- **Env-sync gate**: a Tier 1 fix that looked revalidated did not stop the next
  occurrence — an unguarded loop fall-through plus an urgency guard that measured the
  token the script had just replaced.
  `references/envsync-budget-fallthrough-and-inoperable-urgency-guard-2026-09-29.md`.
- **Naming trap**: `hello_operator/server.py` names the class serving
  `/v1/chat/completions` `Router`, so `ps | grep router` cannot see it and "the gateway
  never restarted" is evidence about a *different* process than a
  `hello-operator.service` restart. `references/hello-operator-envsync-restart-drops-inflight.md`;
  for unexplained unit restarts read `/root/.hello-operator/stop-forensics.log` first.

## Safety / conformance / platform

- **Safety envelope + Tier 1 auto-fix registry**: `references/fix-safety.md`.
- **Background-task conformance + registry health**: `references/conformance.md`.
- **Activity model** (rebuilt each deep scan from a 14-day window) **+ schedule
  optimization**: `references/schedule-optimization.md`.
- **Storage/platform**: `references/background-tasks.md`,
  `references/platform-compatibility.md`.
