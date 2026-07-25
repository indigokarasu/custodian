# secret-audit.md — Custodian secret-leak audit & migration

## What it finds

`custodian.secrets.audit` (script: `scripts/secret_audit.py`) scans the
Hermes tree for **API keys / tokens / client secrets / passwords** stored
INLINE in files — the "wrong places" — instead of the canonical secret store.

Scan roots:
- `~/.hermes/config.yaml`
- `~/.hermes/profiles/*` (each profile's `config.yaml` + `skills/` + `scripts/` + `plugins/`)
- `~/.hermes/plugins/*` (live plugin / MCP server code)

Excluded: `node_modules`, `.bun`, `gentube-output`, `state-snapshots`,
`backups`, `.venv`, `references/`, `assets/`, profile `home/` (userland
caches like `home/.bun`), and any `.git`/`.git/` tree. (Lessons from the
2026-07-24 first pass: scanning all 279k files or the profile `home/`
bun cache produced 14k false positives — TypeScript `.d.ts` / build JSON
matching loose prefixes. Scope is the committable/loadable surface only.)

Detection:
- **Strong-prefix token shapes** (no context needed): `sk-…`, `sk-ant-…`,
  `cfat_…`, `alv2_…`, `ghp_/gho_/ghs_/ghu_/ghr_…`, `xox[baprs]-…`,
  `AKIA…`, `GOCSPX-…`, `AIza…`, `ya29.…`, `ck_…`, `sk_live_…`,
  `rk_live_…`, and `-----BEGIN … PRIVATE KEY-----` blocks.
- **Secret-bearing key + high-entropy value**: lines matching
  `api_key`/`access_token`/`client_secret`/`password`/`authorization`/etc.
  with a value of Shannon entropy ≥ 3.0 bits/char and length ≥ 16.
  Pure Python identifiers (`args`, `api_key`), ALLCAPS constants
  (`BLAND_DEFAULT_VOICE`), and dotted refs (`args.api_key`) are rejected.

Every reported value is **masked** (`abcd…xy`). The script never prints or
returns a full secret.

De-dup: findings are grouped by **(type, masked preview, length)** so one
secret that appears in N files (e.g. `GOCSPX-…` in 10 sites) reports
once with all `sites` listed. The 2026-07-24 live run found
**10 unique secrets across 21 leaf sites**.

## Why this matters (correct framing)

Earlier drafts wrongly assumed a GitHub-push leak. Confirmed 2026-07-24:
**there is NO git repo at `~/.hermes` and `config.yaml` is NOT tracked.**
So the risk is **plaintext-at-rest in predictable paths**, not a commit/push.
Caveat: anything under a real OCAS skill git repo (e.g. if a skill dir is
itself a git checkout that gets pushed to `<agent-handle>/*`) WOULD be a
push risk — the scanner still covers those because it walks `skills/`.

## The canonical secret store (where secrets SHOULD live)

Confirmed from `/usr/local/lib/hermes-agent` source:
1. **`~/.hermes/profiles/<profile>/.env`** — loaded into `os.environ`
   by the gateway at startup (`hermes_cli/env_loader.py`,
   `load_hermes_dotenv`). This is the primary store. `.env` is gitignored
   by design. (`HERMES_HOME` selects the profile; here
   `HERMES_HOME=~/.hermes/profiles/indigo`.)
2. **`secrets.bitwarden.access_token_env`** (default `BWS_ACCESS_TOKEN`)
   — `agent/secret_sources/bitwarden.py` reads the token from that env var.
3. **MCP `mcp_servers[*].headers`** support **`${ENV}` indirection**
   (confirmed in `tests/cli/test_cli_mcp_config_watch.py`:
   `"Authorization": "Bearer ${MCP_GH_API_KEY}"`). So inline
   `Authorization: Bearer cfat_…` should become
   `Authorization: ${INDIGO_CLOUDFLARE_API_TOKEN}`.
4. **`security.redact_secrets: true`** — an in-file redaction gate.
   Confirmed PARTIAL: it redacted the embeddings `api_key`
   (`«redacted:sk-…»`) but left Cloudflare/Google/Aion/Composio values
   raw. Do NOT assume `redact_secrets` covers everything.

## Commands

```
# read-only audit, write masked JSON report
python3 scripts/secret_audit.py --mode audit --json /tmp/secret_audit.json

# migration PLAN only (no writes)
python3 scripts/secret_audit.py --mode remediate

# perform the SAFE subset of the migration
python3 scripts/secret_audit.py --mode remediate --apply
```

## Remediation rules (non-destructive)

`--mode remediate --apply` performs ONLY the safe subset:
- **MCP `headers`** (`config-mcp-inline-header` class): replace the literal
  with `${ENV}` indirection + append the env-var NAME (not value) to
  `.env` as a manifest comment. The raw value is sourced from the live
  gateway `os.environ` at apply time, NOT re-read from disk (the scan is
  masked). Backs up `config.yaml` to `config.yaml.custsecbak_<ts>` first.
- **`.env` writes are append-only**: never overwrites an existing key.
- **Credential-blob `.json`** (`auth.json`, `google_*.json`,
  `[Google OAuth credentials]*.json`, `credentials/free-llm-apis.json`)
  and **hardcoded `.py` literals** (`scripts/google_auth*.py`,
  `skills/infrastructure/gcloud-cli/scripts/google_oauth_finish.py`) are
  flagged **MANUAL** — they require code refactor (read via `os.getenv`)
  or relocation, not a blind inline edit. Re-run `audit` to confirm 0
  inline hits after manual cleanup.

## Post-migration: test every secret, delete dead ones

You required: after migration, all secrets must be **tested**, and
**broken/expired/revoked** ones **deleted** (no value in storing stale
secrets). Procedure (agent-driven, not auto):
- For each migrated secret, probe the live endpoint / token validity:
  - `cfat_` (Cloudflare MCP): `curl -sH "Authorization: Bearer $TOK" https://api.cloudflare.com/client/v4/user/tokens/verify`
  - `alv2_` (Aion Labs): hit `https://api.aionlabs.ai/v1/...` with the key.
  - `sk-ant-` / `sk-` (Anthropic/OpenAI): model list / auth probe.
  - `GOCSPX-` (Google OAuth client secret): only meaningful with the
    matching client_id — verify the OAuth client still exists in Google
    Cloud Console; if the client was deleted, the secret is dead → delete
    the blob + literal.
  - `ya29.…` (Google OAuth access token): short-lived; if expired,
    refresh or delete the blob.
  - `ck_` (Composio): probe `https://connect.composio.dev/mcp`.
- Any probe returning 401/403/expired/revoked/invalid → **delete** the
  secret from `.env` AND every flagged site (blob/json/literal). Do NOT
  rotate silently — rotation is a <operator> decision; deletion of a dead
  secret is within scope.
- Re-run `custodian.secrets.audit` → expect 0 inline hits after cleanup.

## Cron integration

Optional scheduled audit (dry-run report; escalation if inline secrets found):
```
cronjob action=create name="custodian:secrets-audit"
  schedule="0 3 * * *"
  prompt="Run: python3 ~/.hermes/profiles/indigo/skills/ocas-custodian/scripts/secret_audit.py --mode audit --json /tmp/secret_audit_<date>.json . If total_findings > 0, report the by_type breakdown and flag for <operator>; do NOT auto-remediate."
  skills=["ocas-custodian"]
```
