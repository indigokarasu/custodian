# Credential Leak via Backup Commit Pattern

## Incident: 2026-06-14

**Commit:** `ed1271e1b70fffde921b22508a42092942afd16a` — "backup: 4 files — 2026-06-14 13:00 UTC"
**Repo:** `indigokarasu/indigo` (private, monitored by GitGuardian)
**Trigger:** GitGuardian alert "4 internal incidents detected — Generic High Entropy Secret"

## Root Cause

A backup cron job (`<hermes-home>/scripts/backup_hermes_config.py` or similar) commits the entire Hermes config directory to a git repo, including live credential files:

| File | Credentials Exposed |
|------|---------------------|
| `config/.env` | 30+ secrets: Telegram bot token, OpenRouter API key, Facebook/Instagram app secret & access token, Spotify client secret, Alpaca paper trading keys, Cloudflare API token, ElevenLabs API key, GitHub token, Groq API key, NVIDIA API keys (2), Pika dev key, Trello API key/secret/token |
| `config/auth.json` | Nous Research OAuth access/refresh tokens, agent keys (JWT) |
| `credentials/auth/nous-auth.json` | Additional Nous auth tokens |

**Clean:** `commons/data/ocas-sands/config.json` — no secrets

## Detection

- **GitGuardian** monitors the private repo and detects "Generic High Entropy Secret"
- **Security monitor cron** (`<hermes-home>/scripts/security_monitor.py`) polls Gmail for GitGuardian alerts
- Classified as **LOW** by keyword-based severity (no "secret leak"/"exposed credential" in subject) but **actual severity: HIGH** — real production credentials in git history

## Fix Pattern

1. **Rotate ALL credentials** in the compromised files immediately
2. **Add `.gitignore` entries** to prevent future commits (safe, non-force-push, can be applied immediately):
   ```bash
   echo -e "\n# Secrets - never commit auth/credential files\nconfig/auth.json\ncredentials/\nconfig/credentials/\n*.auth.json" >> .gitignore
   git add .gitignore && git commit -m "fix: exclude auth/credential files from git"
   git push origin main
   ```
3. **Analyze token expiry before history rewrite** — not every committed secret is still live:
   - `access_token` fields: Check `expires_at` — many expire in minutes (Nous tokens: 899-second TTL)
   - `refresh_token` fields: Unknown expiry — treat as LIVE until confirmed revoked by provider
   - **Rotation timing**: Rotate provider (portal.nousresearch.com, Cloud Console) BEFORE rewriting history, otherwise the new tokens get exposed by the next backup
4. **Rewrite git history** (only after token rotation) if secrets span multiple commits:
   ```bash
   git filter-repo --path config/auth.json --path credentials/auth/nous-auth.json --invert-paths
   git push origin --force --all
   ```
   **Prefer `git filter-repo`** over `git filter-branch` — faster, handles tags/LFS correctly, no stale refs. Requires `pip install git-filter-repo`.
   **Last resort for most recent commit only**: `git reset --hard HEAD~1` then force-push (loses one commit of non-secret changes too).
5. **Check remote URL for embedded credentials** — `git remote -v` may show PAT in URL (`https://user:***@github.com/...`). Rotate and update url:
   ```bash
   git remote set-url origin https://github.com/indigokarasu/indigo.git  # remove embedded token
   ```
6. **Verify GitGuardian incidents resolve** after rotation + history rewrite
7. **Update backup script** to never `cp` secret files into the repo

## Custodian Integration

### Scan Check
During `custodian.scan.deep` or light scan, check backup commit messages for credential files:
```bash
# Find recent backup commits that may include secrets
git log --oneline --grep="backup:" -20 --name-only | grep -E "\.env$|auth\.json$|nous-auth\.json$|credentials/"
```

### Fingerprint
- **Name:** `oc_backup_commit_credential_leak`
- **Pattern:** Backup commit message + credential files in diff
- **Severity:** Tier 1 (auto-fix: add to .gitignore, rewrite history if recent)
- **Confidence:** High if GitGuardian alert correlates

### Auto-Fix (Tier 1)
1. Add `config/.env`, `config/auth.json`, `credentials/auth/*.json` to backup repo's `.gitignore` and push immediately
2. Analyze committed secrets for expiry: `access_token` may be expired (check `expires_at`), but `refresh_token` is likely still valid
3. If commit is most recent AND secrets are rotated: `git reset --hard HEAD~1` and force-push
4. If secrets span multiple commits OR tokens are still live: escalate to <operator> for provider-side rotation BEFORE history rewrite

## Prevention

- **Backup script must filter secrets** — never commit `.env`, `auth.json`, `*credentials*`, `*.key`, `*.pem`, `*token*`
- **Use `.gitignore` in backup repo** — track config structure, not secret values
- **Separate secret storage** — use 1Password, HashiCorp Vault, or Hermes credential pool (not git)
- **Pre-commit hook** — run `ggshield` or `trufflehog` on backup repo before push

## Related References

- `references/cron-script-path-security-model.md` — backup script path issues
- `references/known-script-auth-issues.md` — auth failures in cron scripts
- `references/critical-pitfalls.md` — general cron pitfalls