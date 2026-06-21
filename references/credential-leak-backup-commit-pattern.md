# Credential Leak via Backup Commit Pattern

## Incident: 2026-06-14

**Commit:** `ed1271e1b70fffde921b22508a42092942afd16a` — "backup: 4 files — 2026-06-14 13:00 UTC"
**Repo:** `indigokarasu/indigo` (private, monitored by GitGuardian)
**Trigger:** GitGuardian alert "4 internal incidents detected — Generic High Entropy Secret"

## Root Cause

A backup cron job (`<hermes-root>/scripts/backup_hermes_config.py` or similar) commits the entire Hermes config directory to a git repo, including live credential files:

| File | Credentials Exposed |
|------|---------------------|
| `config/.env` | 30+ secrets: Telegram bot token, OpenRouter API key, Facebook/Instagram app secret & access token, Spotify client secret, Alpaca paper trading keys, Cloudflare API token, ElevenLabs API key, GitHub token, Groq API key, NVIDIA API keys (2), Pika dev key, Trello API key/secret/token |
| `config/auth.json` | Nous Research OAuth access/refresh tokens, agent keys (JWT) |
| `credentials/auth/nous-auth.json` | Additional Nous auth tokens |

**Clean:** `commons/data/ocas-sands/config.json` — no secrets

## Detection

- **GitGuardian** monitors the private repo and detects "Generic High Entropy Secret"
- **Security monitor cron** (`<hermes-root>/scripts/security_monitor.py`) polls Gmail for GitGuardian alerts
- Classified as **LOW** by keyword-based severity (no "secret leak"/"exposed credential" in subject) but **actual severity: HIGH** — real production credentials in git history

## Fix Pattern

1. **Rotate ALL credentials** in the 3 compromised files immediately
2. **Rewrite git history** to remove commit `ed1271e`:
   ```bash
   git filter-branch --force --index-filter \
     'git rm --cached --ignore-unmatch config/.env config/auth.json credentials/auth/nous-auth.json' \
     --prune-empty --tag-name-filter cat -- --all
   ```
   Or use BFG Repo-Cleaner for larger repos.
3. **Force-push** to remote (private repo only — coordinate if shared)
4. **Verify GitGuardian incidents resolve** after rotation
5. **Update backup script** to exclude secret files via `.gitignore` or explicit filter

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
1. Add `config/.env`, `config/auth.json`, `credentials/auth/*.json` to backup repo's `.gitignore`
2. If commit is most recent: `git reset --hard HEAD~1` and force-push
3. If commit is older: rewrite history (requires coordination)

## Prevention

- **Backup script must filter secrets** — never commit `.env`, `auth.json`, `*credentials*`, `*.key`, `*.pem`, `*token*`
- **Use `.gitignore` in backup repo** — track config structure, not secret values
- **Separate secret storage** — use 1Password, HashiCorp Vault, or Hermes credential pool (not git)
- **Pre-commit hook** — run `ggshield` or `trufflehog` on backup repo before push

## Related References

- `references/cron-script-path-security-model.md` — backup script path issues
- `references/known-script-auth-issues.md` — auth failures in cron scripts
- `references/critical-pitfalls.md` — general cron pitfalls