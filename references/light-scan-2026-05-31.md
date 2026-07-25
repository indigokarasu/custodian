# Light Scan — 2026-05-31 23:30 PT

## Errors Detected

### HTTP 429 Rate Limiting (7 jobs — all transient)
- `custodian:light` — 03:29
- `spot:watch-sweep` — 06:05
- `praxis:journal_ingest` — 06:43, 07:26, 07:55 (3 occurrences)
- `ocas-finch:daily` — 07:09
- `look:update` — 09:24 (job is DISABLED — stale error)
- `Backup Hermes Sessions to GitHub` — 03:35
- `corvus:deep` — 10:13

All transient, self-resolving. Tier 2. The recurrence pattern across multiple jobs in a short window (06:00–10:00) suggests upstream rate limit surge, not individual job issues.

### `ocas-finch:weekly` — HTTP 401 (Tier 3, escalated)
- First error: 08:45 — 429 from OpenRouter (retryable)
- Then: 09:09 — 401 from `https://app.manifest.build/v1/` (custom provider)
- Fingerprint: `oc_http_401_manifest_provider`
- Already tracked in issues.jsonl as `oc_finch_weekly_manifest_401_20260531`

### `email:check` — Auth Failure (Tier 3, NEW)
- `last_status=error` at 22:59
- Script: `<hermes-home>/scripts/email_check.py`
- Error: `google.auth.exceptions.RefreshError: invalid_grant: Token has been expired or revoked`
- Root cause: Script imports `from google_auth_mcp import get_gmail_service` — this module doesn't exist as a standalone Python module. The credentials directory at `<gworkspace-creds>/credentials/` has valid files (<user-google-email>.json, 2868 bytes, updated 18:04).
- The script's auth path is broken — it can't authenticate via either the old token file path or the MCP credentials.
- **This is NOT a transient error** — it will recur every run until the import path is fixed.
- Created issue: `oc_email_check_auth_failure_20260531`

### Gateway Restarts (5× today)
- SIGTERM at: 22:08, 22:14, 22:27, 22:29, 23:12
- After each restart: MCP servers (mempalace, stealth-browser, google-workspace) fail to reconnect
- Known pattern — MCP failures are noise during restart recovery

### Config Warning
- `config.yaml` line 635: `mcp: null` → triggers "empty section" warning on every gateway restart
- Should be `mcp: {}` or the line removed
- Tier 2 — cosmetic but noisy

## System Health
- Gateway: OK | Disk: 75% | All 53 skills initialized

## Notes
- `rally:research` shows `last_status=error` from May 28 — NOT today's issue, stale
- `look:update` is DISABLED — 429 error from 09:24 is stale, no action needed
- `elephas:ingest` last_status=ok at 22:12 — running normally
- `finch:work` and `finch:scan` both ok — the finch:work task-list.json path issue from 2026-05-30 appears resolved (it uses `<hermes-home>/commons/data/ocas-finch/task-list.json` which exists)