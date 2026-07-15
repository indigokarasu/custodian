# Provider-Recovery Verification (forward-stale closure check)

When an escalation loop finds issues already marked `resolved` on a "provider
recovered" note, or a probe (`find_missed_user_gated_jobs.py`) flags jobs as
MISSED, verify the provider is ACTUALLY live before re-pausing/re-enrolling.
Two failure modes this catches (both confirmed 2026-07-13):

## 1. Provider misattribution in the resolution note
A forward-stale closure may cite the WRONG provider. Example:
`oc_provider_auth_token_expired_*` was closed because "OpenRouter returned 200",
but the live `token_expired` (code `token_expired`) errors were on `provider=None`
jobs, which route through the DEFAULT provider (Nous `tencent/hy3:free`), a
different provider. The OpenRouter recovery was irrelevant to those jobs.

**Fix:** probe the provider the failing jobs actually use.
- For `provider=None` jobs → the config default: `config.yaml` top-level
  `model:` / `provider:` (here: `provider: nous`, `model: tencent/hy3:free`).
- Probe:
  ```
  hermes chat -q "reply with only the word PONG" --provider nous --model "tencent/hy3:free"
  ```
  A successful "PONG" response = default provider is live → stale errors
  self-clear on next run. Do NOT re-pause/re-enroll those jobs.

## 2. Stale errors vs real misses (find_missed false positives)
`find_missed_user_gated_jobs.py` buckets EVERY enabled+erroring job not in
`jobs_paused` against known fingerprints, ignoring error age. It will report
jobs with 165–312h-old errors (e.g. the 5 OpenRouter/owl-alpha jobs under
`oc_openrouter_402_credits_exhausted_20260706`) as "MISSED" even though the
parent issue is correctly resolved and OpenRouter `/models` returns HTTP 200.

**Fix:** check error age before acting.
```python
from datetime import datetime, timezone
lru = datetime.fromisoformat(job["last_run_at"]).astimezone(timezone.utc)
age_h = (datetime.now(timezone.utc) - lru).total_seconds() / 3600
```
If `age_h` exceeds the cited recovery window (e.g. > 24h past the recovery
timestamp), the error is forward-stale → NO re-pause/re-enroll. The probe is a
leads list, not an enforcement order.

## Script
`scripts/verify_provider_recovery.py` automates both checks: splits enabled+
erroring jobs into STALE (`>= --stale-hours`, default 24) vs RECENT, derives the
default provider/model from `config.yaml`, probes it live via `hermes chat -q`,
and prints a per-bucket recommendation. Run it as the provider-side cross-check
BEFORE re-pausing any job a probe flagged as MISSED.

---

# Durable credential-persistence auto-fix (oc_no_agent_git / gh token)

For `oc_no_agent_git_https_no_credential` / `oc_no_agent_gh_cli_no_token` the
root cause is NOT a missing token — it's that the `no_agent` cron sandbox does
not carry `GH_TOKEN`/`GITHUB_TOKEN` into `gh`/`git`. The token in
`HERMES_HOME/.env` is typically valid. Verified fix path (2026-07-13):

1. Confirm the token works in a clean-env shell (simulating the sandbox):
   ```
   env -u GH_TOKEN -u GITHUB_TOKEN bash -c 'set -a; . $HERMES_HOME/.env; set +a; \
     [ -z "${GH_TOKEN:-}" ] && [ -n "${GITHUB_TOKEN:-}" ] && export GH_TOKEN="$GITHUB_TOKEN"; \
     gh api /users/<user>/events --jq length'
   ```
2. Persist the credential GLOBALLY so `gh`/`git` work without inherited env:
   ```
   GHT=$(grep -oE '^GITHUB_TOKEN=.+' $HERMES_HOME/.env | cut -d= -f2- | tr -d '\r')
   printf '%s' "$GHT" | env -u GH_TOKEN -u GITHUB_TOKEN gh auth login --with-token
   env -u GH_TOKEN -u GITHUB_TOKEN gh auth setup-git
   git config --global credential.helper store
   printf 'https://x-access-token:%s@github.com\n' "$GHT" >> ~/.git-credentials
   chmod 600 ~/.git-credentials
   ```
   **Note:** `gh auth login --with-token` with the token ALREADY in env does NOT
   persist a stored credential (gh says so). Pipe the token into `env -u GH_TOKEN
   -u GITHUB_TOKEN gh auth login --with-token` so gh actually stores it
   (`hosts.yml`), not just uses the inherited env value.
3. Verify in a clean-env subshell: `gh api` = 0, `git ls-remote` = 0, and the
   job's `no_agent` script (e.g. `update_skill.sh <skill>`) exits 0.
4. Re-run the job via the scheduler to confirm end-to-end: `hermes cron run <id>`
   then check `last_status=ok`, `last_error` cleared in `jobs.json`.

This is a Tier-1 auto-fix (credential provisioning, no interactive OAuth
required). Distinct from `oc_taste_spotify_token_missing`, which DOES require
owner's interactive OAuth and is genuinely user-gated.

---

# issues.jsonl reconciliation (safe, preserve everything)

When closing resolved issues / keeping user-gated open, edit `issues.jsonl` with
a brace-depth parser that preserves all 31 (or N) records and only mutates the
target entries:
```python
import json
def parse(p):
    recs=[]; buf=""; depth=0
    for ch in open(p).read():
        buf+=ch
        if ch=='{': depth+=1
        elif ch=='}':
            depth-=1
            if depth==0:
                try: recs.append(json.loads(buf))
                except: pass
                buf=""
    return recs
```
For each resolved: set `status="resolved"`, `escalation_needed=False`,
`resolved_at=now_iso`, `resolved_by="escalation-execution-loop"`,
`resolution_note=<verified fix>`. For each kept-open user-gated: set
`user_gated=True`, `escalation_needed=True`, leave `status` as-is, add
`mitigation_note`. Write all records back (`json.dumps(r)+"\n"` per line). Never
delete entries; never trust a prior scan's "resolved" flag without live
`jobs.json` verification (both directions).
