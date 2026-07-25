# Redaction-placeholder corruption of skill source files

**Fingerprint:** `oc_<skill>_redaction_placeholder_syntax_error` — code-defect, fixable by the escalation loop. **NOT user-gated** (distinct from auth/token failures).

## Pattern
A secret-redaction / scrub transform runs over the environment's files to strip secrets from logs and stored configs. If it mis-fires on a *source* file (e.g. a skill's `scripts/*.py`), it substitutes a literal placeholder token into the code itself, e.g.:

```python
'client_secret=<GOOGLE_OAUTH_CLIENT_SECRET>('client_secret', ''),
```

instead of the intended:

```python
'client_secret': token_data.get('client_secret', ''),
```

This breaks the Python literal → `SyntaxError: unterminated string literal` (detected at the line). The cron job then fails with bare `Script exited with code 1` plus a traceback naming the skill source file (unlike a masked no_agent wrapper, the traceback here is visible in `last_error`).

Confirmed 2026-07-15: `ocas-weave/scripts/google_api.py:35` corrupted this way; killed `weave:sync-contacts` (`rr_weave_sync.sh` → `google_sync.py` import).

## Why it appears in the escalation loop
The job shows in `verify_escalation_state.py`'s enabled+erroring list and as a UNKNOWN job in `find_missed_user_gated_jobs.py`. **Prior light scans may NOT have escalated it** if it only just occurred (fresh fault, no open issue). The loop's proactive enabled+erroring inspection is what catches it. The old weave path-bug issues (`oc_weave_home_path_bug`, `oc_weave_skill_path_bug`) are unrelated — this is a new fingerprint.

## Detection
- `python3 -m py_compile <file>` → confirms SyntaxError + exact line.
- `grep -rn '<[A-Z_]*>' <file>` (or search_files for `GOOGLE_OAUTH_CLIENT_SECRET` / any `<...>` token) → finds the leaked placeholder. A placeholder token appearing in executable source is the tell; compare against the line above (symmetric dict entry is the intended shape).

## Fix
Restore the intended code (the symmetric `.get()`-style default). Do **NOT** "fix" by hardcoding a real token, and do **NOT** copy credential files manually. The redaction artifact is never the real secret — it's a scrubber placeholder that leaked into source.

## Verification (exact-env re-run — `py_compile` alone is insufficient)
Replicate the wrapper the cron job invokes:
```bash
export HERMES_HOME=<hermes-home>/profiles/indigo
export AGENT_ROOT=<hermes-home>/profiles/indigo
if [ -f "$HERMES_HOME/.env" ]; then set -a; . "$HERMES_HOME/.env" 2>/dev/null || true; set +a; fi
timeout 110 <hermes-venv>/bin/python <hermes-home>/profiles/indigo/scripts/<wrapper.sh>
# or the worker directly:
<hermes-venv>/bin/python <hermes-home>/profiles/indigo/skills/<skill>/scripts/<worker.py>
```
Expect `EXIT_CODE=0` with real output (e.g. the weave fix re-ran with 981 inbound upserted, 587 outbound pushed, 0 failures). If clean, mark `status: resolved`, clear any carried `user_gated: false`.

## Persistence
Commit the source fix to the skill's git repo (`git -C <skill_dir> add -A && git -C <skill_dir> commit`) so a subsequent scrubber pass doesn't re-corrupt a stale copy. Write the issue to the **authoritative profile `issues.jsonl`** (`…/commons/data/ocas-custodian/issues.jsonl`, NOT the stale `…/commons/journals/…` copy) with `verified` evidence, then resolve.

## Distinction (do not misclassify)
This is a **CODE defect**, never an auth failure. Do NOT confuse with `invalid_grant` / `token_expired` / `oc_google_oauth_token_revoked` (which need browser re-auth and stay user-gated). A `SyntaxError` in a skill source file is always source corruption — fixable here, not waiting on <operator>.