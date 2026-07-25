# Skill Conformance Checking

On every deep scan: scan `{agent_root}/skills/`, parse each SKILL.md for background task declarations from BOTH sources:
1. `## Background tasks` markdown table — extract `| Job name | Mechanism | Schedule | Command |` rows. **Strip backticks from names** (SKILL.md uses `` `bower:scan` `` but cron stores `bower:scan`). Also strip surrounding whitespace.
2. YAML frontmatter `metadata.hermes.cron` array — extract all `name:` fields under `cron:` (these don't use backticks, but strip quotes: `"` and `'`).

Cross-reference declared tasks against the platform scheduling registry (jobs.json). Missing tasks: Tier 1 fix. Schedule mismatches: Tier 2. Orphaned `custodian:*` jobs: Tier 2. Write `skill_conformance.jsonl` per skill.

**⚠️ Pitfall — Backtick-wrapped names in Background tasks tables:** SKILL.md markdown tables wrap job names in backticks (e.g., `` `bower:scan` ``). Cron job names are stored without backticks (e.g., `bower:scan`). Always strip backticks before comparing declared names against the cron registry. Without stripping, ALL ocas-* skills will falsely appear "missing from cron." Confirmed on 2026-05-30: every ocas-* background task name in SKILL.md tables uses backtick wrapping.

**⚠️ Frontmatter cron name parsing — filter non-job names:** The YAML frontmatter may contain `name:` fields that are NOT cron jobs (API key names like `ALPHAVANTAGE_API_KEY`, MCP server descriptors, OAuth field names). Filter these out before comparing against the cron registry. See the platform-compat reference for the full filtering regex patterns.

**Cron registry health checks** (also on every deep scan):
- **Dead skill references**: verify each job's `skills` array entries exist as directories under `{agent_root}/skills/`. Remove dead entries or delete the job. Tier 1.
- **Dead script references**: verify each job's `script` file exists on disk. Update or delete if missing. Tier 1.

   **⚠️ Pitfall — Relative script paths that are actually in `scripts/` dir:** Cron job `script` fields may contain bare filenames (e.g., `update_voyage.sh`) that don't resolve from the cron runner's cwd. Before flagging as dead, also check `{agent_root}/scripts/<filename>`. If the file exists there but not at the literal path, the fix is to update the job's `script` field to the absolute path `{agent_root}/scripts/<filename>`. This is a safe, reversible Tier 1 fix (backup `jobs.json` first). Confirmed pattern: 12 jobs were fixed this way on 2026-05-30 — all `update_*.sh`, `update_*.py`, `vesper_deliver.py`, `email_check.py`, `weave_health_check.py`, and `elephas_ingest_wrapper.sh` lived in `~/.hermes/scripts/` but were referenced as bare filenames.
- **Duplicate function detection**: group jobs by script path, prompt prefix (200 chars), and display name. Keep canonical (matches SKILL.md name or earliest ID), delete duplicates. Tier 1.

# Skill Initialization

**Pre-check — Does this skill actually need initialization?** Before creating data dirs, check whether the skill's SKILL.md references any data files, config, or storage. Skills that are pure MCP pass-through (like `rapidapi`), utility libraries, or skills whose SKILL.md never mentions `commons/data/` or config files do NOT need initialization. Creating data dirs for them is wasteful and creates phantom state. Heuristic: if the SKILL.md has no `## Background tasks` section AND no `ConfigBase` class AND no references to `commons/data/` or `config.json` in the body, skip initialization entirely.

Uninitialized when: data dir missing, config.json missing, or journal dir missing — AND the skill's SKILL.md actually references these. Sequence (additive only, never overwrite):

1. Create `{agent_root}/commons/data/{skill-name}/` if missing
2. Write default config.json with ConfigBase fields -- only if absent
3. Create `{agent_root}/commons/journals/{skill-name}/` if missing
4. Verify commons/ directory structure exists
5. Run conformance check for background tasks
6. Register missing tasks (Tier 1, subject to parameter availability)
7. Register verify job (15 min delay)
8. Append DecisionRecord

**⚠️ Category/meta directories without SKILL.md are NOT skills:** Directories under `{agent_root}/skills/` that lack a `SKILL.md` file are category/meta folders (e.g., `creative/`, `infrastructure/`, `ocas-bower/`), not actual skills. Do NOT flag them as "uninitialized" or create data dirs for them. Only directories with a `SKILL.md` file are skills that need initialization. See `references/light-scan-2026-05-20.md` for examples.

**⚠️ execute_code blocked in cron mode:** When running as a cron job, `execute_code` is blocked. All skill directory scanning, file existence checks, and JSON parsing during skill conformance must use `terminal(command='python3 << PYEOF ... PYEOF')` with heredoc syntax. See `references/critical-pitfalls.md` pitfall #1.

Do not run the skill's own `{skill}.init` command.

# Token & Auth Escalation

When `oc_google_token_missing` or `oc_google_token_invalid` is detected during a scan:

1. Check `google-workspace-auth` skill (under `infrastructure/`) for the re-auth procedure
2. The init script is: `python3 <hermes-home>/skills/infrastructure/google-workspace-auth/scripts/google_oauth_init.py`
3. This is ALWAYS Tier 3 — requires interactive user action, cannot be automated
4. Write escalation proposal to `references/proposals/` with `escalation_needed: true` for Mentor heartbeat

# Activity Model

Maintained in `activity_model.json`, rebuilt every deep scan from 14-day gateway log window. Confidence per hour: `active_days / total_days` (>= 0.75 high, >= 0.40 med, < 0.40 low). `current_state: active` if hour confidence >= med and in active window, else `quiet`.

Repair rules: quiet = all Tier 1; active = urgent only (failure in last 5 min), defer rest; low confidence = execute but suppress noisy effects. Cold start: `01:00, 07:00, 13:00, 19:00 PT`. Optimization begins after 7 days.

**⚠️ Cron-only traffic pattern:** When all sessions in the 14-day window are `source: cron` or `source: heartbeat` (no user sessions), the activity model will show uniformly high confidence across all hours. This is expected — cron jobs fire around the clock. In this case, `current_state` should be treated as "always active" and the schedule optimization should maintain the existing 6-hour spacing. Do NOT attempt to optimize the schedule based on a uniform cron distribution — there is no meaningful quiet window to target. The model note should explicitly state "all sessions are cron/heartbeat sourced, no user activity detected" so future scans don't waste cycles trying to find quiet windows that don't exist.