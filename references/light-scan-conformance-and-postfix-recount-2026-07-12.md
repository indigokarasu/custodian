# Light scan conformance + post-fix recount notes (2026-07-12)

## When this applies

Use during `custodian.scan.light` after checking active cron jobs, uninitialized skills, and applying any Tier 1 fix that can change the live error set.

## Missing skill files: classify by location

When cross-referencing active cron jobs against installed skills:

- Missing `data/` or `journals/` subdirectories for an active referenced skill are operational initialization defects. If the skill directory exists, Custodian may remediate these according to the initialization references.
- Missing `config.json` inside an installed skill package is not equivalent. Creating it would write inside the skill package directory, which Custodian normally must not do. Record it as surface-only / escalation context unless a skill-specific recovery reference explicitly authorizes creation outside the package directory.
- A missing skill directory still requires the archived/merged-skill check before creating anything; if the skill was archived, remove or null the dead cron reference instead of fabricating a skill.

## Recount after Tier 1 fixes

If a light scan applies a Tier 1 fix before writing the journal, re-read or re-derive live state before journaling counts. Example: fixing a broken MCP server package reduced enabled error jobs from 92 to 91 in the same scan. The journal should use the post-fix count and mention the pre-fix symptom in `fixes_applied`, not preserve stale pre-fix counts.

Minimal sequence:

1. Read `jobs.json` and classify live errors.
2. Apply Tier 1 fix.
3. Verify fix directly.
4. Re-read `jobs.json` / re-run classifier if the fix could affect cron-visible status or log-derived counts.
5. Write journal using post-fix counts plus explicit `fixes_applied` evidence.