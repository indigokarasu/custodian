# Divergent Branch Handling During Self-Update

Topic branches may contain content NOT on origin/main. They are often **divergent** from HEAD.

## Detection
```bash
git branch -r | while read b; do echo "=== $b ==="; git log HEAD..$b --oneline --no-merges 2>&1 | head -5; done
git merge-base HEAD origin/<branch>
```
Exit code 1 + empty output = DIVERGENT (no common ancestor).

## Why They Exist
Branches created from old release tags (v1.0.0-v1.3.4) while main continued with Hermes adaptations.

## Safe Extraction
Do NOT git merge divergent branches - imports incompatible frontmatter (openclaw: key), old version strings, removed files. Instead: identify unique commits with `git log HEAD..origin/<branch> --oneline --no-merges`, assess compatibility with `git diff HEAD..origin/<branch> -- SKILL.md | grep -c 'openclaw'`, extract valuable sections as manual patches via the patch tool.

## Known Divergent Branches (2026-06-04)
| Branch | Unique commits | Valuable content | Status |
|---|---|---|---|
| origin/docs/known-code-fixes | 3 | Known Code Fixes section, 429 patterns, google_token fingerprint | Extracted in v1.5.1 |
| origin/feat/cron-registry-health-checks | 0 | Already merged into HEAD | No action needed — 0 unique commits |
| origin/fix/cron-schedule-staggering | 1 | Older SKILL.md rewrite (incompatible, pre-Hermes) | Skip — older divergent branch |
| origin/merge/consolidate-helpers | 1 | api-key-audit section | Extracted in v1.5.1 |
| origin/merge/searchx-guardian | 2 | SearXNG health monitoring, platform auto-detection fingerprints | Extracted in v1.5.1 |
| origin/merge/skill-status-diagnostic | 2 | 429 sub-patterns, skill-status-diagnostic merge | Extracted in v1.5.1 |
| origin/patch/hermes-execution-patterns | 18 | Old Hermes execution patterns from pre-adaptation era; contains incompatible `scripts/custodian.py` (1708 lines, OpenClaw-era backup scripts, gateway_health_check.py) | **Do NOT merge** — divergent, all valuable content already extracted. Has 1 unique tip commit (`92b7a57`) but entire branch is pre-Hermes. |

## Version Warning
"1.3.4+hermes" may be MORE adapted than "1.5.1" (raw upstream). Check content and commit dates, not version strings. The +hermes suffix indicates Hermes adaptations applied.
