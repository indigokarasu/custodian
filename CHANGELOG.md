# Changelog

All notable changes to ocas-custodian are recorded in this file.

## [3.2.0] - 2026-10-03

### Fixed
- **`scripts/append_issue_row.py` did not exist.** SKILL.md and the pitfall catalogue both instructed every append to go through a shared helper that asserts `issue_id` and `escalation_needed` per row and verifies the open-count delta — a helper that has never existed. This is the third instance of the "control that exists but is invoked by nothing" pattern in this skill's own history (`custodian_sweep.py`, the env-parity probe, and now the append helper), and the first one that was documented as done. Built, with `--help`, `--dry-run`, meaningful exit codes (0/2/3/4), and a derivation of the expected delta from the open filter as written rather than a hand-maintained kind table.
- **The helper's delta rule was wrong for reopens, and the control suite caught it.** The stated rule — "0 for any superseding row" — raises a phantom failure when an open row supersedes the last row of an already-*closed* issue, because the open count genuinely returns to +1. Corrected to `is_open(this_row) - is_open(previous_last_row)`, which is the filter applied twice and has no special cases. Found by running `references/append_issue_row_control.py` (24 arms: 11 positive, 9 that must fail), not by reading the code.

### Added
- `references/append_issue_row_control.py` — control suite for the append helper. Throwaway store under a unique temp dir; the real `issues.jsonl` is never opened. Each negative arm is reproduced from a defect observed in production, including the omitted-`escalation_needed` invisibility class and the terminal-row-claiming-escalation contradiction.
- `references/measurement-pitfalls.md` — the Critical Pitfalls catalogue (40+ rules) extracted verbatim from SKILL.md.
- `references/escalation-execution-lessons.md` — the escalation-execution lessons extracted verbatim from SKILL.md.
- `references/scan-surface-is-a-plugin-tool.md` — the `custodian.scan.light` is-not-a-CLI-verb incident and its recovery notes.

### Changed
- **SKILL.md: 63,196 → 17,555 chars, 305 → 281 lines.** Three extractions, all verbatim into `references/`: `## Critical Pitfalls` (41KB, 76% of the file) → `references/measurement-pitfalls.md`; `## Escalation execution: hard-won lessons` (6.2KB) → `references/escalation-execution-lessons.md`; the `custodian.scan.light`-is-not-a-CLI-verb block (2.0KB) → `references/scan-surface-is-a-plugin-tool.md`. The per-script inventory moved to `references/using-script.md`, which already held the usage surface. Highest-frequency rules stay inline: the six measurement rules that cause most findings, the four highest-leverage escalation lessons, three run gates, and the Error Handling table. Body is now 3,600 approx tokens (excluding frontmatter).
- `references/support-file-map.md` — five new entries with conditional When-to-read language, inserted at the head so on-demand navigation costs one read; now 108 entries.
- Corrected a stale claim in `## Operational Hints`: it read as though `hermes cron edit` did not exist. It does — for prompt/schedule/name it is a per-job atomic edit. That contradiction with the escalation lessons was live in the same file.
- The `issues.jsonl` delta rule stated inline was wrong (see Fixed). It now reads `is_open(this_row) - is_open(previous_last_row_for_this_issue_id)`.

## [3.1.0] - 2026-09-16

### Changed
- **Two-stage self-healing contract** — enforced decision-log → repair → re-validation gate before any fix is marked resolved (`spec-ocas-recovery.md`).
- **Recovery log compaction** — recovery/evidence logs compacted when exceeding 1,000 entries (retain per-day rollups, drop stale per-run entries).