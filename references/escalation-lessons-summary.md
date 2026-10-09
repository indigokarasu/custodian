# Escalation Execution Lessons — Summary

Full text: `references/escalation-execution-lessons.md`. Read before applying a Tier 1/2 fix or filing anything `user_gated`.

Four that change what you do:

- **Repair the CLASS, not the member.** An issue repaired member-by-member more than twice
  has the wrong repair unit — enumerate the whole set in one pass, then say whether the
  correction weakens anything.
- **A negative control is the only thing that distinguishes a passing detector from a
  broken one, and it catches the fixer.** Derive exception lists by RULE, never by literal
  name. When your own control's assertion fails, suspect the fixture first.
- **A `user_gated` flag is a CLAIM about the fix path, not a verdict on the risk.** Run
  `hermes cron edit --help` before concluding a cron prompt/schedule is uneditable.
- **Diff the backup FILE, never a reconstructed snapshot, and assert the NET substitution**
  (`apply(SUB, original) == live`); classify scheduler-written volatile fields
  (`fire_claim`, `next_run_at`, `last_dispatch`, `pending_slot`) separately so their drift
  never fails a config-integrity assertion.

Corollary: `hermes cron run <id>` can contend with the store this run just wrote — when the
assertion under test is a text signature in the registry, re-reading the registry after the
edit IS the re-validation; record the behavioral confirmation as pending, with the specific
check and the scheduled run that will produce it.
