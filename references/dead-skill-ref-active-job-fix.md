# Dead skill reference on an active (enabled) job — fix procedure

When Step 5 finds a skill whose directory is missing (`<hermes-root>/profiles/<profile>/skills/<skill>/` absent) but the skill is in the live active-skill set (referenced by an enabled, non-paused job), the correct remediation depends on WHICH directory is missing:

## Case A: skill dir PRESENT, but `data/` or `journals/` dirs missing
→ Tier 1: `mkdir` the missing dirs + write a `config.json` (see `fix-missing-skill-data-directories-2026-06-30.md` / `fix-missing-skill-journals-directories-2026-06-30.md`). The skill legitimately exists; it just needs storage.

## Case B: skill dir ITSELF missing (skill was archived / merged / never installed)
→ Do NOT recreate the skill directory. The skill is gone by design. Instead, remove the dead reference from the job:
  - For a job with `"skill": "ocas-lucid"` → set `skill: null`.
  - For a job with `"skills": ["ocas-lucid", ...]` → remove the entry from the array.
  - Edit `<hermes-root>/profiles/<profile>/cron/jobs.json` directly (cron context: CLI reads the wrong path), then re-run the active-skill re-derivation to confirm zero dead refs remain.
This is safe when the job currently runs OK (`last_status: ok`) — the ref is vestigial metadata the framework tolerates; removing it prevents a latent skill-load failure on a future run that honors the `skill` field.

### Confirmed 2026-07-08
`lucid:update` (id `a0a8b54c5637`) was enabled+scheduled+`last_status=ok` but `skill: "ocas-lucid"` while `<hermes-home>/skills/ocas-lucid/` did not exist (lucid archived/merged). A prior scan intended to remove this ref but it did not persist on this job. Light scan removed the ref (`skill: null`), re-verified: 0 active jobs with dead skill refs. Sibling `lucid:dream` was paused (billing), so its ref was info-only, not actionable.

### Distinguish from escalation
An active job referencing an archived skill is a latent registry defect, not a billing/API-key user-gated issue. Fix it directly (Tier 1) rather than opening a user-gated escalation. (The `oc_lucid_skill_archived_enabled_jobs` escalation pattern from 2026-07-07 applies only when the jobs are failing and the question is whether/when to archive — if the job runs OK, just remove the ref.)
