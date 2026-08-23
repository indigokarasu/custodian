# 503 Upstream Capacity Scope-Expansion (2026-07-28)

## Pattern

When `oc_provider_503_upstream_capacity` already exists as an open issue but covers fewer live 503 jobs than are currently erroring, the light-scan journal must flag the scope gap. Do NOT write a duplicate issue or a second entry for the same fingerprint — all 6 Nous/ocas 503 jobs are one upstream capacity event; the issue needs its `affected_job_ids` broadened.

## Occurrence

2026-07-28: `oc_provider_503_upstream_capacity_20260727T060251Z` covered only `vesper:evening` (1 of 6). The other 5 live 503 jobs were not in `affected_job_ids`:

- `bower:scan` (d751b1530df5)
- `vesper:morning` (3e413ca11625)
- `mentor:deep` (74bd00fc42bc)
- `ocas-finch:daily` (6f21c8f249a4)
- `menu-monitor-weekly` (a6788bcd3411)

## Fix Direction

1. Light scan journal records `scope_gaps.oc_provider_503_upstream_capacity` with the list of missing job IDs and the note "Issue scope needs expansion."
2. Mentor/escalation-runner expands the issue's `affected_job_ids` to include all 5 missing jobs.
3. No duplicate issue created — all share fingerprint `oc_provider_503_upstream_capacity`.

## Rule

One fingerprint = one issue. If an existing open issue already matches the fingerprint, do NOT create a new issue. Expand the existing issue's scope instead.