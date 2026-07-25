# Escalation-Loop Issue-Status Scan Trap (2026-07-15)

When an escalation execution loop scans `issues.jsonl` for "open / escalated" issues, the
naive predicate `status in ("escalated", "fix_attempted_failed")` finds **nothing** on the
current schema. Those string values are not used. This caused a real risk of emitting
`[SILENT]` with a live user-gated issue left unhandled.

## Actual status vocabulary (verified 2026-07-15)

Parsed 37 entries in `<hermes-home>/profiles/indigo/commons/data/ocas-custodian/issues.jsonl`:

- `resolved` — closed (incl. stale-premise resolutions). NOT open.
- `duplicate` — a MERGE of another issue (e.g. `oc_openrouter_402_credits_exhausted_20260712T040120`
  merged into the 20260706 entry). Looks "open" at a glance but is NOT actionable — it was
  folded into another issue. Treat as closed.
- `user_gated` — genuinely OPEN and unresolvable in cron (needs <operator>: OAuth, billing,
  interactive credential flow). tier-3 typically carries `escalation_needed: true`.

There is NO `escalated` or `fix_attempted_failed` status in the live file.

## Reliable open-signal predicate

```
open = (status not in ("resolved","duplicate")) AND (escalation_needed == true OR status == "user_gated")
```

Enumerate and filter per-entry. Do NOT rely on a single status string.

## `parse_issues_jsonl.py` overcounts "open"

The script's summary line reports `open: N` where N includes `duplicate` entries (it counts
anything not literally `resolved`). On 2026-07-15 it printed `open: 2` → in reality 1 genuine
`user_gated` + 1 `duplicate`. **Always dump and inspect each open entry's full object**;
never trust the summary count as the actionable count.

## Correct loop procedure (already embedded in SKILL.md Execution Loop section)

1. Parse profile `issues.jsonl` (brace-depth parser — multiple objects per line).
2. Filter with the predicate above.
3. For each open entry: verify live state both directions (job still erroring? premise
   stale? dead-reference?). Re-run the actual job if needed.
4. Auto-fix only what is actually fixable (tier-1 auto-fix registry; the known-auto-fix
   list in the loop prompt). Interactive OAuth (e.g. Spotify `SPOTIFY_REFRESH_TOKEN`) is
   NOT auto-fixable — reconcile metadata (last_verified_at) and leave open.
5. Reconcile `jobs_paused` vs live disabled state; use `race_safe_issue_patch.py`.
6. Do NOT report user-gated issues as "fixed". Honesty rule.
