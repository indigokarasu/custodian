# Live vs Stale Provider-Error Recipe

When a deep/light scan surfaces a storm of 401/402/404 errors across many jobs, the single most
important question is: **is the outage live right now, or are these stale errors from jobs that
haven't re-run?** Misclassifying kills the verdict (false escalation or a false `[SILENT]`).

## Decisive checks (run in order)

1. **You are proof.** The Custodian scan you are running executes on the default provider/model.
   If you are producing output, the token/route is not globally dead. (Caveat: a fallback route may
   differ from the failing jobs — still corroborate with #2.)

2. **Scan the live gateway log for successful completions in the last hour.**
   - Find it: `find <hermes-home>/profiles/<profile>/logs -name "*.log" -mmin -90`
   - Path is normally `<hermes-home>/profiles/<profile>/logs/gateway.log`
     (NOT `<hermes-home>/logs/gateway.log` — that one is stale/rotated).
   - `grep -nE "response ready|Sending response" <live_gateway.log> | tail -20`
   - If real LLM responses landed in the last hour, the provider route works for live traffic → the
     job errors are stale.

3. **Compute error age from `last_run_at`.** Errors spanning hours-to-days with no recent re-run are
   frozen from a prior failure; the scheduler hasn't retried since the provider may have recovered.
   - Parse with `datetime.fromisoformat(s).astimezone(timezone.utc)` — jobs.json timestamps carry
     explicit offsets (e.g. `-07:00`); never compare the raw string to a UTC `now` (that misreads
     "7h overdue"). See `references/jobs-json-timestamp-offset-misread-pitfall.md`.

## Decision
- Provider live (checks 1–2 pass) → errors are stale → **leave jobs running** (do NOT pause; per the
  2026-07-11 correction, provider/auth/credit outages are not kill switches). They self-recover on the
  next scheduled run.
- Provider genuinely down (no successful responses, repeated fresh failures) → user-gated escalation;
  verify live FIRST, then leave running unless retry is futile/destructive.

## Confirmed example (2026-07-13)
66 enabled error jobs (mostly 401 `token_expired`), yet the gateway served Telegram LLM responses
00:xx–01:20 PDT and the deep scan ran fine on nous/hy3:free. All provider-cluster errors were stale;
correctly left running. The lone genuine failures were 2 ocas-weave jobs with a deterministic
`FileNotFoundError` (skill-code `Path.home()` bug) — those were paused (futile retry) + escalated.

## Related
- `references/escalation-deepscan-pitfalls-2026-07.md` — pitfall #3 (this recipe's home) plus the
  `find_missed` pause-conflict, the `parse_issues_jsonl.py` sync-copy trap, and the doubled-path
  skill-code bug.
