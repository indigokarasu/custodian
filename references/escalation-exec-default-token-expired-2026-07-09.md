# Escalation execution: default-provider `token_expired` missed-enrollment pattern (2026-07-09)

## Summary

During an escalation execution loop, `verify_escalation_state.py` reported no inverse/forward discrepancies, but `find_missed_user_gated_jobs.py` found many enabled+erroring jobs. Most were known Nous 401 jobs, but several agent-mode jobs failed with:

```text
RuntimeError: Error code: 401 - {'error': {'message': 'Provided authentication token is expired. Please try signing in again.', 'code': 'token_expired'}, 'status': 401}
```

This is a distinct user-gated fingerprint from Nous/OpenRouter/owl-alpha: `oc_default_provider_token_expired`.

## Correct handling

1. Run both probes, in order:
   - `scripts/verify_escalation_state.py` for internal consistency of already-enrolled issues.
   - `scripts/find_missed_user_gated_jobs.py` for enabled+erroring jobs absent from every `jobs_paused` list.
2. For jobs with default-provider `token_expired`, create or update an issue like:
   - `issue_id`: `oc_default_provider_token_expired_<date>`
   - `fingerprint`: `oc_default_provider_token_expired`
   - `status`: `user_gated`
   - `escalation_needed`: `true`
   - `jobs_paused`: affected job IDs
3. Pause the affected jobs directly in the profile `jobs.json`:
   - `enabled: false`
   - `state: paused`
   - `paused_at: <utc iso timestamp>`
   - `pause_reason`: user-gated provider/auth failure; resume after sign-in/token refresh
4. Write an action journal. Be explicit that this is mitigation, not resolution.
5. Re-run `find_missed_user_gated_jobs.py`; success means `missed=0` and `unknown=0` except intentionally-running transients.

## Verification-script blind spot

`verify_escalation_state.py` may show `extra` paused jobs for a new fingerprint it does not know how to map (example: `oc_default_provider_token_expired` showed `actual_paused=0 extra=9`). Treat that as a script mapping limitation, not necessarily a reconciliation failure, after confirming:

- the new issue exists in `issues.jsonl`,
- the listed job IDs are disabled/paused in live `jobs.json`, and
- `find_missed_user_gated_jobs.py` no longer reports them as missed.

## `monitor:journals` exit-1 during the same pass

A bare `Script exited with code 1` on `monitor:journals` is not automatically user-gated. Inspect the script and state first. In the 2026-07-09 pass, state showed a pending newer journal, running `monitor_journals.py` exited `0` and enqueued the journal, so the prior error was stale and `jobs.json` could be reset to `last_status=ok`.

Caution: existing readonly verification remains preferred when the state indicates no new journals; running the monitor can enqueue work. Only run it intentionally when enqueuing pending work is acceptable and document that side effect in the action journal.