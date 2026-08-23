# send_email.py Template Type Mismatch

## Pattern

`send_email.py` in the email-templates directory only recognizes `job_search`
as a valid template type. Any other template name (e.g. `vesper_evening`)
produces `Unknown template type: <type>` and exits with code 1.

## Symptom

A cron job that delivers email briefings fails with:
```
EMAIL SEND FAILED: 1 | Unknown template type: vesper_evening
```
The wrapper's `except` block catches this and falls through to a local
HTML failover (writes `<pre>` content to `email-failover/`). The briefing
is NOT lost but is NOT emailed either.

## Detection

Grep for `Unknown template type` in gateway logs or `last_error` fields
of delivery-related cron jobs.

## Root Cause

`send_email.py` line 91: `if template_type != "job_search": sys.exit(1)`
was never generalized beyond the original job_search use case.

## Fix Direction

Two options:
1. **Extend send_email.py** — add `vesper_evening` (and other template types)
   to the allowed list, with a `render()` function per type.
2. **Bypass send_email.py** — per SKILL.md guidance, use MCP tools directly
   for vesper briefings (`mcp_google_workspace_send_gmail_message`) and
   let `send_email.py` handle only `job_search`.

Option 2 is preferred per SKILL.md — `briefing_deliver.py` is explicitly
flagged as broken and MCP-based delivery is the recommended path.

## Related

- `oc_vesper_template_missing` fingerprint in issues.jsonl
- `references/ocas-custodian.md` § Error Handling table for escalation
