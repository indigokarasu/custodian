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

## Status: FIXED 2026-09-25

`send_email.py` no longer has a hardcoded template allowlist. It now discovers any
`<name>.py` module in `commons/email-templates/` at runtime (must export
`subject(data)`/`render(data)`) and actually sends via `send_html()` — the CLI path
previously always printed "Delivery simulated" and never sent real email regardless
of which template was requested; that's fixed too. All templates (`dream_journal`,
`vesper_briefing`, `job_search`) are now built on a shared `_base.py` (the file this
doc's "Fix Direction" section correctly anticipated but which didn't exist yet) for
consistent, mobile-safe, light/dark-aware HTML. `--dry-run` (already documented in
the email-sending skill, never implemented) now works.

Separately: `dispatch:briefing-deliver`'s cron target
(`~/.hermes/scripts/briefing_deliver.py`) was an unrelated stub that only marked
briefings `delivered: true` without ever sending anything — the real, working sender
sat unused at `ocas-dispatch/scripts/briefing_deliver.py`. The root script is now a
thin wrapper that execs the real one, which has been updated to use the shared
`vesper_briefing` template. This is why briefings were being silently no-op'd rather
than hitting this doc's `Unknown template type` error path at all.
